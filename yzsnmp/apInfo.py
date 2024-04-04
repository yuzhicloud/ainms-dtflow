from pysnmp.hlapi import *
import csv
import logging


def fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                csv_writer):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    # 存储整个表的数据，key为列号，value为该列的所有数据
    table_data = {}

    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        col_data = []

        iterator = nextCmd(engine,
                           user_data,
                           target,
                           context,
                           ObjectType(ObjectIdentity(col_oid)),
                           lexicographicMode=False)
        while True:
            try:
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
                if errorIndication:
                    logging.error("Error: %s", errorIndication)
                    break
                elif errorStatus:
                    logging.error("Error: %s at %s", errorStatus.prettyPrint(),
                                  varBinds[int(errorIndex) - 1][0] if errorIndex else '?')
                    break
                else:
                    for varBind in varBinds:
                        oid, value = varBind
                        if str(oid).startswith(col_oid):
                            col_data.append(value.prettyPrint())
                        else:
                            # 如果当前OID不是该列的一部分，跳出循环
                            break
                    # 如果已处理完所有数据，结束当前列的数据收集
                    if not str(oid).startswith(col_oid):
                        break
            except StopIteration:
                break


        table_data[col_index] = col_data

    # 确定最大行数
    max_rows = max(len(col) for col in table_data.values())

    # 按行写入CSV
    for row_index in range(max_rows):
        row = [table_data[col_index][row_index] if row_index < len(table_data[col_index]) else '' for col_index in
               range(1, max_cols + 1)]
        csv_writer.writerow(row)


def snmp_main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    # 以下配置需要根据你的实际情况进行修改
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 18  # Number of columns in the table

    with open('snmp_table_data.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                    csv_writer)

    logging.info("Table data fetching and CSV writing completed.")


if __name__ == "__main__":
    snmp_main()
