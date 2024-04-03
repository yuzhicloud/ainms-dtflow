from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                               csvfile):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    csv_writer = csv.writer(csvfile, lineterminator='\n')

    # 初始化一个空字典，用于按索引（行）存储数据
    table_data = {}

    for col_index in range(1, max_cols + 1):
        last_oid = f"{base_oid}.{col_index}"
        while True:
            errorIndication, errorStatus, errorIndex, varBinds = next(
                nextCmd(engine, user_data, target, context,
                        ObjectType(ObjectIdentity(last_oid)),
                        lexicographicMode=False)
            )

            if errorIndication or errorStatus:
                logging.error("SNMP command failed: %s", errorIndication or errorStatus.prettyPrint())
                break

            for varBind in varBinds:
                oid, value = varBind
                if not str(oid).prettyPrint().startswith(base_oid):
                    break  # 跳出循环，处理下一个列
                # 分析索引（行号），这里简化处理，取OID的最后一部分作为索引
                index = oid.prettyPrint().split('.')[-1]
                # 将值存入对应的索引（行）中
                if index not in table_data:
                    table_data[index] = [value.prettyPrint()]
                else:
                    table_data[index].append(value.prettyPrint())

                last_oid = oid  # 更新last_oid以获取下一个值

            if not str(oid).prettyPrint().startswith(base_oid):
                break  # 当跳到下一个MIB时，结束当前列的处理

    # 按索引（行）顺序写入CSV
    for index in sorted(table_data.keys(), key=int):
        csv_writer.writerow(table_data[index])

    logging.info("Data fetching and CSV writing completed.")



def snmp_main():
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 18  # Number of columns in the table

    with open('snmp_table_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                   csvfile)

    logging.info("Table data fetching and CSV writing completed.")


if __name__ == "__main__":
    snmp_main()
