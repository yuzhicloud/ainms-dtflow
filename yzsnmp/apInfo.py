from pysnmp.hlapi import *
import csv
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    table = []
    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        col = []
        for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
                engine,
                user_data,
                target,
                context,
                ObjectType(ObjectIdentity(col_oid)),
                lexicographicMode=False
        ):
            if errorIndication:
                logging.error("Error: %s", errorIndication)
                break
            elif errorStatus:
                logging.error("Error: %s at %s", errorStatus.prettyPrint(),
                              errorIndex and varBinds[int(errorIndex) - 1][0] or '?')
                break
            else:
                for varBind in varBinds:
                    oid, value = varBind
                    if str(oid).startswith(base_oid):
                        col.append(value.prettyPrint())
                    else:
                        break
                if not str(oid).startswith(base_oid):
                    break
        table.append(col)
        # 记录处理完的一列（对应表的一行）
        logging.debug("Processed column OID: %s with values: %s", col_oid, ', '.join(col))

    # 转置表格以按行组织数据，并记录每行数据
    transposed_table = list(map(list, zip(*table))) if table else []
    for row_index, row in enumerate(transposed_table, start=1):
        logging.debug("Processed row %d: %s", row_index, ', '.join(row))

    return transposed_table


def snmp_main():
    # 这里替换为你的实际配置
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 18  # 假定的列数

    table_data = fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols)

    # 保存到CSV
    with open('snmp_table_data.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for row in table_data:
            csvwriter.writerow(row)

    logging.info("Table data fetched and saved to snmp_table_data.csv")


if __name__ == "__main__":
    snmp_main()
