from pysnmp.hlapi import *
import csv
import logging

def fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols):
    # 创建 SNMP Engine 实例
    engine = SnmpEngine()

    # 设置用户数据，这里假设authProtocol和privProtocol已经是正确的对象
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)

    # 设置传输目标
    target = UdpTransportTarget((ip, port))

    # 创建上下文
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
                logging.error(errorIndication)
                break
            elif errorStatus:
                logging.error('%s at %s', errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex) - 1][0] or '?')
                break
            else:
                for varBind in varBinds:
                    oid, value = varBind
                    # 检查OID是否仍在期望的列中
                    if str(oid).startswith(base_oid):
                        col.append(value.prettyPrint())
                    else:
                        break
                # 检查是否已经处理完当前列的所有行
                if not str(oid).startswith(base_oid):
                    break
        table.append(col)

    # 转置表格以按行组织数据
    return list(map(list, zip(*table))) if table else []

def snmp_main():
    # 示例配置，需要根据实际情况调整
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 18  # Number of columns in the table

    table_data = fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols)

    # Save to CSV
    with open('snmp_table_data.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for row in table_data:
            csvwriter.writerow(row)

    logging.info("Table data fetched and saved to snmp_table_data.csv")

if __name__ == "__main__":
    snmp_main()
