from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csvfile):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    csv_writer = csv.writer(csvfile, lineterminator='\n')

    # 收集所有行的数据，每个元素是一行数据的列表
    rows = []

    # 假设所有列的索引都相同，我们使用第一列来获取所有可能的索引
    indexes = []
    for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
        engine, user_data, target, context,
        ObjectType(ObjectIdentity(f"{base_oid}.1")),  # 假设第一列包含所有索引
        lexicographicMode=False):

        if errorIndication or errorStatus:
            logging.error(f"SNMP Error: {errorIndication or errorStatus.prettyPrint()}")
            break

        for varBind in varBinds:
            oid, value = varBind
            index = oid.prettyPrint().split('.')[-1]
            if index not in indexes:
                indexes.append(index)

    # 对每个索引，收集所有列的数据
    for index in indexes:
        row = []
        for col_index in range(1, max_cols + 1):
            oid = f"{base_oid}.{col_index}.{index}"
            errorIndication, errorStatus, errorIndex, varBinds = next(
                getCmd(engine, user_data, target, context,
                       ObjectType(ObjectIdentity(oid)),
                       lexicographicMode=False))

            if errorIndication or errorStatus:
                value = 'Error'
                logging.error(f"SNMP Error: {errorIndication or errorStatus.prettyPrint()}")
            else:
                for varBind in varBinds:
                    _, value = varBind
                    value = value.prettyPrint()
            row.append(value)
        rows.append(row)

    # 将每行数据写入CSV文件
    for row in rows:
        csv_writer.writerow(row)
        logging.debug("Wrote one row to CSV: %s", row)

    logging.info("Completed fetching SNMP table data and writing to CSV.")


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
        fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csvfile)

    logging.info("Table data fetching and CSV writing completed.")


if __name__ == "__main__":
    snmp_main()
