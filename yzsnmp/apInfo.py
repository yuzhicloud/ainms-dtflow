from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_table_and_write_to_csv(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                 csv_writer):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    logging.debug("Starting to fetch the SNMP table data and writing to CSV...")

    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        logging.debug("Fetching column: %s", col_oid)
        col = []
        last_oid = ObjectIdentity(col_oid)

        iterator = nextCmd(engine, user_data, target, context, ObjectType(last_oid), lexicographicMode=False)

        while True:
            try:
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)

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
                        if str(oid).startswith(col_oid):
                            logging.debug("Found value: %s for OID: %s", value.prettyPrint(), oid.prettyPrint())
                            col.append(value.prettyPrint())
                        else:
                            logging.debug("OID: %s is outside of the column: %s", oid.prettyPrint(), col_oid)
                            break
                    if not str(oid).startswith(col_oid):
                        logging.debug("Completed fetching for column: %s", col_oid)
                        break
            except StopIteration:
                break

        if col:  # 如果这一列有数据，则写入CSV文件
            csv_writer.writerow(col)

    logging.debug("Completed fetching the SNMP table data and writing to CSV.")


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

    with open('snmp_table_data.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # 直接在获取数据的函数中写入CSV，每获取到一行数据后立即写入
        fetch_table_and_write_to_csv(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                     csvwriter)

    logging.info("Table data fetching and CSV writing completed.")


if __name__ == "__main__":
    snmp_main()
