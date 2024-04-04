from pysnmp.hlapi import *
import csv
import logging

# 设置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')


def fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                               csvfile_path):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    with open(csvfile_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, lineterminator='\n')

        # 获取所有索引
        indexes = set()
        logging.debug("Attempting to retrieve indexes using the base OID.")
        for (errorIndication, errorStatus, errorIndex, varBinds) in nextCmd(
                engine, user_data, target, context,
                ObjectType(ObjectIdentity(base_oid)),
                lexicographicMode=False):

            if errorIndication:
                logging.error("Error: %s", errorIndication)
                return
            elif errorStatus:
                logging.error("Error: %s at %s", errorStatus.prettyPrint(),
                              errorIndex and varBinds[int(errorIndex) - 1][0] or '?')
                return
            else:
                for varBind in varBinds:
                    oid, _ = varBind
                    # Assuming the last digit in OID is the index
                    index = '.'.join(oid.prettyPrint().split('.')[-max_cols:])
                    indexes.add(index)

        logging.debug("Indexes found: %s", list(indexes))

        # 对于每个索引，获取所有列的数据
        for index in sorted(indexes):
            row = []
            for col_index in range(1, max_cols + 1):
                column_oid = f"{base_oid}.{col_index}.{index}"
                errorIndication, errorStatus, errorIndex, varBinds = next(
                    getCmd(engine, user_data, target, context,
                           ObjectType(ObjectIdentity(column_oid))))

                if errorIndication:
                    logging.error("Error fetching OID %s: %s", column_oid, errorIndication)
                    row.append('')
                elif errorStatus:
                    logging.error("Error fetching OID %s: %s", column_oid, errorStatus.prettyPrint())
                    row.append('')
                else:
                    for varBind in varBinds:
                        _, value = varBind
                        row.append(value.prettyPrint())

            # 将获取的行数据写入CSV
            csv_writer.writerow(row)
            logging.debug("Wrote data for index %s to CSV: %s", index, row)

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
    max_cols = 18
    csvfile_path = 'snmp_table_data.csv'

    fetch_and_write_table_data(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                               csvfile_path)


if __name__ == "__main__":
    snmp_main()
