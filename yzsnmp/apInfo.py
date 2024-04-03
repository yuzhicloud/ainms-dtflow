from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    table = []
    for start_oid in [f"{base_oid}.{i}" for i in range(1, max_cols + 1)]:
        col = []
        last_oid = start_oid
        while True:
            errorIndication, errorStatus, errorIndex, varBinds = next(
                nextCmd(engine, user_data, target, context,
                        ObjectType(ObjectIdentity(last_oid)),
                        lexicographicMode=False)
            )

            if errorIndication or errorStatus:
                logging.error(errorIndication or errorStatus.prettyPrint())
                break

            for varBind in varBinds:
                oid, value = varBind
                if not str(oid).startswith(base_oid):
                    break
                col.append(value.prettyPrint())
                last_oid = oid.prettyPrint()

            if not str(oid).startswith(base_oid):
                break
        table.append(col)

    # Transpose the table to get rows instead of columns
    return list(map(list, zip(*table)))


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

    table_data = fetch_table(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols)

    # Save to CSV
    with open('snmp_table_data.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        for row in table_data:
            csvwriter.writerow(row)

    logging.info("Table data fetched and saved to snmp_table_data.csv")


if __name__ == "__main__":
    snmp_main()
