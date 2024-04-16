import threading
from pysnmp.hlapi import *
import csv
import logging


# 以下配置需要根据你的实际情况进行修改
def hex_to_chinese(hex_str):
    """
    Convert a hex string to a Chinese string (assuming UTF-8 encoding).
    """
    if not isinstance(hex_str, str):
        return "Invalid format"
    hex_str = hex_str.replace('0x', '').lower()
    if not hex_str or len(hex_str) % 2 != 0:
        return "Not a valid hexadecimal value"
    try:
        bytes_str = bytes.fromhex(hex_str)
    except ValueError:
        return "Contains non-hexadecimal characters"
    try:
        return bytes_str.decode('utf-8')
    except UnicodeDecodeError:
        return "Decoding failed"


def fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, filename):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    logging.debug("Starting to fetch SNMP table data.")
    table_data = []
    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        logging.debug("Fetching data for column OID: %s", col_oid)

        iterator = nextCmd(engine, user_data, target, context, ObjectType(ObjectIdentity(col_oid)), lexicographicMode=False)

        col_data = []
        while True:
            try:
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
                if errorIndication:
                    logging.error("Error: %s", errorIndication)
                    break
                elif errorStatus:
                    logging.error("Error: %s at %s", errorStatus.prettyPrint(), varBinds[int(errorIndex) - 1][0] if errorIndex else '?')
                    break
                else:
                    for varBind in varBinds:
                        oid, value = varBind
                        if str(oid).startswith(col_oid):
                            if str(oid) == '1.3.6.1.4.1.2011.6.139.13.3.3.1.5':
                                value = hex_to_chinese(value.prettyPrint())
                            col_data.append(value)
            except StopIteration:
                break
        table_data.append(col_data)

    with open(filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        for row_data in zip(*table_data):
            csv_writer.writerow(row_data)

    logging.info("Table data fetching and CSV writing completed.")


def snmp_main(ips):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    # 以下配置需要根据你的实际情况进行修改
    # ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 3  # Number of columns in the table

    threads = []
    for ip in ips:
        suffix = ip.split('.')[-1]
        filename = f'snmp_table_data_{suffix}.csv'
        thread = threading.Thread(target=fetch_data_and_write_by_row, args=(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, filename))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    logging.info("Table data fetching and CSV writing completed.")


if __name__ == "__main__":
    # ips = ['10.170.69.101', '10.170.69.102', '10.170.69.103', '10.170.69.104']  
    ips = ['10.170.69.101']
    snmp_main(ips)
