import os
from pysnmp.hlapi import *
import csv
import logging
from threading import Thread


# Update the generate_csv_filename to clear the directory before generating the filename
def generate_csv_filename(ip):
    filename = f"snmp_table_data_{ip.replace('.', '_')}.csv"
    # 构建文件完整路径，考虑 apinfo.py 位于 yzsnmp 文件夹下
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'csvfiles')  # 使用 os.path.dirname(__file__) 定位当前文件所在目录
    # No need to check if directory exists here since clear_directory will create it if not present
    full_path = os.path.join(base_dir, filename)
    return full_path


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


# Integrate the hex_to_chinese function within the fetch_data_and_write_by_row
def fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_writer):
    """
    Fetch SNMP data by row for the given IP and write to CSV.
    """
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    logging.debug("Starting to fetch SNMP table data.")
    table_data = {}
    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        logging.debug(f"Fetching data for column OID: {col_oid}")
        iterator = nextCmd(engine, user_data, target, context, ObjectType(ObjectIdentity(col_oid)), lexicographicMode=False)
        record_count = 0

        while True:
            try:
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)

                if errorIndication:
                    logging.error(f"Error: {errorIndication}")
                    break
                elif errorStatus:
                    logging.error(f"Error: {errorStatus.prettyPrint()} at {errorIndex}")
                    break
                else:
                    for varBind in varBinds:
                        oid, value = varBind
                        # The last number of the OID should be the index of the row
                        row_index = int(str(oid).rsplit('.', 1)[-1])
                        if str(oid).startswith(col_oid):
                            if row_index not in table_data:
                                table_data[row_index] = [''] * max_cols
                            # Decode hex string to Chinese if necessary
                            if col_oid.endswith('1.5'):
                                decoded_value = hex_to_chinese(value.prettyPrint())
                                table_data[row_index][col_index - 1] = decoded_value
                            else:
                                table_data[row_index][col_index - 1] = value.prettyPrint()
                        else:
                            logging.debug(f"Reached the end of column OID: {col_oid}")
                            break

                record_count += 1
                if record_count >= 100:
                    logging.info(f"Reached 100 records for IP: {ip}, exiting fetch.")
                    break

            except StopIteration:
                logging.debug(f"Completed fetching data for column OID: {col_oid}")
                break

            if record_count >= 100:
                break

    logging.info(f"Table data fetching completed for IP: {ip}. Now writing to CSV.")

    # Write the collected data to CSV
    for row_index in sorted(table_data.keys()):
        csv_writer.writerow(table_data[row_index])
        logging.debug(f"Wrote row {row_index} to CSV for IP: {ip}")

    logging.info(f"CSV writing completed for IP: {ip}.")


# 多线程工作
def thread_function(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_filename,
                    column_titles):
    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # 写入列标题
        csv_writer.writerow(column_titles)
        logging.info(f"Fetching data for IP: {ip}")
        fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                    csv_writer)
        logging.info(f"Table data fetching and CSV writing completed for IP: {ip}.")


def snmp_main(ips):
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    ips = ['10.170.69.101', '10.170.69.104', '10.170.69.107', '10.170.69.110']
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol
    base_oid = '1.3.6.1.4.1.2011.6.139.13.3.3.1'
    max_cols = 18  # Number of columns in the table

    # 定义列标题
    column_titles = ['hwWlanApMac', 'hwWlanApSn', 'hwWlanApTypeInfo', 'hwWlanApName', 'hwWlanApGroup',
                     'hwWlanApRunState', 'hwWlanApSoftwareVersion', 'hwWlanApHardwareVersion', 'hwWlanApCpuType',
                     'hwWlanApCpufrequency', 'hwWlanApMemoryType', 'hwWlanApDomain', 'hwWlanApIpAddress',
                     'hwWlanApIpNetMask', 'hwWlanApGatewayIp', 'hwWlanApMemorySize', 'hwWlanApFlashSize',
                     'hwWlanApRunTime']
    threads = []
    snmp_csv_files = []
    for ip in ips:
        # snmp_table_data_10_170_69_101.csv
        csv_filename = generate_csv_filename(ip)
        snmp_csv_files.append(csv_filename)
        thread = Thread(target=thread_function, args=(
            ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_filename,
            column_titles))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return snmp_csv_files
