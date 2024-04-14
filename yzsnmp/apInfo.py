from pysnmp.hlapi import *
import csv
import logging
from threading import Thread


def generate_csv_filename(ip):
    # 将IP中的点替换为下划线
    filename = f"snmp_table_data_{ip.replace('.', '_')}.csv"
    # 构建文件完整路径，假设apinfo.py在/yzsnmp目录下，需要回到上级目录然后进入/csvfiles
    full_path = os.path.join('..', 'csvfiles', filename)
    return full_path


def fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols,
                                csv_writer):
    engine = SnmpEngine()
    user_data = UsmUserData(user, authKey, privKey, authProtocol=authProtocol, privProtocol=privProtocol)
    target = UdpTransportTarget((ip, port))
    context = ContextData()

    table_data = {}

    logging.debug("Starting to fetch SNMP table data.")
    for col_index in range(1, max_cols + 1):
        col_oid = f"{base_oid}.{col_index}"
        logging.debug("Fetching data for column OID: %s", col_oid)
        col_data = []

        iterator = nextCmd(engine, user_data, target, context, ObjectType(ObjectIdentity(col_oid)), lexicographicMode=False)

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
                            # 检查是否为特定的需要转换的OID
                            if str(oid).endswith('1.5'):  # 假设需要转换的OID以'.1.5'结尾
                                # 对值进行十六进制到中文的转换
                                converted_value = convert_hex_to_chinese(value.prettyPrint())
                                col_data.append(converted_value)
                                logging.debug("Fetched and converted value %s for OID %s", converted_value, oid)
                            else:
                                col_data.append(value.prettyPrint())
                                logging.debug("Fetched value %s for OID %s", value.prettyPrint(), oid)
                        else:
                            logging.debug("Reached the end of column OID: %s", col_oid)
                            break
                    if not str(oid).startswith(col_oid):
                        break
            except StopIteration:
                logging.debug("Completed fetching data for column OID: %s", col_oid)
                break

        table_data[col_index] = col_data

    logging.debug("Completed fetching table data. Now writing to CSV.")
    max_rows = max(len(col) for col in table_data.values())

    for row_index in range(max_rows):
        row = [table_data[col_index][row_index] if row_index < len(table_data[col_index]) else '' for col_index in range(1, max_cols + 1)]
        csv_writer.writerow(row)
        logging.debug("Wrote row %d to CSV", row_index + 1)

    logging.info("Table data fetching and CSV writing completed.")

#多线程工作
def thread_function(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_filename, column_titles):
    with open(csv_filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # 写入列标题
        csv_writer.writerow(column_titles)
        logging.info(f"Fetching data for IP: {ip}")
        fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_writer)
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
    snm_csv_files = []
    for ip in ips:
        #snmp_table_data_10_170_69_101.csv
        csv_filename = generate_csv_filename(ip)
        snmp_csv_files.append(csv_filename)
        thread = Thread(target=thread_function, args=(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid, max_cols, csv_filename, column_titles))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return snmp_csv_files

