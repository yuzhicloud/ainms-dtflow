from pysnmp.hlapi import *
import csv
import logging


def convert_hex_to_chinese(hex_string):
    """
    将十六进制形式的字符串转换为中文字符串。
    """
    # 移除可能的十六进制前缀"0x"，并将每两个十六进制字符转换为字节
    bytes_obj = bytes.fromhex(hex_string.replace("0x", ""))
    try:
        # 尝试使用utf-8编码解码字节对象
        return bytes_obj.decode('utf-8')
    except UnicodeDecodeError:
        # 如果解码失败，返回原始的十六进制字符串
        return hex_string


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


def snmp_main():
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

    for ip in ips:
        csv_filename = f'snmp_table_data_{ip.replace(".", "_")}.csv'
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # 写入列标题
            csv_writer.writerow(column_titles)

            logging.info(f"Fetching data for IP: {ip}")
            fetch_data_and_write_by_row(ip, port, user, authKey, privKey, authProtocol, privProtocol, base_oid,
                                        max_cols, csv_writer)

        logging.info(f"Table data fetching and CSV writing completed for IP: {ip}.")


if __name__ == "__main__":
    snmp_main()

