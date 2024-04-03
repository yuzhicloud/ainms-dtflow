from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def get_snmp_data():
    # SNMP目标设备的配置
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    # 创建 SNMP 认证和加密参数
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol

    oids = [
        '.1.3.6.1.4.1.2011.6.139.13.3.3.1.1',
        '.1.3.6.1.4.1.2011.6.139.13.3.3.1.2',
        '.1.3.6.1.4.1.2011.6.139.13.3.3.1.3',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.4',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.5',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.6',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.7',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.13',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.18'
    ]

    results = []

    while True:
        errorIndication, errorStatus, errorIndex, varBinds = next(
            getCmd(SnmpEngine(),
                   UsmUserData(user, authKey, privKey,
                               authProtocol=authProtocol,
                               privProtocol=privProtocol),
                   UdpTransportTarget((ip, port)),
                   ContextData(),
                   ObjectType(ObjectIdentity(oids)),
                   lookupMib=False)
        )

        if errorIndication:
            logging.error(errorIndication)
            break
        elif errorStatus:
            logging.error('%s at %s', errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex) - 1][0] or '?')
            break
        else:
            for varBind in varBinds:
                oid = varBind[0].prettyPrint()
                value = varBind[1].prettyPrint()
                # Check if the returned OID is beyond the base OID
                if not oid.startswith(base_oid):
                    break
                results.append([oid, value])
                logging.debug('OID: %s, Value: %s', oid, value)
                # Update the base_oid to the newly returned OID for the next iteration
                base_oid = oid
            else:
                # Continue the loop if no break occurred in the for loop
                continue
            # A break occurred in the for loop, so we break the while loop
            break

        # 保存结果到CSV文件
    with open('snmp_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['OID', 'Value'])
        csvwriter.writerows(results)

    logging.info("完成，结果已保存到snmp_results.csv")

