from pysnmp.hlapi import *
import csv
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

def get_snmp_data():
    ip = '10.170.69.101'
    port = 161
    user = 'clypgac'
    authKey = 'longyuandianli@123'
    privKey = 'longyuandianli@123'
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol

    oids = [
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.1',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.2',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.3',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.4',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.5',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.6',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.7',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.13',
        '1.3.6.1.4.1.2011.6.139.13.3.3.1.18'
    ]

    results = []

    for oid in oids:
        errorIndication, errorStatus, errorIndex, varBinds = next(
            nextCmd(SnmpEngine(),
                   UsmUserData(user, authKey, privKey,
                               authProtocol=authProtocol,
                               privProtocol=privProtocol),
                   UdpTransportTarget((ip, port), timeout=3, retries=5),
                   ContextData(),
                   ObjectType(ObjectIdentity(oid)),
                   lookupMib=False)  # Use snmpget for each OID
        )

        if errorIndication:
            logging.error(errorIndication)
        elif errorStatus:
            logging.error('%s at %s', errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex) - 1][0] or '?')
        else:
            for varBind in varBinds:
                oid = varBind[0].prettyPrint()
                value = varBind[1].prettyPrint()
                results.append([oid, value])
                logging.debug('OID: %s, Value: %s', oid, value)

    # 保存结果到CSV文件
    with open('snmp_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['OID', 'Value'])
        csvwriter.writerows(results)

    logging.info("完成，结果已保存到snmp_results.csv")

