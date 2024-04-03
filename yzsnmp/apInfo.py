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
    authProtocol = usmHMAC192SHA256AuthProtocol
    privProtocol = usmAesCfb256Protocol

    # 表的根节点
    oid_base = '1.3.6.1.4.1.2011.6.139.13.3.3.1'

    results = []

    for (errorIndication,
         errorStatus,
         errorIndex,
         varBinds) in nextCmd(SnmpEngine(),
                              UsmUserData(user, authKey, privKey,
                                          authProtocol=authProtocol,
                                          privProtocol=privProtocol),
                              UdpTransportTarget((ip, port)),
                              ContextData(),
                              ObjectType(ObjectIdentity(oid_base)),
                              lexicographicMode=False):  # 防止遍历到表之外

        if errorIndication:
            logging.error(errorIndication)
            break
        elif errorStatus:
            logging.error('%s at %s', errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex)-1][0] or '?')
            break
        else:
            for varBind in varBinds:
                oid, value = varBind
                # 确认OID仍然在表的范围内
                if not str(oid).startswith(oid_base):
                    break
                results.append([oid.prettyPrint(), value.prettyPrint()])
                logging.debug('OID: %s, Value: %s', oid.prettyPrint(), value.prettyPrint())

    # 保存结果到CSV文件
    with open('snmp_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['OID', 'Value'])
        csvwriter.writerows(results)

    logging.info("完成，结果已保存到snmp_results.csv")

