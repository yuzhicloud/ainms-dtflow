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

    # 起始OID
    oid_start = '1.3.6.1.4.1.2011.6.139.13.3.3.1.1'
    # 结束OID，用于判断何时停止遍历
    oid_end = '1.3.6.1.4.1.2011.6.139.13.3.3.1.18'

    results = []

    for (errorIndication,
         errorStatus,
         errorIndex,
         varBinds) in bulkCmd(SnmpEngine(),
                              UsmUserData(user, authKey, privKey,
                                          authProtocol=authProtocol,
                                          privProtocol=privProtocol),
                              UdpTransportTarget((ip, port)),
                              ContextData(),
                              0, 25,  # 非重复者，最大重复者
                              ObjectType(ObjectIdentity(oid_start)),
                              lexicographicMode=False,  # 不超出指定的OID范围
                              lookupMib=False):

        if errorIndication:
            logging.error(errorIndication)
            break
        elif errorStatus:
            logging.error('%s at %s', errorStatus.prettyPrint(), errorIndex and varBinds[int(errorIndex)-1][0] or '?')
            break
        else:
            for varBind in varBinds:
                oid, value = varBind
                if not str(oid).startswith(oid_start[:oid_start.rfind('.')]):
                    # 如果超出了表的范围，则停止
                    break
                results.append([oid.prettyPrint(), value.prettyPrint()])
                logging.debug('OID: %s, Value: %s', oid.prettyPrint(), value.prettyPrint())

        if not str(oid).startswith(oid_start[:oid_start.rfind('.')]):
            # 确保当遍历到表的末尾时跳出外层循环
            break

    # 保存结果到CSV文件
    with open('snmp_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['OID', 'Value'])
        csvwriter.writerows(results)

    logging.info("完成，结果已保存到snmp_results.csv")

