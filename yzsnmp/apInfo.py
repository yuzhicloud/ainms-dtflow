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

    # 设置起始OID为表的开始
    oid_base = '1.3.6.1.4.1.2011.6.139.13.3.3.1.1'
    # 需要捕获的最后一个OID的开始部分，用于判断何时结束遍历
    oid_end = '1.3.6.1.4.1.2011.6.139.13.3.3.1.19'

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
                              0, 50,  # 非重复者，最大重复者
                              ObjectType(ObjectIdentity(oid_base)),
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
                # 判断返回的OID是否超出了表的范围
                if str(oid).startswith(oid_end):
                    break
                results.append([oid.prettyPrint(), value.prettyPrint()])
                logging.debug('OID: %s, Value: %s', oid.prettyPrint(), value.prettyPrint())
            else:
                # 如果未跳出循环，则继续
                continue
            # 如果已处理所有数据，则跳出while循环
            break

    # 保存结果到CSV文件
    with open('snmp_results.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['OID', 'Value'])
        csvwriter.writerows(results)

    logging.info("完成，结果已保存到snmp_results.csv")
