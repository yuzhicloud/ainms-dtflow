from yzsnmp import apInfo


def print_hi(name):
    print(f'Hi, {name}')

if __name__ == '__main__':
    print_hi('PyCharm')

print("begin to get snmp data")
apInfo.get_snmp_data();
