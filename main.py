from yzsnmp.apinfo import snmp_main
from yzsql import csvToDatabase



def main():
    ips = ['10.170.69.101', '10.170.69.104', '10.170.69.107', '10.170.69.110']
    # 如果没有csvfiles目录，则创建
    csv_dir = os.path.join('.', 'csvfiles')
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)  # 创建目录

    # snmp_main,通过snmp协议获取AC数据，并保存到csv文件
    snmp_csv_files = snmp_main(ips)
    print("Generated CSV files:", csv_files)

    # csvToCn,把csv文件里面的16进制转成中文
    hex_to_chinese(snmp_csv_files);

if __name__ == "__main__":
    main()