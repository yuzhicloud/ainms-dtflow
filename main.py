from yzsnmp.apInfo import snmp_main
from yzsql import csvToDatabase


def main():
    # 调用snmp_main函数，并接收返回的CSV文件名
    csv_file_name = snmp_main()
    print(f"Generated CSV file: {csv_file_name}")

    # 使用这个CSV文件名作为参数，调用csvToDatabase函数进行数据处理
    # csvToDatabase(csv_file_name)


if __name__ == "__main__":
    main()
