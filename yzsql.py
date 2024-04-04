import csv
# import mysql.connector


def csvToDatabase(csv_file_name):
    # 数据库连接配置，根据实际情况填写
    db_config = {
        'user': 'your_username',
        'password': 'your_password',
        'host': 'localhost',
        'database': 'your_database_name',
        'raise_on_warnings': True
    }

    # try:
    #     # 连接数据库
    #     conn = mysql.connector.connect(**db_config)
    #     cursor = conn.cursor()
    #
    #     # 打开CSV文件
    #     with open(csv_file_name, 'r', newline='', encoding='utf-8') as csvfile:
    #         reader = csv.reader(csvfile)
    #         next(reader)  # 跳过标题行
    #         for row in reader:
    #             # 准备插入数据的SQL语句，假设apinfo表结构与CSV列直接对应
    #             insert_sql = """
    #             # INSERT INTO apinfo (hwWlanApMac, hwWlanApSn, hwWlanApTypeInfo, hwWlanApName, hwWlanApGroup,
    #             # hwWlanApRunState, hwWlanApSoftwareVersion, hwWlanApHardwareVersion, hwWlanApCpuType,
    #             # hwWlanApCpufrequency, hwWlanApMemoryType, hwWlanApDomain, hwWlanApIpAddress,
    #             # hwWlanApIpNetMask, hwWlanApGatewayIp, hwWlanApMemorySize, hwWlanApFlashSize, hwWlanApRunTime)
    #             # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #             # """
    #             # 执行SQL语句
    #             cursor.execute(insert_sql, tuple(row))
    #
    #         # 提交事务
    #         conn.commit()
    #
    # except mysql.connector.Error as err:
    #     print(f"Failed to insert data: {err}")
    # finally:
    #     if conn.is_connected():
    #         cursor.close()
    #         conn.close()
    #         print("MySQL connection is closed")
