import logging
import os

import pandas as pd
import numpy as np
import logging


def create_csv(csv_dir, allapg_file_path, snmp_csv_files):
    # Load apg.csv
    apg_df = pd.read_csv(allapg_file_path)
    logging.debug("Loaded apg.csv")

    output_files = []  # 初始化存储生成的文件名列表
    for snmp_file_name in snmp_csv_files:
        snmp_file_path = os.path.join(csv_dir, snmp_file_name)  # Ensure the path is correctly constructed
        logging.debug(f"Processing file: {snmp_file_path}")

        # Load each snmp.csv file
        snmp_df = pd.read_csv(snmp_file_path)
        logging.debug(f"Loaded {snmp_file_path}")

        # Create a new 'group_id' column in snmp_df, initialize with default 404
        snmp_df['group_id'] = 404

        # Iterate over apg_df to find matches and update 'group_id'
        for index, apg_row in apg_df.iterrows():
            # Find rows in snmp_df where 'hwWlanApGroup' contains the 'name' from apg_df
            mask = snmp_df['hwWlanApGroup'].str.contains(apg_row['name'], na=False)
            matched_count = snmp_df.loc[mask].shape[0]
            if matched_count > 0:
                logging.debug(f"Found {matched_count} matches for {apg_row['name']}")
            snmp_df.loc[mask, 'group_id'] = apg_row['id']

        # Replace NaN with None for proper NULL handling in SQL
        snmp_df = snmp_df.where(pd.notnull(snmp_df), None)

        # Construct output filename
        output_filename = os.path.join(csv_dir, snmp_file_name.replace('.csv', '_db.csv'))
        logging.debug(f"Saving to {output_filename}")
        # Save the updated DataFrame to a new CSV file
        snmp_df.to_csv(output_filename, index=False)
        logging.info(f"File saved: {output_filename}")

        output_files.append(output_filename)

    return output_files


def insert_csv_data_to_db(engine, csv_files, table_name, if_exists='append', index=False):

    for csv_file in csv_files:
        logging.debug(f"Loading data from {csv_file}")

        # 读取CSV文件
        df = pd.read_csv(csv_file)

        # 重命名列以匹配数据库字段
        df.rename(columns={
            'hwWlanApMac': 'nemac',
            'hwWlanApSn': 'neesn',
            'hwWlanApTypeInfo': 'netype',
            'hwWlanApName': 'nename',
            'hwWlanApRunState': 'nestate',
            'hwWlanApSoftwareVersion': 'version',
            'group_id': 'group_id'  # 确保这个字段也正确映射
        }, inplace=True)

        df.replace({'': None, 'NULL': None, 'null': None}, inplace=True)
        # 确保DataFrame包含数据库表的所有列，未提及的列设置为None
        expected_columns = ['id', 'nedn', 'neid', 'aliasname', 'nename', 'necategory', 'netype',
                            'nevendorname', 'neesn', 'neip', 'nemac', 'version', 'nestate',
                            'createtime', 'neiptype', 'subnet', 'neosversion', 'group_id']
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # 筛选出期望的列，防止因额外的列引起错误
        df = df[expected_columns]

        try:
            # 插入数据到数据库表
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=index)
            logging.info(f"Data from {csv_file} has been successfully loaded into the database table {table_name}.")
        except Exception as e:
            logging.error(f"Failed to load data from {csv_file} into database. Error: {e}")
