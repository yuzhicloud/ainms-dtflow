import os
import pandas as pd
import logging


def ap_db_operation(engine, csv_dir, allAPG_file_path, snmp_csv_files, table_name):
    logging.info("Starting the data processing script...")

    # 读取映射数据
    try:
        # group_df = pd.read_csv('./csvfiles/allAPG.csv')
        group_df = pd.read_csv(allAPG_file_path)
        # 直接使用name字段的完整内容作为键创建映射字典
        group_mapping = pd.Series(group_df.id.values, index=group_df.name).to_dict()
        logging.info("Group mapping loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load group mapping: {e}")
        raise SystemExit(e)

    # 文件处理和数据插入
    file_names = snmp_csv_files

    for file_name in file_names:
        file_path = f"{csv_dir}/{file_name}"
        if not os.path.exists(file_path):
            logging.warning(f"File {file_path} does not exist. Skipping...")
            continue

        try:
            df = pd.read_csv(file_path)
            logging.info(f"Processing file {file_name}.")

            # 映射字段...
            df = df.rename(columns={
                'hwWlanApSn': 'neesn',
                'hwWlanApName': 'nename',
                'hwWlanApTypeInfo': 'netype',
                'hwWlanApRunState': 'nestate',
                'hwWlanApSoftwareVersion': 'version',
            })

            # 使用hwWlanApGroup的完整内容进行匹配来设置group_id
            df['group_id'] = df['hwWlanApGroup'].map(group_mapping).fillna(404).astype(int)

            # 遍历DataFrame中的每一行，为每一行记录一条插入前的日志
            for index, row in df.iterrows():
                logging.debug(f"Inserting: neesn={row['neesn']}, nename={row['nename']}, netype={row['netype']}, nestate={row['nestate']}, version={row['version']}, group_id={row['group_id']}")

            # 插入数据库
            df[['nemac', 'nename', 'netype', 'nestate', 'version', 'group_id']].to_sql('access_point', con=engine, if_exists='append', index=False)
            logging.info(f"Data from {file_name} successfully inserted into the database.")
        except Exception as e:
            logging.error(f"Failed to process file {file_name}: {e}")
