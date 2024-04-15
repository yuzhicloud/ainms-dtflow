import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc
import logging
from sqlalchemy.orm import sessionmaker


def preprocess_data(df, group_mapping):
    # 只保留需要的字段
    df = df[['hwWlanApSn', 'hwWlanApName', 'hwWlanApTypeInfo', 'hwWlanApRunState', 'hwWlanApSoftwareVersion']].copy()

    # 重命名列以符合数据库表结构
    df.rename(columns={
        'hwWlanApSn': 'neesn',
        'hwWlanApName': 'nename',
        'hwWlanApTypeInfo': 'netype',
        'hwWlanApRunState': 'nestate',
        'hwWlanApSoftwareVersion': 'version',
    }, inplace=True)

    # 映射 group_id
    df['group_id'] = df['nename'].map(group_mapping).fillna(404).astype(int)
    return df


def load_group_mapping(allAPG_file_path):
    group_df = pd.read_csv(allAPG_file_path)
    return pd.Series(group_df.id.values, index=group_df.name).to_dict()


def insert_data(df, engine, table_name):
    from sqlalchemy.exc import IntegrityError
    inserted_count = 0
    for index, row in df.iterrows():
        # 将单行数据转换为 DataFrame
        row_df = pd.DataFrame([row])
        try:
            row_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            inserted_count += 1
        except IntegrityError as e:
            logging.error(f"Integrity error occurred when inserting {row['neesn']}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred when inserting {row['neesn']}: {e}")

    logging.info(f"Successfully inserted {inserted_count} new records.")


def ap_db_operation(engine, csv_dir, allAPG_file_path, snmp_csv_files, table_name):
    group_mapping = load_group_mapping(allAPG_file_path)

    for file_name in snmp_csv_files:
        file_path = f"{csv_dir}/{file_name}"
        df = pd.read_csv(file_path)
        df = preprocess_data(df, group_mapping)
        insert_data(df, engine, table_name)

# def ap_db_operation(engine, csv_dir, allAPG_file_path, snmp_csv_files, table_name):
#     logging.info("Starting the data processing script...")
#
#     # Load group mapping
#     group_df = pd.read_csv(allAPG_file_path)
#     group_mapping = pd.Series(group_df.id.values, index=group_df.name).to_dict()
#
#     # Process each file
#     for file_name in snmp_csv_files:
#         file_path = f"{csv_dir}/{file_name}"
#         df = pd.read_csv(file_path)
#         df = preprocess_data(df)
#         logging.info(f"Processing file {file_name}.")
#
#         # Map fields and filter duplicates
#         df.rename(columns={
#             'hwWlanApSn': 'neesn',
#             'hwWlanApName': 'nename',
#             'hwWlanApTypeInfo': 'netype',
#             'hwWlanApRunState': 'nestate',
#             'hwWlanApSoftwareVersion': 'version',
#         }, inplace=True)
#         df['group_id'] = df['hwWlanApGroup'].map(group_mapping).fillna(404).astype(int)
#
#         # 尝试逐条插入数据
#         successful_count = 0
#         for index, row in df.iterrows():
#             try:
#                 # 将单条记录转换为DataFrame
#                 single_row_df = pd.DataFrame([row])
#                 single_row_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
#                 successful_count += 1
#             except exc.IntegrityError as e:
#                 logging.error(f"插入失败 - 完整性错误，数据: {row.to_dict()}")
#             except Exception as e:
#                 logging.error(f"插入失败 - 意外错误，数据: {row.to_dict()}")
#
#         logging.info(f"成功插入 {successful_count} 条记录到数据库。")
#
#         # # Filter to avoid inserting duplicates
#         # existing_neesns = pd.read_sql(f"SELECT neesn FROM {table_name} WHERE neesn IS NOT NULL", con=engine)[
#         #     'neesn'].tolist()
#         #
#         # # 在过滤重复数据之前和之后，添加日志输出记录数据量
#         # logging.info(f"Original data count from file {file_name}: {len(df)}")
#         # df = df[~df['neesn'].isin(existing_neesns)]
#         # logging.info(f"Data count after removing duplicates from file {file_name}: {len(df)}")
#         #
#         # # Select only the columns needed for the database insert
#         # columns_to_insert = ['neesn', 'nename', 'netype', 'nestate', 'version', 'group_id']
#         # df = df[columns_to_insert]
#         #
#         # Session = sessionmaker(bind=engine)
#         # session = Session()
#         #
#         # if not df.empty:
#         #     try:
#         #         batch_size = 1000  # 可以调整这个大小进行测试
#         #         for start in range(0, len(df), batch_size):
#         #             end = start + batch_size
#         #             batch_df = df[start:end]
#         #             try:
#         #                 batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
#         #                 logging.info(f"Successfully inserted batch from {start} to {end} of {file_name}.")
#         #             except Exception as e:
#         #                 logging.error(f"Failed to insert batch from {start} to {end} of {file_name}: {e}")
#         #     except exc.IntegrityError as e:
#         #         logging.error(f"Failed to insert data due to integrity error: {e}")
#         #     except exc.SQLAlchemyError as e:  # 捕捉更广泛的SQLAlchemy相关错误
#         #         logging.error(f"SQLAlchemy error occurred: {e}")
#         #     except Exception as e:
#         #         logging.error(f"An unexpected error occurred during data insertion from {file_name}: {e}")
#         #
#         # else:
#         #     logging.info(f"No new data to insert from {file_name}.")