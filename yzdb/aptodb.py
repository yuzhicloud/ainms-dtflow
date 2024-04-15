import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc
import logging


def preprocess_data(df, group_mapping):
    # 保留所需字段并重命名列以符合数据库表结构
    df = df[['hwWlanApSn', 'hwWlanApName', 'hwWlanApTypeInfo', 'hwWlanApRunState', 'hwWlanApSoftwareVersion']]
    df.rename(columns={
        'hwWlanApSn': 'neesn',
        'hwWlanApName': 'nename',
        'hwWlanApTypeInfo': 'netype',
        'hwWlanApRunState': 'nestate',
        'hwWlanApSoftwareVersion': 'version',
    }, inplace=True)

    # 映射 group_id
    df['group_id'] = df['nename'].map(group_mapping).fillna(404).astype(int)
    df['neesn'].replace({np.nan: "unknown"}, inplace=True)  # Replace NaN with "unknown" or another placeholder
    return df


def load_group_mapping(allAPG_file_path):
    group_df = pd.read_csv(allAPG_file_path)
    return pd.Series(group_df.id.values, index=group_df.name).to_dict()


def ap_db_operation(engine, csv_dir, allAPG_file_path, snmp_csv_files, table_name):
    logging.info("开始数据处理...")

    # 加载 group mapping
    group_mapping = load_group_mapping(allAPG_file_path)

    for file_name in snmp_csv_files:
        file_path = f"{csv_dir}/{file_name}"
        df = pd.read_csv(file_path)
        df = preprocess_data(df, group_mapping)
        logging.info(f"处理文件 {file_name}.")

        # 尝试逐条插入数据
        successful_count = 0
        for index, row in df.iterrows():
            try:
                # 将单条记录转换为 DataFrame 并插入数据库
                single_row_df = pd.DataFrame([row])
                single_row_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
                successful_count += 1
            except exc.IntegrityError as e:
                logging.error(f"插入失败 - 完整性错误，数据: {row.to_dict()}")
            except Exception as e:
                logging.error(f"插入失败 - 意外错误，数据: {row.to_dict()}")

        logging.info(f"成功插入 {successful_count} 条记录到数据库。")

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
