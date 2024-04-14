import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# 初始化日志系统
logging.basicConfig(level=logging.DEBUG, filename='app.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 尝试记录一个启动日志，确认日志系统工作正常
logging.info("Starting the data processing script...")

# 数据库配置
db_config = {
    'user': 'root',
    'password': 'RootPassword123!',
    'host': '192.168.1.230',
    'port': 13306,
    'database': 'ainms'
}

# 尝试连接数据库
try:
    db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    engine = create_engine(db_url)
    logging.info("Database connection successfully established.")
except Exception as e:
    logging.error(f"Failed to connect to the database: {e}")
    raise SystemExit(e)  # 终止脚本运行，因为没有数据库连接脚本无法继续

# 读取映射数据
try:
    group_df = pd.read_csv('./csvfiles/apgallnew.csv')
    # 直接使用name字段的完整内容作为键创建映射字典
    group_mapping = pd.Series(group_df.id.values, index=group_df.name).to_dict()
    logging.info("Group mapping loaded successfully.")
except Exception as e:
    logging.error(f"Failed to load group mapping: {e}")
    raise SystemExit(e)

# 文件处理和数据插入
file_names = ['apg101.csv', 'apg104.csv', 'apg107.csv', 'apg110.csv']
csv_dir = './csvfiles'

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
            'hwWlanApSn': 'nemac',
            'hwWlanApName': 'nename',
            'hwWlanApTypeInfo': 'netype',
            'hwWlanApRunState': 'nestate',
            'hwWlanApSoftwareVersion': 'version',
        })

        # 使用hwWlanApGroup的完整内容进行匹配来设置group_id
        df['group_id'] = df['hwWlanApGroup'].map(group_mapping).fillna(404).astype(int)

        # 遍历DataFrame中的每一行，为每一行记录一条插入前的日志
        for index, row in df.iterrows():
            logging.debug(f"Inserting: nemac={row['nemac']}, nename={row['nename']}, netype={row['netype']}, nestate={row['nestate']}, version={row['version']}, group_id={row['group_id']}")

        # 插入数据库
        df[['nemac', 'nename', 'netype', 'nestate', 'version', 'group_id']].to_sql('access_point', con=engine, if_exists='append', index=False)
        logging.info(f"Data from {file_name} successfully inserted into the database.")
    except Exception as e:
        logging.error(f"Failed to process file {file_name}: {e}")
