import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

# 读取CSV文件
acdata_df = pd.read_csv('acdata_translated.csv')
output_df = pd.read_csv('temp/output.csv')

# 数据库连接信息
db_info = 'mysql+pymysql://root:RootPassword123!@192.168.1.230:13306/ainms'
engine = create_engine(db_info)

# 预处理output_df以便快速匹配
output_df['match_part'] = output_df['pp_name'].apply(lambda x: ''.join(x.split('-')))

# 逐行处理acdata_df
for index, row in acdata_df.iterrows():
    # 从hwWlanApGroup字段生成group_name_part
    group_name_part = ''.join(row['hwWlanApGroup'].split('-')) if not pd.isna(row['hwWlanApGroup']) else None

    # 使用np.isnan检查是否为NaN，因为直接比较NaN是不可靠的
    version = None if pd.isna(row['hwWlanApSoftwareVersion']) else row['hwWlanApSoftwareVersion']

    # 查找group_id
    group_id = None
    if group_name_part is not None:
        matched_group = output_df[output_df['match_part'].str.contains(group_name_part, case=False, na=False)]
        if not matched_group.empty:
            group_id = matched_group.iloc[0]['id']

    # 如果group_id为None，设置为特殊值900900
    group_id =  900900 if group_id is None else group_id

    # 准备SQL语句参数
    sql_params = {
        "nename": row['hwWlanApName'],
        "netype": row['hwWlanApTypeInfo'],
        "nemac": row['hwWlanApSn'],
        "version": version,
        "nestate": row['hwWlanApRunState'],
        "group_id": group_id
    }

    # 构建SQL插入语句
    insert_query = text("""
        INSERT INTO access_point
        (nedn, neid, aliasname, nename, necategory, netype, nevendorname, neesn, neip, nemac, version, nestate, createtime, neiptype, subnet, neosversion, group_id)
        VALUES
        (NULL, NULL, NULL, :nename, NULL, :netype, NULL, NULL, NULL, :nemac, :version, :nestate, NULL, NULL, NULL, NULL, :group_id)
    """)

    # 插入数据到数据库
    with engine.connect() as conn:
        conn.execute(insert_query, sql_params)


print("Data insertion completed.")
