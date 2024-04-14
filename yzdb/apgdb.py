import pandas as pd
import os

# 读取powerplant.csv文件
powerplant_df = pd.read_csv('./csvfiles/powerplant.csv')
# 创建一个从power_plant_name到id的映射字典
powerplant_dict = pd.Series(powerplant_df.id.values, index=powerplant_df.power_plant_name).to_dict()

# 文件和对应的controller_id的映射
file_controller_id_map = {
    'apg101.csv': 1500,
    'apg104.csv': 1501,
    'apg107.csv': 1502,
    'apg110.csv': 1503
}

# 初始化新文件的id起始值
start_id = 1000

# 用于存储所有DataFrame的列表
all_dfs = []

csv_dir = './csvfiles'
for file_name, controller_id in file_controller_id_map.items():
    file_path = os.path.join(csv_dir, file_name)
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # 筛选出包含power_plant_name的记录
        matched_rows = []
        for _, row in df.iterrows():
            for power_plant_name, power_plant_id in powerplant_dict.items():
                if power_plant_name in row['hwWlanApGroup']:
                    matched_rows.append([start_id, row['hwWlanApGroup'], controller_id, power_plant_id])
                    start_id += 1
                    break

        # 如果有匹配的记录，则生成新的DataFrame并去除重复项
        if matched_rows:
            new_df = pd.DataFrame(matched_rows, columns=['id', 'name', 'controller_id', 'power_plant_id'])
            new_df = new_df.drop_duplicates(subset=['name', 'controller_id', 'power_plant_id'])
            all_dfs.append(new_df)  # 将DataFrame添加到列表中
            new_file_name = 'new' + file_name
            new_df.to_csv(os.path.join(csv_dir, new_file_name), index=False)

# 合并所有DataFrame并保存为一个文件
if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.to_csv(os.path.join(csv_dir, 'apgallnew.csv'), index=False)
