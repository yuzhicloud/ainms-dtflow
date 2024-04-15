import pandas as pd
import os
import threading
import logging

# 全局变量用于 ID 分配
next_id = 1000
id_lock = threading.Lock()


def process_file(file_name, controller_id, powerplant_dict, csv_dir, dfs_list):
    global next_id
    file_path = os.path.join(csv_dir, file_name)
    logging.debug(f'Processing file: {file_path}')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        logging.debug(f"Successfully processed the file: {file_path}")
        matched_rows = []
        for _, row in df.iterrows():
            for power_plant_name, power_plant_id in powerplant_dict.items():
                if power_plant_name in row['hwWlanApGroup']:
                    with id_lock:
                        current_id = next_id
                        next_id += 1
                    matched_rows.append([current_id, row['hwWlanApGroup'], controller_id, power_plant_id])
                    break

        if matched_rows:
            new_df = pd.DataFrame(matched_rows, columns=['id', 'name', 'controller_id', 'power_plant_id'])
            new_df = new_df.drop_duplicates(subset=['name', 'controller_id', 'power_plant_id'])
            dfs_list.append(new_df)


def process_ap_name_multithreaded(snmp_csv_files, csv_dir):
    # 打印当前工作目录
    print("Current working directory:", os.getcwd())
    # 获取当前文件的目录路径
    dir_path = os.path.dirname(__file__)
    logging.debug(dir_path)
    # 构建 csv 文件的完整路径
    csv_file_path = os.path.join(dir_path, 'pp.csv')
    logging.debug(csv_file_path)
    try:
        powerplant_df = pd.read_csv(csv_file_path)
        logging.debug("File loaded successfully.")
        powerplant_dict = pd.Series(powerplant_df.id.values, index=powerplant_df.power_plant_name).to_dict()
        file_controller_id_map = {
            'snmp_table_data_10_170_69_100.csv': 1500,
            'snmp_table_data_10_170_69_103.csv': 1501,
            'snmp_table_data_10_170_69_106.csv': 1502,
            'snmp_table_data_10_170_69_109.csv': 1503
        }
        threads = []
        dfs_list = []

        # 使用线程锁来同步对dfs_list的访问
        dfs_lock = threading.Lock()

        logging.debug('Start processing files')
        for file_name, controller_id in file_controller_id_map.items():
            thread = threading.Thread(target=process_file, args=(file_name, controller_id, powerplant_dict, csv_dir, dfs_list))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # After all threads complete their execution
        if dfs_list:
            with dfs_lock:
                final_df = pd.concat(dfs_list, ignore_index=True)
                final_file_path = os.path.join(csv_dir, 'allAPG.csv')
                final_df.to_csv(final_file_path, index=False)
                return final_file_path  # Return the path to the CSV file instead of the DataFrame

        logging.debug('finish processing files:')

    except FileNotFoundError:
        print(f'CSV file not found at: {csv_file_path}')
    except PermissionError:
        print(f'Permission denied for file: {csv_file_path}')
    except pd.errors.EmptyDataError:
        print('CSV file is empty or does not contain any columns.')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
