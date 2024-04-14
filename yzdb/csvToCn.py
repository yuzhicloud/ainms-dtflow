import pandas as pd
import os

def hex_to_chinese(hex_str):
    if not isinstance(hex_str, str):
        return "格式错误"
    hex_str = hex_str.replace('0x', '').lower()
    if not hex_str or len(hex_str) % 2 != 0:
        return "非有效16进制值"
    try:
        bytes_str = bytes.fromhex(hex_str)
    except ValueError:
        return "包含非16进制字符"
    try:
        return bytes_str.decode('utf-8')
    except UnicodeDecodeError:
        return "解码失败"

    # 指定需要处理的文件名
    file_names = ['101.csv', '104.csv', '107.csv', '110.csv']
    csv_dir = './csvfiles'

    for file_name in file_names:
        file_path = os.path.join(csv_dir, file_name)
        # 检查文件是否为空或存在
        if os.path.exists(file_path) and os.stat(file_path).st_size > 0:
            df = pd.read_csv(file_path)
            # 检查'hwWlanApGroup'列是否存在
            if 'hwWlanApGroup' in df.columns:
                df['hwWlanApGroup'] = df['hwWlanApGroup'].apply(hex_to_chinese)
                new_filename = os.path.join(csv_dir, 'apg' + file_name)
                df.to_csv(new_filename, index=False)
            else:
                print(f"'hwWlanApGroup'列在文件{file_name}中不存在，跳过处理。")
        else:
            print(f"文件{file_name}为空或不存在，跳过处理。")
