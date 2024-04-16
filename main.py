import os
import logging
from datetime import datetime

from sqlalchemy import create_engine, text
import yzsnmp
import shutil
import yzdb
from yzdb import process_ap_name_multithreaded, aptodb, apgtodb

# Define the clear_directory function to accept a path to clear
def clear_csv_directory(csv_dir):
    # Attempt to clear the directory
    for filename in os.listdir(csv_dir):
        file_path = os.path.join(csv_dir, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.error(f'Failed to delete {file_path}. Reason: {e}')


def setup_logging():
    # 确保 logs 目录存在
    try:
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"ainms_dtflow_{timestamp}.log"
        log_filepath = os.path.join(log_directory, log_filename)

        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=log_filepath,
            filemode='w'
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger().addHandler(console_handler)
        return log_filepath
    except Exception as e:
        print(f"Failed to set up logging: {e}")
        raise


# Database configuration
db_config = {
    'user': 'root',
    'password': 'RootPassword123!',
    'host': '192.168.22.5',
    'port': 13306,
    'database': 'ainms'
}


def create_db_engine(db_config):
    """Create and return a SQLAlchemy engine using the provided database configuration."""
    db_url = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    try:
        engine = create_engine(db_url)
        logging.info("Database connection successfully established.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise SystemExit(e)  # Exit if cannot connect to database


def truncate_table(engine, table_name):
    try:
        with engine.begin() as conn:  # 使用事务确保操作的完整性
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            logging.info(f"Table {table_name} has been truncated successfully.")
    except Exception as e:
        logging.error(f"Failed to truncate table {table_name}: {e}")


def main():
    log_filepath = setup_logging()
    logging.info(f"Logging to {log_filepath}")
    logging.info("Script started.")

    csv_dir = 'csvfiles'  # Relative path to csvfiles directory
    # Print current working directory and Python sys.path for debugging
    logging.info("Current working directory: %s", os.getcwd())

    clear_csv_directory(csv_dir)
    logging.info("CSV directory cleared.")

    # Call the SNMP main function and get the list of processed CSV files
    ips = ['10.170.69.101', '10.170.69.104', '10.170.69.107', '10.170.69.110']
    snmp_csv_files = yzsnmp.snmp_main(ips)
    logging.debug(f"Processed files: {snmp_csv_files}")

    # snmp_csv_files = ['snmp_table_data_10_170_69_101.csv',
    #                   'snmp_table_data_10_170_69_104.csv',
    #                   'snmp_table_data_10_170_69_107.csv',
    #                   'snmp_table_data_10_170_69_110.csv']
    # snmp_csv_files = ['snmp_table_data_10_170_69_100.csv',
    #                   'snmp_table_data_10_170_69_103.csv',
    #                   'snmp_table_data_10_170_69_106.csv',
    #                   'snmp_table_data_10_170_69_109.csv']

    allapg_file_path = process_ap_name_multithreaded(snmp_csv_files, csv_dir)
    logging.debug(f"allAPG file path: {allapg_file_path}")

    engine = create_db_engine(db_config)
    logging.debug(" Engine created successfully.")
    apg_table_name = 'access_point_group'
    ap_table_name = 'access_point'

    # 首先清空access_point_group表和access_point表
    truncate_table(engine, 'access_point')
    truncate_table(engine, 'access_point_group')

    apgtodb.load_data_to_database(engine, allapg_file_path, apg_table_name)
    ap_csv_files = aptodb.create_csv(csv_dir, allapg_file_path, snmp_csv_files)
    aptodb.insert_csv_data_to_db(engine, ap_csv_files, ap_table_name)

    logging.info("Data loaded to database successfully.")
    logging.info("Script execution complete.")


if __name__ == "__main__":
    main()
