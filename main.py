import os
import logging
from sqlalchemy import create_engine
import yzsnmp
import shutil
import yzdb
from yzdb import process_ap_name_multithreaded, aptodb, apgtodb

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the clear_directory function to accept a path to clear
def clear_directory(csv_dir):
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


def main():
    csv_dir = 'csvfiles'  # Relative path to csvfiles directory
    # # Print current working directory and Python sys.path for debugging
    # logging.debug("Current working directory:", os.getcwd())
    #
    # clear_directory(csv_dir)
    # logging.info("CSV directory cleared.")
    #
    # # Call the SNMP main function and get the list of processed CSV files
    # ips = ['10.170.69.101', '10.170.69.104', '10.170.69.107', '10.170.69.110']
    # snmp_csv_files = yzsnmp.snmp_main(ips)
    # logging.debug("Processed files:", snmp_csv_files)

    snmp_csv_files = ['snmp_table_data_10_170_69_101.csv',
                      'snmp_table_data_10_170_69_104.csv',
                      'snmp_table_data_10_170_69_107.csv',
                      'snmp_table_data_10_170_69_110.csv']

    allapg_file_path = process_ap_name_multithreaded(snmp_csv_files, csv_dir)
    logging.debug("allAPG file path: %s", allapg_file_path)

    engine = create_db_engine(db_config)
    logging.debug(" Engine created successfully.")
    csv_file_path = allapg_file_path
    apg_table_name = 'access_point_group'
    ap_table_name = 'access_point'
    apgtodb.load_data_to_database(engine, csv_file_path, apg_table_name)
    aptodb.ap_db_operation(engine, csv_dir, csv_file_path, snmp_csv_files, ap_table_name)

    logging.info("Data loaded to database successfully.")
    logging.info("Script execution complete.")


if __name__ == "__main__":
    main()
