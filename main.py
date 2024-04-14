import os
import sys
import logging
import yzsnmp
import shutil


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


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

    # Directory for CSV files
    csv_dir = './csvfiles'  # Relative path to csvfiles directory

    # Print current working directory and Python sys.path for debugging
    print("Current working directory:", os.getcwd())
    print("Python sys.path:", sys.path)

    # Clear the CSV directory
    clear_directory(csv_dir)
    logging.info("CSV directory cleared.")

    # Call the SNMP main function and get the list of processed CSV files
    ips = ['10.170.69.101', '10.170.69.104', '10.170.69.107', '10.170.69.110']
    snmp_csv_files = yzsnmp.snmp_main(ips)
    print("Processed files:", snmp_csv_files)


if __name__ == "__main__":
    main()
