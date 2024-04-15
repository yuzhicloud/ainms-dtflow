import pandas as pd
import logging


def load_data_to_database(engine, file_path, table_name):
    try:
        # Load data from CSV
        data = pd.read_csv(file_path)
        # Replace empty strings or missing values with None (which becomes NULL in SQL)
        data = data.replace({pd.NA: None, "": None})

        # Write data into the database table
        data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Data successfully loaded into database table {table_name}.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
