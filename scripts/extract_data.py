#!/usr/bin/env python3
"""
Generates a series of dbt data tests for all objects
that were built in dbt prior to the larger datawarehouse migration.
The tests will run comparisons on denormalized data of tables to ensure
the results match between replicated and migrated objects.

All necessary configs need to be set in the main function.

The following modules are required:
snowflake-connector-python
snowflake-connector-python[pandas]
progressbar

To execute from root of repository: python scripts/extract_2022_data.py

"""

import os, re
import logging
import snowflake.connector
import pandas as pd
import progressbar
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from datetime import datetime

logging.basicConfig(
    filename='logs/extract_data.log',
    level=logging.INFO,
    format='%(asctime)s-%(levelname)s-%(message)s'
)

__author__ = ["John Rensberger"]
__version__ = "1.0"
__email__ = ["john.rensberger@redpillanalytics.com"]
__status__ = "Development"

def get_dir(args):
    cwd = os.getcwd()
    path = cwd

    if args:
        path = f"{cwd}{args}"
        if os.path.exists(path):
            return path
        else:
            os.makedirs(path)
            if os.path.exists(path):
                return path
    else:
        return path

def clear_existing_compares(path):
    """
    Removes all files in the supplied path

    Parameters:
    path(str): Path to create the comparison test sql file in
    """

    logging.info(f'Removing all files from {path}')
    print(f'Removing all files from {path}.')

    os_path = get_dir(path)
    for root, dirs, files in os.walk(os_path):
        for file in files:
            os.remove(os.path.join(root,file))
            logging.info(f'Removing file {file}.')
            print(f'Removing file {file}.')

def sf_connection_using_keypair(key_path, key_password, account, user, role, warehouse, database):
    """
    Returns a Snowflake connection to be used as needed.

    Parameters:
    key_path: The fully qualified path to your private key.
    key_password: Use 'None', otherwise os.environ['PRIVATE_KEY_PASSPHRASE'].encode()
    account: Snowflake account
    user: Snowflake user
    role: Snowflake Role
    warehouse: Snowflake Warehouse
    database: Snowflake Database

    Returns:
    SnowflakeConnection: A connection to Snowflake to be used as needed with a cursor.
    """
    
    with open(key_path, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            key_password,
            backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    
    ctx = snowflake.connector.connect(
        user=user,
        account=account,
        private_key=pkb,
        warehouse=warehouse,
        database=database,
        role=role
    )
    
    logging.info('Snowflake connection successful.')

    return ctx

def sf_connection(account, user, password, role, warehouse, database):
    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        role=role
    )
    
    logging.info('Snowflake connection successful.')

    return ctx

def sf_connection_close(connection):
    """
    A simple function to help remind us to close the connection

    Input:
    connection: Snowflake Connection
    """
    
    connection.close()

    logging.info('Snowflake connection closed.')

def get_data(connection, path):
    """
    Inputs:
    connection: Snowflake Connection to use
    path(str): Path to create the comparison test sql file in
    """

    query = f"""
        with
        calendar_day as (select * from intermediate.int_conformed_calendar_day),
        raw as (select * from raw.school_learning_modalities),

        final as (
        
            select
                'd' || calendar_day.year_number || '-' || lpad(calendar_day.quarter_of_year, 2, '0') || '-id' || raw.district_nces_id || '.csv' as filename,
                calendar_day.year_number,
                calendar_day.quarter_of_year,
                raw.district_nces_id
            from raw
            inner join calendar_day on to_date(raw.week) = calendar_day.date_day
            -- where 1=1
            -- and calendar_day.year_number = 2021
            -- and calendar_day.quarter_of_year = 4
            -- and raw.district_nces_id = '0400005'
            group by 1,2,3,4
        )
        select * from final;
    """

    logging.debug(f'Retrieving list of districts by quarter: {query}')
    print('Retrieving list of districts by quarter.')
    
    # results = connection.cursor().execute(query).fetchmany(1)
    results = connection.cursor().execute(query).fetchall()
    
    # Following Snowflake adapter convention of using a tuple instead of a list 
    object = ()
    objects = []
    num_files = 0
    for (filename, year_number, quarter_of_year, district_nces_id) in results:
        num_files = num_files + 1
        object = (filename, year_number, quarter_of_year, district_nces_id)
        objects.append(object)
    
    logging.info(f'Retrieved {num_files} files to create.')
    print(f'Retrieved {num_files} files to create.')
    connection.cursor().close()

    num_files_complete = 0
    bar = progressbar.ProgressBar(maxval=num_files, \
    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
    bar.start()
    
    for filename, year_number, quarter_of_year, district_nces_id in objects:

        query = f"""
            with
            calendar_day as (select * from intermediate.int_conformed_calendar_day),
            raw as (select * from raw.school_learning_modalities),

            final as (

                select
                    raw.district_nces_id,
                    raw.district_name,
                    raw.week,
                    raw.learning_modality,
                    raw.operational_schools,
                    raw.student_count,
                    raw.city,
                    raw.state,
                    raw.zip_code
                from raw
                inner join calendar_day on to_date(raw.week) = calendar_day.date_day
                where
                    calendar_day.year_number = {year_number}
                    and calendar_day.quarter_of_year = {quarter_of_year}
                    and raw.district_nces_id = {district_nces_id}

            )
            select * from final
        """
        logging.debug(f'Retrieving rows for district {district_nces_id} and quarter {quarter_of_year}.')
        results = connection.cursor().execute(query)
        num_rows = results.rowcount
        if num_rows == 0:
            logging.info(f'ERROR - Unable to retrieve data for district {district_nces_id} and quarter {quarter_of_year}: {query}.')
            print(f'ERROR - Unable to retrieve data for district {district_nces_id} and quarter {quarter_of_year}: {query}.')
            exit()
        
        results.fetch_pandas_all().to_csv(f'{path}/{year_number}/{quarter_of_year}/{filename}', index=False)

        num_files_complete = num_files_complete + 1
        # Show progress
        bar.update(num_files_complete)

    connection.cursor().close()

    bar.finish()

def main():

    logging.info('Started processing extract_data.')
    print('Started processing extract_data.')

    output_path = '~/git/dbt_coalesce_workshop/extracts'

    # Get a connection to Snowflake
    # sf = sf_connection_using_keypair(
    #     '',   # key_path
    #     None, # key_password
    #     '',   # account
    #     '',   # user
    #     '',   # role
    #     '',   # warehouse
    #     ''    # database
    # )


    sf = sf_connection(
        '', # account
        '', # user
        '', # password
        '', # role
        '', # warehouse
        ''  # database
    )

    # Delete existing tests in case any were deprecated
    # clear_existing_compares(output_path)

    # Get list of objects
    objects = get_data(sf, output_path)

    # Close the connection to Snowflake
    sf_connection_close(sf)

    logging.info('End of processing.')
    print('End of processing.')

if __name__ == '__main__':
    main()
