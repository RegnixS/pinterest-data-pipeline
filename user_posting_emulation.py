# %%
"""User Posting Emulation Module

This module implements an emulation of users randomly posting Pins to Pinterest at random intervals.
"""
from dotenv import load_dotenv
import json
import os
import random
import requests
import sqlalchemy
from time import sleep


load_dotenv()

random.seed(100)


class AWSDBConnector:
    """
    This class is used to connect to a database.
    
    Attributes:
        HOST (string): The URL to the MQSQL database hosted in RDS.
        DATABASE (string) : Database name.
        PORT (string) : Database port number.
        USER (string) : Database user name.
        PASSWORD (string) : Database user password (from .ENV).
    """
    def __init__(self):
        """
            Initializes DatabaseConnector.
        """
        self.HOST = os.getenv('MYSQL_HOST')
        self.DATABASE = os.getenv('MYSQL_DATABASE')
        self.PORT = os.getenv('MYSQL_PORT')
        self.USER = os.getenv('MYSQL_USER')
        self.PASSWORD = os.getenv('MYSQL_PASSWORD')
        
    def create_db_connector(self):
        """
        Creates the database engine to use for the connection to the MYSQL database.
        
        Returns:
            Sqlalchemy Engine: Engine for connecting to the MYSQL database.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    """
    Infinitely loops and pulls a random set of records from the MYSQL database and sends them to the API Gateway. 
    """
    x = 0
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        x = x + 1

        with engine.connect() as connection:

            pin_result = get_data_from_rds(connection, "pinterest_data", random_row)

            geo_result = get_data_from_rds(connection, "geolocation_data", random_row)
        
            user_result = get_data_from_rds(connection, "user_data", random_row)

        # Convert the datetime.datetime objects to make them serializable
        geo_result["timestamp"] = geo_result["timestamp"].strftime('%Y-%m-%d %H:%M:%S')
        user_result["date_joined"] = user_result["date_joined"].strftime('%Y-%m-%d %H:%M:%S')

        post_data_to_api(pin_result, "pin")
        post_data_to_api(geo_result, "geo")
        post_data_to_api(user_result, "user")

def get_data_from_rds(connection, table_name, random_row):
    """
    Retrieves a record from the MYSQL database. 
    
    Args:
        Connection : The connection to the MYSQL database.
        String : The name of the table to be read.
        Integer : The randomw row number.  
    
        String : The name of the table to be read.
        Integer : The randomw row number.  
    
    Returns:
        Dictionary : Contains the data to be sent to the API.
    """
    select_string = sqlalchemy.text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
    selected_row = connection.execute(select_string)
    
    for row in selected_row:
        mapped_result = dict(row._mapping)
    
    print(mapped_result)

    return mapped_result


def post_data_to_api(dict_result, topic_suffix):
    """
    Takes a dictionary of data and posts it to the API Gateway. 
            
    Args:
        Dictionary : Contains the data to be sent to the API.
        String : The suffix of the Kafka topic where the data is to be routed.  
    """
    invoke_url = "https://mlbaqhr3m2.execute-api.us-east-1.amazonaws.com/test/topics/129a67850695." + topic_suffix
    # JSON messages need to follow this structure
    payload = json.dumps({
        "records": [
            {
            # Data is sent as pairs of column_name:value, with different columns separated by commas
            "value": dict_result
            }
        ]
    })

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    
    print(response)
    print(response.json())


if __name__ == "__main__":
    """
    Continuously run the emulation by downloading data from RDS and posting to the API.
    """
    run_infinite_post_data_loop()
    print('Working')

# %%
