import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

CITY = 'CITY'
PROVINCE = 'PROVINCE'
REGION = 'REGION'
AGE = 'AGE'
GENDER = 'GENDER'
INDICATOR_INCOME = 'INDICATOR_INCOME'


# connect DB
def connect_DB():
    # DB sqlite
    connectionDB = f'sqlite:///{DB_SQLITE}'
    engineDB = create_engine(connectionDB)
    return engineDB

# get data DB
def get_dimension(engineDB, name):
    # query 
    query = f'''
    SELECT * 
    FROM {name} 
    '''
    
    #print(query)

    json_data = pd.read_sql_query(query, engineDB).to_json()
    ###json_format = json.dumps(json.loads(json_data), indent=2)

    ###print(json_data)
    
    return json_data

# get cities
def get_cities():
    # connect
    engineDB = connect_DB()
    # select
    result = get_dimension(engineDB, CITY)
    return result

# get provinces
def get_provinces():
    # connect
    engineDB = connect_DB()
    # select
    result = get_dimension(engineDB, PROVINCE)
    return result

# get regions
def get_regions():
    # connect
    engineDB = connect_DB()
    # select
    result = get_dimension(engineDB, REGION)
    return result

# get indicators incomes
def get_indicators_incomes():
    # connect
    engineDB = connect_DB()
    # select
    result = get_dimension(engineDB, INDICATOR_INCOME)
    return result
