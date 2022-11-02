import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json
import configparser


DB_SQLITE = './db/db_collect.db'
CITY = 'CITY'
PROVINCE = 'PROVINCE'
REGION = 'REGION'
INDICATOR_INCOME = 'INDICATOR_INCOME'
CONFIG_FILE = './datatype.properties'


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

    dict_data = pd.read_sql_query(query, engineDB).to_dict(orient='records')
    
    ###print(json_data)
    
    return dict_data

# get type and description of columns
def get_dimension_metadata(data, name):
    # def type info
    info = name
    # get properties file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)
    # convert to df
    df = pd.DataFrame(data)
    # get cols of df
    cols = df.columns
    list_datatype = []
    # every col
    for c in cols:
        dict_datatype = {}
        # id
        dict_datatype['id'] = c
        if config.has_option(info, c):
            # get type
            dict_datatype['type'] = json.loads(config.get(info, c))['type']
            # get text
            dict_datatype['description'] = json.loads(config.get(info, c))['text']
            list_datatype.append(dict_datatype)
    
    dict_datatype = pd.DataFrame(list_datatype).to_dict(orient='records')
    
    return dict_datatype

# get cities
def get_cities():
    # connect
    engineDB = connect_DB()

    # select data
    result_data = get_dimension(engineDB, CITY)
    # build metadata
    result_metadata = get_dimension_metadata(result_data, CITY)
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result


# get provinces
def get_provinces():
    # connect
    engineDB = connect_DB()

    # select data
    result_data = get_dimension(engineDB, PROVINCE)
    # build metadata
    result_metadata = get_dimension_metadata(result_data, PROVINCE)
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result

# get regions
def get_regions():
    # connect
    engineDB = connect_DB()
    
    # select data
    result_data = get_dimension(engineDB, REGION)
    # build metadata
    result_metadata = get_dimension_metadata(result_data, REGION)
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result

# get indicators incomes
def get_indicators_income():
    # connect
    engineDB = connect_DB()
    
    # select data
    result_data = get_dimension(engineDB, INDICATOR_INCOME)
    # build metadata
    result_metadata = get_dimension_metadata(result_data, INDICATOR_INCOME)
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result
