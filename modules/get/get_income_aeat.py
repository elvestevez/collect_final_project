import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json
import configparser


DB_SQLITE = './db/db_collect.db'
INCOME_TABLE = 'INCOME_AEAT'
CONFIG_FILE = './datatype.properties'


# connect DB
def connect_DB():
    # DB sqlite
    connectionDB = f'sqlite:///{DB_SQLITE}'
    engineDB = create_engine(connectionDB)
    return engineDB

# get data DB
def get_data_year(engineDB):
    # query 
    query = f'''
    SELECT DISTINCT "Year"
    FROM {INCOME_TABLE}'
    ORDER BY "Year"'
    '''

    #print(query)

    try:
        dict_data = pd.read_sql_query(query, engineDB).to_dict(orient='records')
    except:
        dict_data = {}
    
    return dict_data

# get years for incomes
def get_years():
    # connect
    engineDB = connect_DB()
    
    # select data
    result_data = get_data_year(engineDB)
    # build metadata
    result_metadata = get_income_metadata(result_data, 'YEAR')
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result
    

# get data DB
def get_income_data(engineDB, year, id_city=None, id_province=None, id_region=None, normalized='no'):
    # query 
    if normalized == 'no':
        query = f'''
        SELECT i.Id_city , c.City ,
            c.Id_province , p.Province ,
            p.Id_region , r.Region , 
            i."Year",            
            i.Owners,
            i.Declarations,
            i.Population,
            i.National_position,
            i.Region_position,
            i.Avg_gross_income,
            i.Avg_net_income
        '''
    else:
        query = f'''
        SELECT i.Id_city ,
            c.Id_province ,
            p.Id_region , 
            i."Year",            
            i.Owners,
            i.Declarations,
            i.Population,
            i.National_position,
            i.Region_position,
            i.Avg_gross_income,
            i.Avg_net_income
        '''
        
    # from
    query = query + f'''
        FROM "{INCOME_TABLE}" i
            INNER JOIN CITY c ON c.Id_city = i.Id_city  
            INNER JOIN PROVINCE p ON p.Id_province = c.Id_province 
            INNER JOIN REGION r ON r.Id_region = p.Id_region
        WHERE 1 = 1
            AND i."Year" = {year}
        '''
    
    # specific city
    if id_city != None:
        query = query + f'''
         AND c.Id_city = '{id_city}'
        '''
    
    # specific province
    if id_province != None:
        query = query + f'''
         AND p.Id_province = '{id_province}'
        '''
    
    # specific region
    if id_region != None:
        query = query + f'''
         AND r.Id_region = '{id_region}'
        '''
    #print(query)

    try:
        dict_data = pd.read_sql_query(query, engineDB).to_dict(orient='records')
    except:
        dict_data = {}
    
    return dict_data

# get type and description of columns
def get_income_metadata(data, name):
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

# get incomes by city
def get_incomes(year, id_city=None, id_province=None, id_region=None, normalized='no'):
    # connect
    engineDB = connect_DB()
    
    # select data
    result_data = get_income_data(engineDB, year, id_city, id_province, id_region, normalized)
    # build metadata
    result_metadata = get_income_metadata(result_data, 'income_aeat')
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result
