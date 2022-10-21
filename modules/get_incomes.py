import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

INCOME = '_INCOME'


# connect DB
def connect_DB():
    # DB sqlite
    connectionDB = f'sqlite:///{DB_SQLITE}'
    engineDB = create_engine(connectionDB)
    return engineDB

# get data DB
def get_income(engineDB, year, id_city=None, id_province=None, id_region=None, id_indicator=None, normalized='no'):
    name_table = year + INCOME
    # query 
    if normalized == 'no':
        query = f'''
        SELECT i.Id_city , c.City ,
            c.Id_province , p.Province ,
            p.Id_region , r.Region , r.Country, 
            i.Id_indicator , ii.Name_indicator ,
            i."Year",
            i.Total 
        '''
    else:
        query = f'''
        SELECT i.Id_city ,
            c.Id_province ,
            p.Id_region , 
            i.Id_indicator, 
            i."Year",
            i.Total 
        '''
        
    # from
    query = query + f'''
        FROM "{name_table}" i
            INNER JOIN CITY c ON c.Id_city = i.Id_city  
            INNER JOIN PROVINCE p ON p.Id_province = c.Id_province 
            INNER JOIN REGION r ON r.Id_region = p.Id_region
            INNER JOIN INDICATOR_INCOME ii ON ii.Id_indicator = i.Id_indicator 
        WHERE 1 = 1
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
    
    # specific indicator
    if id_indicator != None:
        query = query + f'''
         AND ii.Id_indicator = '{id_indicator}'
        '''

    #print(query)

    json_data = pd.read_sql_query(query, engineDB).to_json()
    ###json_format = json.dumps(json.loads(json_data), indent=2)

    ###print(json_data)
    
    return json_data

# get city
def get_incomes(year, id_city=None, id_province=None, id_region=None, id_indicator=None, normalized='no'):
    # connect
    engineDB = connect_DB()
    # select
    result = get_income(engineDB, year, id_city, id_province, id_region, id_indicator, normalized)
    return result
