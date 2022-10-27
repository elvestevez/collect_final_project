import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

INCOME_TABLE = 'INCOME_AEAT'


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
        json_data = pd.read_sql_query(query, engineDB).to_json()
    except:
        json_data = '{}'
    
    return json_data

# get years
def get_years():
    # connect
    engineDB = connect_DB()
    # select
    result = get_data_year(engineDB)
    return result

# get data DB
def get_income(engineDB, year, id_city=None, id_province=None, id_region=None, normalized='no'):
    #name_table = year + INCOMES_TABLE
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
        json_data = pd.read_sql_query(query, engineDB).to_json()
    except:
        json_data = '{}'
    
    return json_data

# get incomes by city
def get_incomes(year, id_city=None, id_province=None, id_region=None, normalized='no'):
    # connect
    engineDB = connect_DB()
    # select
    result = get_income(engineDB, year, id_city, id_province, id_region, normalized)
    return result
