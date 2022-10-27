import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

INCOME_TABLE = 'INCOME_INE'
POPULATION_TABLE = 'POPULATION_INE'


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
    #name_table_pop = year + POPULATION_TABLE
    
    # query 
    query = f'''
    WITH year_pop AS
    (
    SELECT pop.Id_city, SUM(pop.Total) Total_pop
    FROM "{POPULATION_TABLE}" pop
    WHERE pop."Year" = {year}
    GROUP BY pop.Id_city
    )
    '''

    if normalized == 'no':
        query = query + f'''
        SELECT i.Id_city , c.City ,
            c.Id_province , p.Province ,
            p.Id_region , r.Region , 
            i.Id_indicator, ii.Name_indicator ,
            i."Year",
            i.Total,
            po.Total_pop 
        '''
    else:
        query = query + f'''
        SELECT i.Id_city ,
            c.Id_province ,
            p.Id_region , 
            i.Id_indicator,
            i."Year",
            i.Total,
            po.Total_pop 
        '''
        
    # from
    query = query + f'''
        FROM "{INCOME_TABLE}" i
            INNER JOIN CITY c ON c.Id_city = i.Id_city  
            INNER JOIN PROVINCE p ON p.Id_province = c.Id_province 
            INNER JOIN REGION r ON r.Id_region = p.Id_region
            INNER JOIN year_pop po ON po.Id_city = i.Id_city 
            INNER JOIN INDICATOR_INCOME ii ON ii.Id_indicator = i.Id_indicator 
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
