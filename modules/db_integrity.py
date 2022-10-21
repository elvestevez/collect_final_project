import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

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
def get_data(engineDB, query):
    # get data
    df = pd.read_sql_query(query, engineDB)
    return df

# integrity province
def integrity_province():
    # connect
    engineDB = connect_DB()
    # select
    query = '''
    SELECT DISTINCT c.Id_province 
    FROM CITY c
    WHERE c.Id_province NOT IN(SELECT p.Id_province
                               FROM PROVINCE p)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True

# integrity region
def integrity_region():
    # connect
    engineDB = connect_DB()
    # select
    query = '''
    SELECT DISTINCT p.Id_region  
    FROM PROVINCE p
    WHERE p.Id_region NOT IN(SELECT r.Id_region
                             FROM REGION r)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True

# integrity city
def integrity_city(table_name):
    # connect
    engineDB = connect_DB()
    # select
    query = f'''
    SELECT DISTINCT t.Id_city  
    FROM "{table_name}" t
    WHERE t.Id_city NOT IN(SELECT c.Id_city
                           FROM CITY c)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True

# integrity age
def integrity_age(table_name):
    # connect
    engineDB = connect_DB()
    # select
    query = f'''
    SELECT DISTINCT t.Id_age  
    FROM "{table_name}" t
    WHERE t.Id_age NOT IN(SELECT a.Id_age
                          FROM AGE a)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True

# integrity gender
def integrity_gender(table_name):
    # connect
    engineDB = connect_DB()
    # select
    query = f'''
    SELECT DISTINCT t.Id_gender  
    FROM "{table_name}" t
    WHERE t.Id_gender NOT IN(SELECT g.Id_gender
                             FROM GENDER g)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True

# integrity indicator income
def integrity_indicator_income(table_name):
    # connect
    engineDB = connect_DB()
    # select
    query = f'''
    SELECT DISTINCT t.Id_indicator
    FROM "{table_name}" t
    WHERE t.Id_indicator NOT IN(SELECT ii.Id_indicator
                                FROM INDICATOR_INCOME ii)
    '''
    result = get_data(engineDB, query)
    if len(result) > 0:
        return False
    else:
        return True
