import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

POPULATION = '_POPULATION'


# connect DB
def connect_DB():
    # DB sqlite
    connectionDB = f'sqlite:///{DB_SQLITE}'
    engineDB = create_engine(connectionDB)
    return engineDB

# get data DB
def get_pop(engineDB, year, id_city=None, id_province=None, id_region=None, gr_province='no', gr_region='no', gr_gender='no', gr_age='no', normalized='no'):
    name_table = year + POPULATION

    # query 
    if normalized == 'no':
        if gr_province == 'no' and gr_region == 'no':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    po.Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
        if gr_province == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
        if gr_region == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT p.Id_region , r.Region , 
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT p.Id_region , r.Region , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
    else:
        if gr_province == 'no' and gr_region == 'no':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po.Id_gender ,
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    po.Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po.Id_gender ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
        if gr_province == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT c.Id_province , 
                    p.Id_region , 
                    po.Id_gender ,
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT c.Id_province , 
                    p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT c.Id_province , 
                    p.Id_region ,
                    po.Id_gender ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT c.Id_province ,
                    p.Id_region , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
        if gr_region == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = f'''
                SELECT p.Id_region , 
                    po.Id_gender , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = f'''
                SELECT p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = f'''
                SELECT p.Id_region , 
                    po.Id_gender , 
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = f'''
                SELECT p.Id_region ,
                    po."Day",
                    po."Month",
                    po."Year",
                    SUM(po.Total) Total
                '''

    # from
    query = query + f'''
        FROM "{name_table}" po
            INNER JOIN CITY c ON c.Id_city = po.Id_city  
            INNER JOIN PROVINCE p ON p.Id_province = c.Id_province 
            INNER JOIN REGION r ON r.Id_region = p.Id_region
            INNER JOIN GENDER g ON g.Id_gender = po.Id_gender
            INNER JOIN AGE a ON a.Id_age = po.Id_age
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
    
    # group by 
    if normalized == 'no':
        if gr_province == 'no' and gr_region == 'no':
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
        if gr_province == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_gender , g.Gender ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region , 
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
        if gr_region == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = query + f'''
                GROUP BY p.Id_region , r.Region , 
                    po.Id_gender , g.Gender ,
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY p.Id_region , r.Region , 
                    po.Id_age , a.Age ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , r.Region , 
                    po.Id_gender , g.Gender ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , r.Region ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
    else:
        if gr_province == 'no' and gr_region == 'no':
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po.Id_gender ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
        if gr_province == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = query + f'''
                GROUP BY c.Id_province , 
                    p.Id_region , 
                    po.Id_gender ,
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY c.Id_province , 
                    p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , 
                    p.Id_region ,
                    po.Id_gender ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY c.Id_province ,
                    p.Id_region , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
        if gr_region == 'yes':
            if gr_gender == 'no' and gr_age == 'no':
                query = query + f'''
                GROUP BY p.Id_region , 
                    po.Id_gender , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'no':
                query = query + f'''
                GROUP BY p.Id_region , 
                    po.Id_age , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'no' and gr_age == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , 
                    po.Id_gender , 
                    po."Day",
                    po."Month",
                    po."Year"
                '''
            if gr_gender == 'yes' and gr_age == 'yes':
                query = query + f'''
                GROUP BY p.Id_region ,
                    po."Day",
                    po."Month",
                    po."Year"
                '''
    
    #print(query)

    json_data = pd.read_sql_query(query, engineDB).to_json()
    ###json_format = json.dumps(json.loads(json_data), indent=2)

    ###print(json_data)
    
    return json_data

# get city
def get_population(year, id_city=None, id_province=None, id_region=None, gr_province='no', gr_region='no', gr_gender='no', gr_age='no', normalized='no'):
    # connect
    engineDB = connect_DB()
    # select
    result = get_pop(engineDB, year, id_city, id_province, id_region, gr_province, gr_region, gr_gender, gr_age, normalized)
    return result
