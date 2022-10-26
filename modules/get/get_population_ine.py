import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json

DB_SQLITE = './db/db_collect.db'

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
    FROM {POPULATION_TABLE}'
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
def get_pop(engineDB, year, id_city=None, id_province=None, id_region=None, age='no', normalized='no'):
    #name_table = year + POPULATION_TABLE

    # query 
    if normalized == 'no':
        if age == 'no':
            query = f'''
            SELECT po.Id_city , c.City ,
                c.Id_province , p.Province ,
                p.Id_region , r.Region ,
                po."Date",
                po."Year",
                po.Total_F,
                po.Total_M,
                po.Total
            '''
        else:
            query = f'''
            SELECT po.Id_city , c.City ,
                c.Id_province , p.Province ,
                p.Id_region , r.Region ,
                po."Date",
                po."Year",
                po.Female_0, 
                po.Female_1, 
                po.Female_2, 
                po.Female_3, 
                po.Female_4, 
                po.Female_5, 
                po.Female_6, 
                po.Female_7, 
                po.Female_8, 
                po.Female_9, 
                po.Female_10, 
                po.Female_11, 
                po.Female_12, 
                po.Female_13, 
                po.Female_14, 
                po.Female_15, 
                po.Female_16, 
                po.Female_17, 
                po.Female_18, 
                po.Female_19, 
                po.Female_20, 
                po.Female_21, 
                po.Female_22, 
                po.Female_23, 
                po.Female_24, 
                po.Female_25, 
                po.Female_26, 
                po.Female_27, 
                po.Female_28, 
                po.Female_29, 
                po.Female_30, 
                po.Female_31, 
                po.Female_32, 
                po.Female_33, 
                po.Female_34, 
                po.Female_35, 
                po.Female_36, 
                po.Female_37, 
                po.Female_38, 
                po.Female_39, 
                po.Female_40, 
                po.Female_41, 
                po.Female_42, 
                po.Female_43, 
                po.Female_44, 
                po.Female_45, 
                po.Female_46, 
                po.Female_47, 
                po.Female_48, 
                po.Female_49, 
                po.Female_50, 
                po.Female_51, 
                po.Female_52, 
                po.Female_53, 
                po.Female_54, 
                po.Female_55, 
                po.Female_56, 
                po.Female_57, 
                po.Female_58, 
                po.Female_59, 
                po.Female_60, 
                po.Female_61, 
                po.Female_62, 
                po.Female_63, 
                po.Female_64, 
                po.Female_65, 
                po.Female_66, 
                po.Female_67, 
                po.Female_68, 
                po.Female_69, 
                po.Female_70, 
                po.Female_71, 
                po.Female_72, 
                po.Female_73, 
                po.Female_74, 
                po.Female_75, 
                po.Female_76, 
                po.Female_77, 
                po.Female_78, 
                po.Female_79, 
                po.Female_80, 
                po.Female_81, 
                po.Female_82, 
                po.Female_83, 
                po.Female_84, 
                po.Female_85, 
                po.Female_86, 
                po.Female_87, 
                po.Female_88, 
                po.Female_89, 
                po.Female_90, 
                po.Female_91, 
                po.Female_92, 
                po.Female_93, 
                po.Female_94, 
                po.Female_95, 
                po.Female_96, 
                po.Female_97, 
                po.Female_98, 
                po.Female_99, 
                po.Female_100, 
                po.Male_0, 
                po.Male_1, 
                po.Male_2, 
                po.Male_3, 
                po.Male_4, 
                po.Male_5, 
                po.Male_6, 
                po.Male_7, 
                po.Male_8, 
                po.Male_9, 
                po.Male_10, 
                po.Male_11, 
                po.Male_12, 
                po.Male_13, 
                po.Male_14, 
                po.Male_15, 
                po.Male_16, 
                po.Male_17, 
                po.Male_18, 
                po.Male_19, 
                po.Male_20, 
                po.Male_21, 
                po.Male_22, 
                po.Male_23, 
                po.Male_24, 
                po.Male_25, 
                po.Male_26, 
                po.Male_27, 
                po.Male_28, 
                po.Male_29, 
                po.Male_30, 
                po.Male_31, 
                po.Male_32, 
                po.Male_33, 
                po.Male_34, 
                po.Male_35, 
                po.Male_36, 
                po.Male_37, 
                po.Male_38, 
                po.Male_39, 
                po.Male_40, 
                po.Male_41, 
                po.Male_42, 
                po.Male_43, 
                po.Male_44, 
                po.Male_45, 
                po.Male_46, 
                po.Male_47, 
                po.Male_48, 
                po.Male_49, 
                po.Male_50, 
                po.Male_51, 
                po.Male_52, 
                po.Male_53, 
                po.Male_54, 
                po.Male_55, 
                po.Male_56, 
                po.Male_57, 
                po.Male_58, 
                po.Male_59, 
                po.Male_60, 
                po.Male_61, 
                po.Male_62, 
                po.Male_63, 
                po.Male_64, 
                po.Male_65, 
                po.Male_66, 
                po.Male_67, 
                po.Male_68, 
                po.Male_69, 
                po.Male_70, 
                po.Male_71, 
                po.Male_72, 
                po.Male_73, 
                po.Male_74, 
                po.Male_75, 
                po.Male_76, 
                po.Male_77, 
                po.Male_78, 
                po.Male_79, 
                po.Male_80, 
                po.Male_81, 
                po.Male_82, 
                po.Male_83, 
                po.Male_84, 
                po.Male_85, 
                po.Male_86, 
                po.Male_87, 
                po.Male_88, 
                po.Male_89, 
                po.Male_90, 
                po.Male_91, 
                po.Male_92, 
                po.Male_93, 
                po.Male_94, 
                po.Male_95, 
                po.Male_96, 
                po.Male_97, 
                po.Male_98, 
                po.Male_99, 
                po.Male_100, 
                po.Total_F,
                po.Total_M,
                po.Total
            '''
    else:
        if age == 'no':
            query = f'''
            SELECT po.Id_city , 
                c.Id_province , 
                p.Id_region , 
                po."Date",
                po."Year",
                po.Total_F,
                po.Total_M,
                po.Total
            '''
        else:
            query = f'''
            SELECT po.Id_city , 
                c.Id_province , 
                p.Id_region , 
                po."Date",
                po."Year",
                po.Female_0, 
                po.Female_1, 
                po.Female_2, 
                po.Female_3, 
                po.Female_4, 
                po.Female_5, 
                po.Female_6, 
                po.Female_7, 
                po.Female_8, 
                po.Female_9, 
                po.Female_10, 
                po.Female_11, 
                po.Female_12, 
                po.Female_13, 
                po.Female_14, 
                po.Female_15, 
                po.Female_16, 
                po.Female_17, 
                po.Female_18, 
                po.Female_19, 
                po.Female_20, 
                po.Female_21, 
                po.Female_22, 
                po.Female_23, 
                po.Female_24, 
                po.Female_25, 
                po.Female_26, 
                po.Female_27, 
                po.Female_28, 
                po.Female_29, 
                po.Female_30, 
                po.Female_31, 
                po.Female_32, 
                po.Female_33, 
                po.Female_34, 
                po.Female_35, 
                po.Female_36, 
                po.Female_37, 
                po.Female_38, 
                po.Female_39, 
                po.Female_40, 
                po.Female_41, 
                po.Female_42, 
                po.Female_43, 
                po.Female_44, 
                po.Female_45, 
                po.Female_46, 
                po.Female_47, 
                po.Female_48, 
                po.Female_49, 
                po.Female_50, 
                po.Female_51, 
                po.Female_52, 
                po.Female_53, 
                po.Female_54, 
                po.Female_55, 
                po.Female_56, 
                po.Female_57, 
                po.Female_58, 
                po.Female_59, 
                po.Female_60, 
                po.Female_61, 
                po.Female_62, 
                po.Female_63, 
                po.Female_64, 
                po.Female_65, 
                po.Female_66, 
                po.Female_67, 
                po.Female_68, 
                po.Female_69, 
                po.Female_70, 
                po.Female_71, 
                po.Female_72, 
                po.Female_73, 
                po.Female_74, 
                po.Female_75, 
                po.Female_76, 
                po.Female_77, 
                po.Female_78, 
                po.Female_79, 
                po.Female_80, 
                po.Female_81, 
                po.Female_82, 
                po.Female_83, 
                po.Female_84, 
                po.Female_85, 
                po.Female_86, 
                po.Female_87, 
                po.Female_88, 
                po.Female_89, 
                po.Female_90, 
                po.Female_91, 
                po.Female_92, 
                po.Female_93, 
                po.Female_94, 
                po.Female_95, 
                po.Female_96, 
                po.Female_97, 
                po.Female_98, 
                po.Female_99, 
                po.Female_100, 
                po.Male_0, 
                po.Male_1, 
                po.Male_2, 
                po.Male_3, 
                po.Male_4, 
                po.Male_5, 
                po.Male_6, 
                po.Male_7, 
                po.Male_8, 
                po.Male_9, 
                po.Male_10, 
                po.Male_11, 
                po.Male_12, 
                po.Male_13, 
                po.Male_14, 
                po.Male_15, 
                po.Male_16, 
                po.Male_17, 
                po.Male_18, 
                po.Male_19, 
                po.Male_20, 
                po.Male_21, 
                po.Male_22, 
                po.Male_23, 
                po.Male_24, 
                po.Male_25, 
                po.Male_26, 
                po.Male_27, 
                po.Male_28, 
                po.Male_29, 
                po.Male_30, 
                po.Male_31, 
                po.Male_32, 
                po.Male_33, 
                po.Male_34, 
                po.Male_35, 
                po.Male_36, 
                po.Male_37, 
                po.Male_38, 
                po.Male_39, 
                po.Male_40, 
                po.Male_41, 
                po.Male_42, 
                po.Male_43, 
                po.Male_44, 
                po.Male_45, 
                po.Male_46, 
                po.Male_47, 
                po.Male_48, 
                po.Male_49, 
                po.Male_50, 
                po.Male_51, 
                po.Male_52, 
                po.Male_53, 
                po.Male_54, 
                po.Male_55, 
                po.Male_56, 
                po.Male_57, 
                po.Male_58, 
                po.Male_59, 
                po.Male_60, 
                po.Male_61, 
                po.Male_62, 
                po.Male_63, 
                po.Male_64, 
                po.Male_65, 
                po.Male_66, 
                po.Male_67, 
                po.Male_68, 
                po.Male_69, 
                po.Male_70, 
                po.Male_71, 
                po.Male_72, 
                po.Male_73, 
                po.Male_74, 
                po.Male_75, 
                po.Male_76, 
                po.Male_77, 
                po.Male_78, 
                po.Male_79, 
                po.Male_80, 
                po.Male_81, 
                po.Male_82, 
                po.Male_83, 
                po.Male_84, 
                po.Male_85, 
                po.Male_86, 
                po.Male_87, 
                po.Male_88, 
                po.Male_89, 
                po.Male_90, 
                po.Male_91, 
                po.Male_92, 
                po.Male_93, 
                po.Male_94, 
                po.Male_95, 
                po.Male_96, 
                po.Male_97, 
                po.Male_98, 
                po.Male_99, 
                po.Male_100, 
                po.Total_F,
                po.Total_M,
                po.Total
            '''

    # from
    query = query + f'''
        FROM "{POPULATION_TABLE}" po
            INNER JOIN CITY c ON c.Id_city = po.Id_city  
            INNER JOIN PROVINCE p ON p.Id_province = c.Id_province 
            INNER JOIN REGION r ON r.Id_region = p.Id_region
        WHERE 1 = 1
            AND po."Year" = {year}
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

# get population by city, province or region
def get_population(year, id_city=None, id_province=None, id_region=None, age='no', normalized='no'):
    # connect
    engineDB = connect_DB()
    # select
    result = get_pop(engineDB, year, id_city, id_province, id_region, age, normalized)
    return result
