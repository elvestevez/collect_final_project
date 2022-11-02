import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import json
import configparser


DB_SQLITE = './db/db_collect.db'
POPULATION_TABLE = 'POPULATION_INE'
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
    FROM {POPULATION_TABLE}'
    ORDER BY "Year"'
    '''

    #print(query)

    try:
        dict_data = pd.read_sql_query(query, engineDB).to_dict(orient='records')
    except:
        dict_data = {}
    
    return dict_data

# get years for population
def get_years():
    # connect
    engineDB = connect_DB()
    
    # select data
    result_data = get_data_year(engineDB)
    # build metadata
    result_metadata = get_pop_metadata(result_data, 'YEAR')
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result

# get data DB
def get_pop(engineDB, year, id_city=None, id_province=None, id_region=None, age='no', gr_province='no', gr_region='no', normalized='no'):
    #name_table = year + POPULATION_TABLE

    # query 
    if normalized == 'no':
        if age == 'no':
            if gr_province == 'no' and gr_region == 'no':
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
            if gr_province == 'yes':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Date",
                    po."Year",
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
            if gr_region == 'yes':
                query = f'''
                SELECT p.Id_region , r.Region ,
                    po."Date",
                    po."Year",
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
        else:
            if gr_province == 'no' and gr_region == 'no':
                query = f'''
                SELECT po.Id_city , c.City ,
                    c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Date",
                    po."Year",
                    po.Female_0,  po.Female_1,  po.Female_2,  po.Female_3,  po.Female_4,  po.Female_5,  po.Female_6,  po.Female_7,  po.Female_8,  po.Female_9, 
                    po.Female_10, po.Female_11, po.Female_12, po.Female_13, po.Female_14, po.Female_15, po.Female_16, po.Female_17, po.Female_18, po.Female_19, 
                    po.Female_20, po.Female_21, po.Female_22, po.Female_23, po.Female_24, po.Female_25, po.Female_26, po.Female_27, po.Female_28, po.Female_29, 
                    po.Female_30, po.Female_31, po.Female_32, po.Female_33, po.Female_34, po.Female_35, po.Female_36, po.Female_37, po.Female_38, po.Female_39, 
                    po.Female_40, po.Female_41, po.Female_42, po.Female_43, po.Female_44, po.Female_45, po.Female_46, po.Female_47, po.Female_48, po.Female_49, 
                    po.Female_50, po.Female_51, po.Female_52, po.Female_53, po.Female_54, po.Female_55, po.Female_56, po.Female_57, po.Female_58, po.Female_59, 
                    po.Female_60, po.Female_61, po.Female_62, po.Female_63, po.Female_64, po.Female_65, po.Female_66, po.Female_67, po.Female_68, po.Female_69, 
                    po.Female_70, po.Female_71, po.Female_72, po.Female_73, po.Female_74, po.Female_75, po.Female_76, po.Female_77, po.Female_78, po.Female_79, 
                    po.Female_80, po.Female_81, po.Female_82, po.Female_83, po.Female_84, po.Female_85, po.Female_86, po.Female_87, po.Female_88, po.Female_89, 
                    po.Female_90, po.Female_91, po.Female_92, po.Female_93, po.Female_94, po.Female_95, po.Female_96, po.Female_97, po.Female_98, po.Female_99, 
                    po.Female_100, 
                    po.Male_0,  po.Male_1,  po.Male_2,  po.Male_3,  po.Male_4,  po.Male_5,  po.Male_6,  po.Male_7,  po.Male_8,  po.Male_9, 
                    po.Male_10, po.Male_11, po.Male_12, po.Male_13, po.Male_14, po.Male_15, po.Male_16, po.Male_17, po.Male_18, po.Male_19, 
                    po.Male_20, po.Male_21, po.Male_22, po.Male_23, po.Male_24, po.Male_25, po.Male_26, po.Male_27, po.Male_28, po.Male_29, 
                    po.Male_30, po.Male_31, po.Male_32, po.Male_33, po.Male_34, po.Male_35, po.Male_36, po.Male_37, po.Male_38, po.Male_39, 
                    po.Male_40, po.Male_41, po.Male_42, po.Male_43, po.Male_44, po.Male_45, po.Male_46, po.Male_47, po.Male_48, po.Male_49, 
                    po.Male_50, po.Male_51, po.Male_52, po.Male_53, po.Male_54, po.Male_55, po.Male_56, po.Male_57, po.Male_58, po.Male_59, 
                    po.Male_60, po.Male_61, po.Male_62, po.Male_63, po.Male_64, po.Male_65, po.Male_66, po.Male_67, po.Male_68, po.Male_69, 
                    po.Male_70, po.Male_71, po.Male_72, po.Male_73, po.Male_74, po.Male_75, po.Male_76, po.Male_77, po.Male_78, po.Male_79, 
                    po.Male_80, po.Male_81, po.Male_82, po.Male_83, po.Male_84, po.Male_85, po.Male_86, po.Male_87, po.Male_88, po.Male_89, 
                    po.Male_90, po.Male_91, po.Male_92, po.Male_93, po.Male_94, po.Male_95, po.Male_96, po.Male_97, po.Male_98, po.Male_99, 
                    po.Male_100, 
                    po.Total_F,
                    po.Total_M,
                    po.Total
                '''
            if gr_province == 'yes':
                query = f'''
                SELECT c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Date",
                    po."Year",
                    SUM(po.Female_0)  Female_0 , SUM(po.Female_1)  Female_1,  SUM(po.Female_2)  Female_2,  SUM(po.Female_3)  Female_3,  
                    SUM(po.Female_4)  Female_4,  SUM(po.Female_5)  Female_5,  SUM(po.Female_6)  Female_6,  SUM(po.Female_7)  Female_7,  
                    SUM(po.Female_8)  Female_8,  SUM(po.Female_9)  Female_9,  SUM(po.Female_10) Female_10, SUM(po.Female_11) Female_11, 
                    SUM(po.Female_12) Female_12, SUM(po.Female_13) Female_13, SUM(po.Female_14) Female_14, SUM(po.Female_15) Female_15, 
                    SUM(po.Female_16) Female_16, SUM(po.Female_17) Female_17, SUM(po.Female_18) Female_18, SUM(po.Female_19) Female_19, 
                    SUM(po.Female_20) Female_20, SUM(po.Female_21) Female_21, SUM(po.Female_22) Female_22, SUM(po.Female_23) Female_23, 
                    SUM(po.Female_24) Female_24, SUM(po.Female_25) Female_25, SUM(po.Female_26) Female_26, SUM(po.Female_27) Female_27, 
                    SUM(po.Female_28) Female_28, SUM(po.Female_29) Female_29, SUM(po.Female_30) Female_30, SUM(po.Female_31) Female_31, 
                    SUM(po.Female_32) Female_32, SUM(po.Female_33) Female_33, SUM(po.Female_34) Female_34, SUM(po.Female_35) Female_35, 
                    SUM(po.Female_36) Female_36, SUM(po.Female_37) Female_37, SUM(po.Female_38) Female_38, SUM(po.Female_39) Female_39, 
                    SUM(po.Female_40) Female_40, SUM(po.Female_41) Female_41, SUM(po.Female_42) Female_42, SUM(po.Female_43) Female_43, 
                    SUM(po.Female_44) Female_44, SUM(po.Female_45) Female_45, SUM(po.Female_46) Female_46, SUM(po.Female_47) Female_47, 
                    SUM(po.Female_48) Female_48, SUM(po.Female_49) Female_49, SUM(po.Female_50) Female_50, SUM(po.Female_51) Female_51, 
                    SUM(po.Female_52) Female_52, SUM(po.Female_53) Female_53, SUM(po.Female_54) Female_54, SUM(po.Female_55) Female_55, 
                    SUM(po.Female_56) Female_56, SUM(po.Female_57) Female_57, SUM(po.Female_58) Female_58, SUM(po.Female_59) Female_59, 
                    SUM(po.Female_60) Female_60, SUM(po.Female_61) Female_61, SUM(po.Female_62) Female_62, SUM(po.Female_63) Female_63, 
                    SUM(po.Female_64) Female_64, SUM(po.Female_65) Female_65, SUM(po.Female_66) Female_66, SUM(po.Female_67) Female_67, 
                    SUM(po.Female_68) Female_68, SUM(po.Female_69) Female_69, SUM(po.Female_70) Female_70, SUM(po.Female_71) Female_71, 
                    SUM(po.Female_72) Female_72, SUM(po.Female_73) Female_73, SUM(po.Female_74) Female_74, SUM(po.Female_75) Female_75, 
                    SUM(po.Female_76) Female_76, SUM(po.Female_77) Female_77, SUM(po.Female_78) Female_78, SUM(po.Female_79) Female_79, 
                    SUM(po.Female_80) Female_80, SUM(po.Female_81) Female_81, SUM(po.Female_82) Female_82, SUM(po.Female_83) Female_83, 
                    SUM(po.Female_84) Female_84, SUM(po.Female_85) Female_85, SUM(po.Female_86) Female_86, SUM(po.Female_87) Female_87, 
                    SUM(po.Female_88) Female_88, SUM(po.Female_89) Female_89, SUM(po.Female_90) Female_90, SUM(po.Female_91) Female_91, 
                    SUM(po.Female_92) Female_92, SUM(po.Female_93) Female_93, SUM(po.Female_94) Female_94, SUM(po.Female_95) Female_95, 
                    SUM(po.Female_96) Female_96, SUM(po.Female_97) Female_97, SUM(po.Female_98) Female_98, SUM(po.Female_99) Female_99, 
                    SUM(po.Female_100) Female_100,
                    SUM(po.Male_0)  Male_0 , SUM(po.Male_1)  Male_1,  SUM(po.Male_2)  Male_2,  SUM(po.Male_3)  Male_3,  
                    SUM(po.Male_4)  Male_4,  SUM(po.Male_5)  Male_5,  SUM(po.Male_6)  Male_6,  SUM(po.Male_7)  Male_7,  
                    SUM(po.Male_8)  Male_8,  SUM(po.Male_9)  Male_9,  SUM(po.Male_10) Male_10, SUM(po.Male_11) Male_11, 
                    SUM(po.Male_12) Male_12, SUM(po.Male_13) Male_13, SUM(po.Male_14) Male_14, SUM(po.Male_15) Male_15, 
                    SUM(po.Male_16) Male_16, SUM(po.Male_17) Male_17, SUM(po.Male_18) Male_18, SUM(po.Male_19) Male_19, 
                    SUM(po.Male_20) Male_20, SUM(po.Male_21) Male_21, SUM(po.Male_22) Male_22, SUM(po.Male_23) Male_23, 
                    SUM(po.Male_24) Male_24, SUM(po.Male_25) Male_25, SUM(po.Male_26) Male_26, SUM(po.Male_27) Male_27, 
                    SUM(po.Male_28) Male_28, SUM(po.Male_29) Male_29, SUM(po.Male_30) Male_30, SUM(po.Male_31) Male_31, 
                    SUM(po.Male_32) Male_32, SUM(po.Male_33) Male_33, SUM(po.Male_34) Male_34, SUM(po.Male_35) Male_35, 
                    SUM(po.Male_36) Male_36, SUM(po.Male_37) Male_37, SUM(po.Male_38) Male_38, SUM(po.Male_39) Male_39, 
                    SUM(po.Male_40) Male_40, SUM(po.Male_41) Male_41, SUM(po.Male_42) Male_42, SUM(po.Male_43) Male_43, 
                    SUM(po.Male_44) Male_44, SUM(po.Male_45) Male_45, SUM(po.Male_46) Male_46, SUM(po.Male_47) Male_47, 
                    SUM(po.Male_48) Male_48, SUM(po.Male_49) Male_49, SUM(po.Male_50) Male_50, SUM(po.Male_51) Male_51, 
                    SUM(po.Male_52) Male_52, SUM(po.Male_53) Male_53, SUM(po.Male_54) Male_54, SUM(po.Male_55) Male_55, 
                    SUM(po.Male_56) Male_56, SUM(po.Male_57) Male_57, SUM(po.Male_58) Male_58, SUM(po.Male_59) Male_59, 
                    SUM(po.Male_60) Male_60, SUM(po.Male_61) Male_61, SUM(po.Male_62) Male_62, SUM(po.Male_63) Male_63, 
                    SUM(po.Male_64) Male_64, SUM(po.Male_65) Male_65, SUM(po.Male_66) Male_66, SUM(po.Male_67) Male_67, 
                    SUM(po.Male_68) Male_68, SUM(po.Male_69) Male_69, SUM(po.Male_70) Male_70, SUM(po.Male_71) Male_71, 
                    SUM(po.Male_72) Male_72, SUM(po.Male_73) Male_73, SUM(po.Male_74) Male_74, SUM(po.Male_75) Male_75, 
                    SUM(po.Male_76) Male_76, SUM(po.Male_77) Male_77, SUM(po.Male_78) Male_78, SUM(po.Male_79) Male_79, 
                    SUM(po.Male_80) Male_80, SUM(po.Male_81) Male_81, SUM(po.Male_82) Male_82, SUM(po.Male_83) Male_83, 
                    SUM(po.Male_84) Male_84, SUM(po.Male_85) Male_85, SUM(po.Male_86) Male_86, SUM(po.Male_87) Male_87, 
                    SUM(po.Male_88) Male_88, SUM(po.Male_89) Male_89, SUM(po.Male_90) Male_90, SUM(po.Male_91) Male_91, 
                    SUM(po.Male_92) Male_92, SUM(po.Male_93) Male_93, SUM(po.Male_94) Male_94, SUM(po.Male_95) Male_95, 
                    SUM(po.Male_96) Male_96, SUM(po.Male_97) Male_97, SUM(po.Male_98) Male_98, SUM(po.Male_99) Male_99, 
                    SUM(po.Male_100) Male_100, 
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
            if gr_region == 'yes':
                query = f'''
                SELECT p.Id_region , r.Region ,
                    po."Date",
                    po."Year",
                    SUM(po.Female_0)  Female_0 , SUM(po.Female_1)  Female_1,  SUM(po.Female_2)  Female_2,  SUM(po.Female_3)  Female_3,  
                    SUM(po.Female_4)  Female_4,  SUM(po.Female_5)  Female_5,  SUM(po.Female_6)  Female_6,  SUM(po.Female_7)  Female_7,  
                    SUM(po.Female_8)  Female_8,  SUM(po.Female_9)  Female_9,  SUM(po.Female_10) Female_10, SUM(po.Female_11) Female_11, 
                    SUM(po.Female_12) Female_12, SUM(po.Female_13) Female_13, SUM(po.Female_14) Female_14, SUM(po.Female_15) Female_15, 
                    SUM(po.Female_16) Female_16, SUM(po.Female_17) Female_17, SUM(po.Female_18) Female_18, SUM(po.Female_19) Female_19, 
                    SUM(po.Female_20) Female_20, SUM(po.Female_21) Female_21, SUM(po.Female_22) Female_22, SUM(po.Female_23) Female_23, 
                    SUM(po.Female_24) Female_24, SUM(po.Female_25) Female_25, SUM(po.Female_26) Female_26, SUM(po.Female_27) Female_27, 
                    SUM(po.Female_28) Female_28, SUM(po.Female_29) Female_29, SUM(po.Female_30) Female_30, SUM(po.Female_31) Female_31, 
                    SUM(po.Female_32) Female_32, SUM(po.Female_33) Female_33, SUM(po.Female_34) Female_34, SUM(po.Female_35) Female_35, 
                    SUM(po.Female_36) Female_36, SUM(po.Female_37) Female_37, SUM(po.Female_38) Female_38, SUM(po.Female_39) Female_39, 
                    SUM(po.Female_40) Female_40, SUM(po.Female_41) Female_41, SUM(po.Female_42) Female_42, SUM(po.Female_43) Female_43, 
                    SUM(po.Female_44) Female_44, SUM(po.Female_45) Female_45, SUM(po.Female_46) Female_46, SUM(po.Female_47) Female_47, 
                    SUM(po.Female_48) Female_48, SUM(po.Female_49) Female_49, SUM(po.Female_50) Female_50, SUM(po.Female_51) Female_51, 
                    SUM(po.Female_52) Female_52, SUM(po.Female_53) Female_53, SUM(po.Female_54) Female_54, SUM(po.Female_55) Female_55, 
                    SUM(po.Female_56) Female_56, SUM(po.Female_57) Female_57, SUM(po.Female_58) Female_58, SUM(po.Female_59) Female_59, 
                    SUM(po.Female_60) Female_60, SUM(po.Female_61) Female_61, SUM(po.Female_62) Female_62, SUM(po.Female_63) Female_63, 
                    SUM(po.Female_64) Female_64, SUM(po.Female_65) Female_65, SUM(po.Female_66) Female_66, SUM(po.Female_67) Female_67, 
                    SUM(po.Female_68) Female_68, SUM(po.Female_69) Female_69, SUM(po.Female_70) Female_70, SUM(po.Female_71) Female_71, 
                    SUM(po.Female_72) Female_72, SUM(po.Female_73) Female_73, SUM(po.Female_74) Female_74, SUM(po.Female_75) Female_75, 
                    SUM(po.Female_76) Female_76, SUM(po.Female_77) Female_77, SUM(po.Female_78) Female_78, SUM(po.Female_79) Female_79, 
                    SUM(po.Female_80) Female_80, SUM(po.Female_81) Female_81, SUM(po.Female_82) Female_82, SUM(po.Female_83) Female_83, 
                    SUM(po.Female_84) Female_84, SUM(po.Female_85) Female_85, SUM(po.Female_86) Female_86, SUM(po.Female_87) Female_87, 
                    SUM(po.Female_88) Female_88, SUM(po.Female_89) Female_89, SUM(po.Female_90) Female_90, SUM(po.Female_91) Female_91, 
                    SUM(po.Female_92) Female_92, SUM(po.Female_93) Female_93, SUM(po.Female_94) Female_94, SUM(po.Female_95) Female_95, 
                    SUM(po.Female_96) Female_96, SUM(po.Female_97) Female_97, SUM(po.Female_98) Female_98, SUM(po.Female_99) Female_99, 
                    SUM(po.Female_100) Female_100,
                    SUM(po.Male_0)  Male_0 , SUM(po.Male_1)  Male_1,  SUM(po.Male_2)  Male_2,  SUM(po.Male_3)  Male_3,  
                    SUM(po.Male_4)  Male_4,  SUM(po.Male_5)  Male_5,  SUM(po.Male_6)  Male_6,  SUM(po.Male_7)  Male_7,  
                    SUM(po.Male_8)  Male_8,  SUM(po.Male_9)  Male_9,  SUM(po.Male_10) Male_10, SUM(po.Male_11) Male_11, 
                    SUM(po.Male_12) Male_12, SUM(po.Male_13) Male_13, SUM(po.Male_14) Male_14, SUM(po.Male_15) Male_15, 
                    SUM(po.Male_16) Male_16, SUM(po.Male_17) Male_17, SUM(po.Male_18) Male_18, SUM(po.Male_19) Male_19, 
                    SUM(po.Male_20) Male_20, SUM(po.Male_21) Male_21, SUM(po.Male_22) Male_22, SUM(po.Male_23) Male_23, 
                    SUM(po.Male_24) Male_24, SUM(po.Male_25) Male_25, SUM(po.Male_26) Male_26, SUM(po.Male_27) Male_27, 
                    SUM(po.Male_28) Male_28, SUM(po.Male_29) Male_29, SUM(po.Male_30) Male_30, SUM(po.Male_31) Male_31, 
                    SUM(po.Male_32) Male_32, SUM(po.Male_33) Male_33, SUM(po.Male_34) Male_34, SUM(po.Male_35) Male_35, 
                    SUM(po.Male_36) Male_36, SUM(po.Male_37) Male_37, SUM(po.Male_38) Male_38, SUM(po.Male_39) Male_39, 
                    SUM(po.Male_40) Male_40, SUM(po.Male_41) Male_41, SUM(po.Male_42) Male_42, SUM(po.Male_43) Male_43, 
                    SUM(po.Male_44) Male_44, SUM(po.Male_45) Male_45, SUM(po.Male_46) Male_46, SUM(po.Male_47) Male_47, 
                    SUM(po.Male_48) Male_48, SUM(po.Male_49) Male_49, SUM(po.Male_50) Male_50, SUM(po.Male_51) Male_51, 
                    SUM(po.Male_52) Male_52, SUM(po.Male_53) Male_53, SUM(po.Male_54) Male_54, SUM(po.Male_55) Male_55, 
                    SUM(po.Male_56) Male_56, SUM(po.Male_57) Male_57, SUM(po.Male_58) Male_58, SUM(po.Male_59) Male_59, 
                    SUM(po.Male_60) Male_60, SUM(po.Male_61) Male_61, SUM(po.Male_62) Male_62, SUM(po.Male_63) Male_63, 
                    SUM(po.Male_64) Male_64, SUM(po.Male_65) Male_65, SUM(po.Male_66) Male_66, SUM(po.Male_67) Male_67, 
                    SUM(po.Male_68) Male_68, SUM(po.Male_69) Male_69, SUM(po.Male_70) Male_70, SUM(po.Male_71) Male_71, 
                    SUM(po.Male_72) Male_72, SUM(po.Male_73) Male_73, SUM(po.Male_74) Male_74, SUM(po.Male_75) Male_75, 
                    SUM(po.Male_76) Male_76, SUM(po.Male_77) Male_77, SUM(po.Male_78) Male_78, SUM(po.Male_79) Male_79, 
                    SUM(po.Male_80) Male_80, SUM(po.Male_81) Male_81, SUM(po.Male_82) Male_82, SUM(po.Male_83) Male_83, 
                    SUM(po.Male_84) Male_84, SUM(po.Male_85) Male_85, SUM(po.Male_86) Male_86, SUM(po.Male_87) Male_87, 
                    SUM(po.Male_88) Male_88, SUM(po.Male_89) Male_89, SUM(po.Male_90) Male_90, SUM(po.Male_91) Male_91, 
                    SUM(po.Male_92) Male_92, SUM(po.Male_93) Male_93, SUM(po.Male_94) Male_94, SUM(po.Male_95) Male_95, 
                    SUM(po.Male_96) Male_96, SUM(po.Male_97) Male_97, SUM(po.Male_98) Male_98, SUM(po.Male_99) Male_99, 
                    SUM(po.Male_100) Male_100,
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
    else:
        if age == 'no':
            if gr_province == 'no' and gr_region == 'no':
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
            if gr_province == 'yes':
                query = f'''
                SELECT c.Id_province , 
                    p.Id_region , 
                    po."Date",
                    po."Year",
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
            if gr_region == 'yes':
                query = f'''
                SELECT p.Id_region , 
                    po."Date",
                    po."Year",
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
        else:
            if gr_province == 'no' and gr_region == 'no':
                query = f'''
                SELECT po.Id_city , 
                    c.Id_province , 
                    p.Id_region , 
                    po."Date",
                    po."Year",
                    po.Female_0,  po.Female_1,  po.Female_2,  po.Female_3,  po.Female_4,  po.Female_5,  po.Female_6,  po.Female_7,  po.Female_8,  po.Female_9, 
                    po.Female_10, po.Female_11, po.Female_12, po.Female_13, po.Female_14, po.Female_15, po.Female_16, po.Female_17, po.Female_18, po.Female_19, 
                    po.Female_20, po.Female_21, po.Female_22, po.Female_23, po.Female_24, po.Female_25, po.Female_26, po.Female_27, po.Female_28, po.Female_29, 
                    po.Female_30, po.Female_31, po.Female_32, po.Female_33, po.Female_34, po.Female_35, po.Female_36, po.Female_37, po.Female_38, po.Female_39, 
                    po.Female_40, po.Female_41, po.Female_42, po.Female_43, po.Female_44, po.Female_45, po.Female_46, po.Female_47, po.Female_48, po.Female_49, 
                    po.Female_50, po.Female_51, po.Female_52, po.Female_53, po.Female_54, po.Female_55, po.Female_56, po.Female_57, po.Female_58, po.Female_59, 
                    po.Female_60, po.Female_61, po.Female_62, po.Female_63, po.Female_64, po.Female_65, po.Female_66, po.Female_67, po.Female_68, po.Female_69, 
                    po.Female_70, po.Female_71, po.Female_72, po.Female_73, po.Female_74, po.Female_75, po.Female_76, po.Female_77, po.Female_78, po.Female_79, 
                    po.Female_80, po.Female_81, po.Female_82, po.Female_83, po.Female_84, po.Female_85, po.Female_86, po.Female_87, po.Female_88, po.Female_89, 
                    po.Female_90, po.Female_91, po.Female_92, po.Female_93, po.Female_94, po.Female_95, po.Female_96, po.Female_97, po.Female_98, po.Female_99, 
                    po.Female_100, 
                    po.Male_0,  po.Male_1,  po.Male_2,  po.Male_3,  po.Male_4,  po.Male_5,  po.Male_6,  po.Male_7,  po.Male_8,  po.Male_9, 
                    po.Male_10, po.Male_11, po.Male_12, po.Male_13, po.Male_14, po.Male_15, po.Male_16, po.Male_17, po.Male_18, po.Male_19, 
                    po.Male_20, po.Male_21, po.Male_22, po.Male_23, po.Male_24, po.Male_25, po.Male_26, po.Male_27, po.Male_28, po.Male_29, 
                    po.Male_30, po.Male_31, po.Male_32, po.Male_33, po.Male_34, po.Male_35, po.Male_36, po.Male_37, po.Male_38, po.Male_39, 
                    po.Male_40, po.Male_41, po.Male_42, po.Male_43, po.Male_44, po.Male_45, po.Male_46, po.Male_47, po.Male_48, po.Male_49, 
                    po.Male_50, po.Male_51, po.Male_52, po.Male_53, po.Male_54, po.Male_55, po.Male_56, po.Male_57, po.Male_58, po.Male_59, 
                    po.Male_60, po.Male_61, po.Male_62, po.Male_63, po.Male_64, po.Male_65, po.Male_66, po.Male_67, po.Male_68, po.Male_69, 
                    po.Male_70, po.Male_71, po.Male_72, po.Male_73, po.Male_74, po.Male_75, po.Male_76, po.Male_77, po.Male_78, po.Male_79, 
                    po.Male_80, po.Male_81, po.Male_82, po.Male_83, po.Male_84, po.Male_85, po.Male_86, po.Male_87, po.Male_88, po.Male_89, 
                    po.Male_90, po.Male_91, po.Male_92, po.Male_93, po.Male_94, po.Male_95, po.Male_96, po.Male_97, po.Male_98, po.Male_99, 
                    po.Male_100, 
                    po.Total_F,
                    po.Total_M,
                    po.Total
                '''
            if gr_province == 'yes':
                query = f'''
                SELECT c.Id_province , 
                    p.Id_region , 
                    po."Date",
                    po."Year",
                    SUM(po.Female_0)  Female_0 , SUM(po.Female_1)  Female_1,  SUM(po.Female_2)  Female_2,  SUM(po.Female_3)  Female_3,  
                    SUM(po.Female_4)  Female_4,  SUM(po.Female_5)  Female_5,  SUM(po.Female_6)  Female_6,  SUM(po.Female_7)  Female_7,  
                    SUM(po.Female_8)  Female_8,  SUM(po.Female_9)  Female_9,  SUM(po.Female_10) Female_10, SUM(po.Female_11) Female_11, 
                    SUM(po.Female_12) Female_12, SUM(po.Female_13) Female_13, SUM(po.Female_14) Female_14, SUM(po.Female_15) Female_15, 
                    SUM(po.Female_16) Female_16, SUM(po.Female_17) Female_17, SUM(po.Female_18) Female_18, SUM(po.Female_19) Female_19, 
                    SUM(po.Female_20) Female_20, SUM(po.Female_21) Female_21, SUM(po.Female_22) Female_22, SUM(po.Female_23) Female_23, 
                    SUM(po.Female_24) Female_24, SUM(po.Female_25) Female_25, SUM(po.Female_26) Female_26, SUM(po.Female_27) Female_27, 
                    SUM(po.Female_28) Female_28, SUM(po.Female_29) Female_29, SUM(po.Female_30) Female_30, SUM(po.Female_31) Female_31, 
                    SUM(po.Female_32) Female_32, SUM(po.Female_33) Female_33, SUM(po.Female_34) Female_34, SUM(po.Female_35) Female_35, 
                    SUM(po.Female_36) Female_36, SUM(po.Female_37) Female_37, SUM(po.Female_38) Female_38, SUM(po.Female_39) Female_39, 
                    SUM(po.Female_40) Female_40, SUM(po.Female_41) Female_41, SUM(po.Female_42) Female_42, SUM(po.Female_43) Female_43, 
                    SUM(po.Female_44) Female_44, SUM(po.Female_45) Female_45, SUM(po.Female_46) Female_46, SUM(po.Female_47) Female_47, 
                    SUM(po.Female_48) Female_48, SUM(po.Female_49) Female_49, SUM(po.Female_50) Female_50, SUM(po.Female_51) Female_51, 
                    SUM(po.Female_52) Female_52, SUM(po.Female_53) Female_53, SUM(po.Female_54) Female_54, SUM(po.Female_55) Female_55, 
                    SUM(po.Female_56) Female_56, SUM(po.Female_57) Female_57, SUM(po.Female_58) Female_58, SUM(po.Female_59) Female_59, 
                    SUM(po.Female_60) Female_60, SUM(po.Female_61) Female_61, SUM(po.Female_62) Female_62, SUM(po.Female_63) Female_63, 
                    SUM(po.Female_64) Female_64, SUM(po.Female_65) Female_65, SUM(po.Female_66) Female_66, SUM(po.Female_67) Female_67, 
                    SUM(po.Female_68) Female_68, SUM(po.Female_69) Female_69, SUM(po.Female_70) Female_70, SUM(po.Female_71) Female_71, 
                    SUM(po.Female_72) Female_72, SUM(po.Female_73) Female_73, SUM(po.Female_74) Female_74, SUM(po.Female_75) Female_75, 
                    SUM(po.Female_76) Female_76, SUM(po.Female_77) Female_77, SUM(po.Female_78) Female_78, SUM(po.Female_79) Female_79, 
                    SUM(po.Female_80) Female_80, SUM(po.Female_81) Female_81, SUM(po.Female_82) Female_82, SUM(po.Female_83) Female_83, 
                    SUM(po.Female_84) Female_84, SUM(po.Female_85) Female_85, SUM(po.Female_86) Female_86, SUM(po.Female_87) Female_87, 
                    SUM(po.Female_88) Female_88, SUM(po.Female_89) Female_89, SUM(po.Female_90) Female_90, SUM(po.Female_91) Female_91, 
                    SUM(po.Female_92) Female_92, SUM(po.Female_93) Female_93, SUM(po.Female_94) Female_94, SUM(po.Female_95) Female_95, 
                    SUM(po.Female_96) Female_96, SUM(po.Female_97) Female_97, SUM(po.Female_98) Female_98, SUM(po.Female_99) Female_99, 
                    SUM(po.Female_100) Female_100,
                    SUM(po.Male_0)  Male_0 , SUM(po.Male_1)  Male_1,  SUM(po.Male_2)  Male_2,  SUM(po.Male_3)  Male_3,  
                    SUM(po.Male_4)  Male_4,  SUM(po.Male_5)  Male_5,  SUM(po.Male_6)  Male_6,  SUM(po.Male_7)  Male_7,  
                    SUM(po.Male_8)  Male_8,  SUM(po.Male_9)  Male_9,  SUM(po.Male_10) Male_10, SUM(po.Male_11) Male_11, 
                    SUM(po.Male_12) Male_12, SUM(po.Male_13) Male_13, SUM(po.Male_14) Male_14, SUM(po.Male_15) Male_15, 
                    SUM(po.Male_16) Male_16, SUM(po.Male_17) Male_17, SUM(po.Male_18) Male_18, SUM(po.Male_19) Male_19, 
                    SUM(po.Male_20) Male_20, SUM(po.Male_21) Male_21, SUM(po.Male_22) Male_22, SUM(po.Male_23) Male_23, 
                    SUM(po.Male_24) Male_24, SUM(po.Male_25) Male_25, SUM(po.Male_26) Male_26, SUM(po.Male_27) Male_27, 
                    SUM(po.Male_28) Male_28, SUM(po.Male_29) Male_29, SUM(po.Male_30) Male_30, SUM(po.Male_31) Male_31, 
                    SUM(po.Male_32) Male_32, SUM(po.Male_33) Male_33, SUM(po.Male_34) Male_34, SUM(po.Male_35) Male_35, 
                    SUM(po.Male_36) Male_36, SUM(po.Male_37) Male_37, SUM(po.Male_38) Male_38, SUM(po.Male_39) Male_39, 
                    SUM(po.Male_40) Male_40, SUM(po.Male_41) Male_41, SUM(po.Male_42) Male_42, SUM(po.Male_43) Male_43, 
                    SUM(po.Male_44) Male_44, SUM(po.Male_45) Male_45, SUM(po.Male_46) Male_46, SUM(po.Male_47) Male_47, 
                    SUM(po.Male_48) Male_48, SUM(po.Male_49) Male_49, SUM(po.Male_50) Male_50, SUM(po.Male_51) Male_51, 
                    SUM(po.Male_52) Male_52, SUM(po.Male_53) Male_53, SUM(po.Male_54) Male_54, SUM(po.Male_55) Male_55, 
                    SUM(po.Male_56) Male_56, SUM(po.Male_57) Male_57, SUM(po.Male_58) Male_58, SUM(po.Male_59) Male_59, 
                    SUM(po.Male_60) Male_60, SUM(po.Male_61) Male_61, SUM(po.Male_62) Male_62, SUM(po.Male_63) Male_63, 
                    SUM(po.Male_64) Male_64, SUM(po.Male_65) Male_65, SUM(po.Male_66) Male_66, SUM(po.Male_67) Male_67, 
                    SUM(po.Male_68) Male_68, SUM(po.Male_69) Male_69, SUM(po.Male_70) Male_70, SUM(po.Male_71) Male_71, 
                    SUM(po.Male_72) Male_72, SUM(po.Male_73) Male_73, SUM(po.Male_74) Male_74, SUM(po.Male_75) Male_75, 
                    SUM(po.Male_76) Male_76, SUM(po.Male_77) Male_77, SUM(po.Male_78) Male_78, SUM(po.Male_79) Male_79, 
                    SUM(po.Male_80) Male_80, SUM(po.Male_81) Male_81, SUM(po.Male_82) Male_82, SUM(po.Male_83) Male_83, 
                    SUM(po.Male_84) Male_84, SUM(po.Male_85) Male_85, SUM(po.Male_86) Male_86, SUM(po.Male_87) Male_87, 
                    SUM(po.Male_88) Male_88, SUM(po.Male_89) Male_89, SUM(po.Male_90) Male_90, SUM(po.Male_91) Male_91, 
                    SUM(po.Male_92) Male_92, SUM(po.Male_93) Male_93, SUM(po.Male_94) Male_94, SUM(po.Male_95) Male_95, 
                    SUM(po.Male_96) Male_96, SUM(po.Male_97) Male_97, SUM(po.Male_98) Male_98, SUM(po.Male_99) Male_99, 
                    SUM(po.Male_100) Male_100,
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
                '''
            if gr_region == 'yes':
                query = f'''
                SELECT p.Id_region , 
                    po."Date",
                    po."Year",
                    SUM(po.Female_0)  Female_0 , SUM(po.Female_1)  Female_1,  SUM(po.Female_2)  Female_2,  SUM(po.Female_3)  Female_3,  
                    SUM(po.Female_4)  Female_4,  SUM(po.Female_5)  Female_5,  SUM(po.Female_6)  Female_6,  SUM(po.Female_7)  Female_7,  
                    SUM(po.Female_8)  Female_8,  SUM(po.Female_9)  Female_9,  SUM(po.Female_10) Female_10, SUM(po.Female_11) Female_11, 
                    SUM(po.Female_12) Female_12, SUM(po.Female_13) Female_13, SUM(po.Female_14) Female_14, SUM(po.Female_15) Female_15, 
                    SUM(po.Female_16) Female_16, SUM(po.Female_17) Female_17, SUM(po.Female_18) Female_18, SUM(po.Female_19) Female_19, 
                    SUM(po.Female_20) Female_20, SUM(po.Female_21) Female_21, SUM(po.Female_22) Female_22, SUM(po.Female_23) Female_23, 
                    SUM(po.Female_24) Female_24, SUM(po.Female_25) Female_25, SUM(po.Female_26) Female_26, SUM(po.Female_27) Female_27, 
                    SUM(po.Female_28) Female_28, SUM(po.Female_29) Female_29, SUM(po.Female_30) Female_30, SUM(po.Female_31) Female_31, 
                    SUM(po.Female_32) Female_32, SUM(po.Female_33) Female_33, SUM(po.Female_34) Female_34, SUM(po.Female_35) Female_35, 
                    SUM(po.Female_36) Female_36, SUM(po.Female_37) Female_37, SUM(po.Female_38) Female_38, SUM(po.Female_39) Female_39, 
                    SUM(po.Female_40) Female_40, SUM(po.Female_41) Female_41, SUM(po.Female_42) Female_42, SUM(po.Female_43) Female_43, 
                    SUM(po.Female_44) Female_44, SUM(po.Female_45) Female_45, SUM(po.Female_46) Female_46, SUM(po.Female_47) Female_47, 
                    SUM(po.Female_48) Female_48, SUM(po.Female_49) Female_49, SUM(po.Female_50) Female_50, SUM(po.Female_51) Female_51, 
                    SUM(po.Female_52) Female_52, SUM(po.Female_53) Female_53, SUM(po.Female_54) Female_54, SUM(po.Female_55) Female_55, 
                    SUM(po.Female_56) Female_56, SUM(po.Female_57) Female_57, SUM(po.Female_58) Female_58, SUM(po.Female_59) Female_59, 
                    SUM(po.Female_60) Female_60, SUM(po.Female_61) Female_61, SUM(po.Female_62) Female_62, SUM(po.Female_63) Female_63, 
                    SUM(po.Female_64) Female_64, SUM(po.Female_65) Female_65, SUM(po.Female_66) Female_66, SUM(po.Female_67) Female_67, 
                    SUM(po.Female_68) Female_68, SUM(po.Female_69) Female_69, SUM(po.Female_70) Female_70, SUM(po.Female_71) Female_71, 
                    SUM(po.Female_72) Female_72, SUM(po.Female_73) Female_73, SUM(po.Female_74) Female_74, SUM(po.Female_75) Female_75, 
                    SUM(po.Female_76) Female_76, SUM(po.Female_77) Female_77, SUM(po.Female_78) Female_78, SUM(po.Female_79) Female_79, 
                    SUM(po.Female_80) Female_80, SUM(po.Female_81) Female_81, SUM(po.Female_82) Female_82, SUM(po.Female_83) Female_83, 
                    SUM(po.Female_84) Female_84, SUM(po.Female_85) Female_85, SUM(po.Female_86) Female_86, SUM(po.Female_87) Female_87, 
                    SUM(po.Female_88) Female_88, SUM(po.Female_89) Female_89, SUM(po.Female_90) Female_90, SUM(po.Female_91) Female_91, 
                    SUM(po.Female_92) Female_92, SUM(po.Female_93) Female_93, SUM(po.Female_94) Female_94, SUM(po.Female_95) Female_95, 
                    SUM(po.Female_96) Female_96, SUM(po.Female_97) Female_97, SUM(po.Female_98) Female_98, SUM(po.Female_99) Female_99, 
                    SUM(po.Female_100) Female_100,
                    SUM(po.Male_0)  Male_0 , SUM(po.Male_1)  Male_1,  SUM(po.Male_2)  Male_2,  SUM(po.Male_3)  Male_3,  
                    SUM(po.Male_4)  Male_4,  SUM(po.Male_5)  Male_5,  SUM(po.Male_6)  Male_6,  SUM(po.Male_7)  Male_7,  
                    SUM(po.Male_8)  Male_8,  SUM(po.Male_9)  Male_9,  SUM(po.Male_10) Male_10, SUM(po.Male_11) Male_11, 
                    SUM(po.Male_12) Male_12, SUM(po.Male_13) Male_13, SUM(po.Male_14) Male_14, SUM(po.Male_15) Male_15, 
                    SUM(po.Male_16) Male_16, SUM(po.Male_17) Male_17, SUM(po.Male_18) Male_18, SUM(po.Male_19) Male_19, 
                    SUM(po.Male_20) Male_20, SUM(po.Male_21) Male_21, SUM(po.Male_22) Male_22, SUM(po.Male_23) Male_23, 
                    SUM(po.Male_24) Male_24, SUM(po.Male_25) Male_25, SUM(po.Male_26) Male_26, SUM(po.Male_27) Male_27, 
                    SUM(po.Male_28) Male_28, SUM(po.Male_29) Male_29, SUM(po.Male_30) Male_30, SUM(po.Male_31) Male_31, 
                    SUM(po.Male_32) Male_32, SUM(po.Male_33) Male_33, SUM(po.Male_34) Male_34, SUM(po.Male_35) Male_35, 
                    SUM(po.Male_36) Male_36, SUM(po.Male_37) Male_37, SUM(po.Male_38) Male_38, SUM(po.Male_39) Male_39, 
                    SUM(po.Male_40) Male_40, SUM(po.Male_41) Male_41, SUM(po.Male_42) Male_42, SUM(po.Male_43) Male_43, 
                    SUM(po.Male_44) Male_44, SUM(po.Male_45) Male_45, SUM(po.Male_46) Male_46, SUM(po.Male_47) Male_47, 
                    SUM(po.Male_48) Male_48, SUM(po.Male_49) Male_49, SUM(po.Male_50) Male_50, SUM(po.Male_51) Male_51, 
                    SUM(po.Male_52) Male_52, SUM(po.Male_53) Male_53, SUM(po.Male_54) Male_54, SUM(po.Male_55) Male_55, 
                    SUM(po.Male_56) Male_56, SUM(po.Male_57) Male_57, SUM(po.Male_58) Male_58, SUM(po.Male_59) Male_59, 
                    SUM(po.Male_60) Male_60, SUM(po.Male_61) Male_61, SUM(po.Male_62) Male_62, SUM(po.Male_63) Male_63, 
                    SUM(po.Male_64) Male_64, SUM(po.Male_65) Male_65, SUM(po.Male_66) Male_66, SUM(po.Male_67) Male_67, 
                    SUM(po.Male_68) Male_68, SUM(po.Male_69) Male_69, SUM(po.Male_70) Male_70, SUM(po.Male_71) Male_71, 
                    SUM(po.Male_72) Male_72, SUM(po.Male_73) Male_73, SUM(po.Male_74) Male_74, SUM(po.Male_75) Male_75, 
                    SUM(po.Male_76) Male_76, SUM(po.Male_77) Male_77, SUM(po.Male_78) Male_78, SUM(po.Male_79) Male_79, 
                    SUM(po.Male_80) Male_80, SUM(po.Male_81) Male_81, SUM(po.Male_82) Male_82, SUM(po.Male_83) Male_83, 
                    SUM(po.Male_84) Male_84, SUM(po.Male_85) Male_85, SUM(po.Male_86) Male_86, SUM(po.Male_87) Male_87, 
                    SUM(po.Male_88) Male_88, SUM(po.Male_89) Male_89, SUM(po.Male_90) Male_90, SUM(po.Male_91) Male_91, 
                    SUM(po.Male_92) Male_92, SUM(po.Male_93) Male_93, SUM(po.Male_94) Male_94, SUM(po.Male_95) Male_95, 
                    SUM(po.Male_96) Male_96, SUM(po.Male_97) Male_97, SUM(po.Male_98) Male_98, SUM(po.Male_99) Male_99, 
                    SUM(po.Male_100) Male_100,
                    SUM(po.Total_F) Total_F,
                    SUM(po.Total_M) Total_M,
                    SUM(po.Total) Total
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
    
    # group by 
    if normalized == 'no':
        if age == 'no':
            if gr_province == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Date",
                    po."Year"
                '''
            if gr_region == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , r.Region ,
                    po."Date",
                    po."Year"
                '''
        else:
            if gr_province == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , p.Province ,
                    p.Id_region , r.Region ,
                    po."Date",
                    po."Year"
                '''
            if gr_region == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , r.Region ,
                    po."Date",
                    po."Year"
                '''
    else:
        if age == 'no':
            if gr_province == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , 
                    p.Id_region , 
                    po."Date",
                    po."Year"
                '''
            if gr_region == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , 
                    po."Date",
                    po."Year"
                '''
        else:
            if gr_province == 'yes':
                query = query + f'''
                GROUP BY c.Id_province , 
                    p.Id_region , 
                    po."Date",
                    po."Year"
                '''
            if gr_region == 'yes':
                query = query + f'''
                GROUP BY p.Id_region , 
                    po."Date",
                    po."Year"
                '''

    #print(query)

    try:
        dict_data = pd.read_sql_query(query, engineDB).to_dict(orient='records')
    except:
        dict_data = {}
    
    return dict_data

# get type and description of columns
def get_pop_metadata(data, name):
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

# get population by city, province or region
def get_population(year, id_city=None, id_province=None, id_region=None, age='no', gr_province='no', gr_region='no', normalized='no'):
    # connect
    engineDB = connect_DB()
    # select data
    result_data = get_pop(engineDB, year, id_city, id_province, id_region, age, gr_province, gr_region, normalized)
    # build metadata
    result_metadata = get_pop_metadata(result_data, 'population_ine')
    
    # set final result
    result = {}
    result['data'] = result_data
    result['metadata'] = result_metadata

    return result
