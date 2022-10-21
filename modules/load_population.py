import os
import pandas as pd
from modules import to_db as db
from modules import db_integrity as db_int


POPULATION_PATH = './data/population/'
POPULATION_TABLE = 'POPULATION'


# save population in db
def save_population(df, name_table):
    db.to_sqlite(df, name_table)

# clean population df
def data_clean_population(df_in, year):
    # filter only city, period
    df_pop = df_in.loc[(df_in['Municipios'].notnull()) &
                       (df_in['Periodo'] == f'1 de enero de {year}')].copy()
    
    # filter sex, age
    df_pop = df_pop.loc[(df_pop['Sexo'] != 'Total') &
                        (df_pop['Edad (año a año)'] != 'Todas las edades')]

    # get year
    df_pop['Year'] = df_pop['Periodo'].str.split(' ').str[-1]
    
    # set date
    df_pop['Day'] = 1
    df_pop['Month'] = 1
    
    # convert Year to int
    df_pop['Year'] = df_pop['Year'].astype('int')
    
    # divide city in cod and description
    df_pop[['Id_city', 'City']] = df_pop['Municipios'].str.split(' ', n=1, expand=True)
    
    # get only years old
    df_pop['Id_age'] = df_pop['Edad (año a año)'].str.split(' ', n=1).str[0]
    
    # encoding
    df_pop['Id_gender'] = df_pop['Sexo'].str.replace('Hombres', 'M').str.replace('Mujeres', 'F').str.strip()

    # convert total to number 
    df_pop['Total'] = pd.to_numeric(df_pop['Total'], errors='coerce')

    # drop columns
    df_pop.drop(['Provincias', 'Municipios'], axis=1, inplace=True)

    # drop rows with null value
    df_pop.dropna(inplace=True)

    # rename and reorder cols
    cols = ['Id_city', 'Id_gender', 'Id_age', 'Day', 'Month', 'Year', 'Total']
    df_pop = df_pop[cols]
    
    return df_pop

# clean population (national) df
def data_clean_national_population(df_in, year):
    # filter sex, age, period
    df_pop = df_in.loc[(df_in['Sexo'] != 'Total') &
                       (df_in['Edad (año a año)'] != 'Todas las edades') &
                       (df_in['Municipios'] != 'Total Nacional') &
                       (df_in['Periodo'] == f'1 de enero de {year}')].copy()
    
    # get year
    df_pop['Year'] = df_pop['Periodo'].str.split(' ').str[-1]
    
    # set date
    df_pop['Day'] = 1
    df_pop['Month'] = 1
    
    # convert Year to int
    df_pop['Year'] = df_pop['Year'].astype('int')
    
    # divide city in cod and description
    df_pop[['Id_city', 'City']] = df_pop['Municipios'].str.split(' ', n=1, expand=True)
    
    # get only years old
    df_pop['Id_age'] = df_pop['Edad (año a año)'].str.split(' ', n=1).str[0]

    # encoding
    df_pop['Id_gender'] = df_pop['Sexo'].str.replace('Hombres', 'M').str.replace('Mujeres', 'F').str.strip()
    
    # convert total to number 
    df_pop['Total'] = pd.to_numeric(df_pop['Total'], errors='coerce')

    # drop columns
    df_pop.drop(['Municipios'], axis=1, inplace=True)

    # drop rows with null value
    df_pop.dropna(inplace=True)

    # rename and reorder cols
    cols = ['Id_city', 'Id_gender', 'Id_age', 'Day', 'Month', 'Year', 'Total']
    df_pop = df_pop[cols]
    
    return df_pop

# read population file
def read_population(file, year):
    df_pop = pd.read_csv(file, sep=';', converters={'Total': lambda x: x.replace('.', '')})
    #df_population = data_clean_population(df_pop, year)
    df_population = data_clean_national_population(df_pop, year)

    return df_population

# read and save in db population
def load_population(year):
    # population files
    files = os.listdir(POPULATION_PATH)
    
    # df final
    df_population = pd.DataFrame([])
    for f in files:
        # read file population
        df_pop = read_population(POPULATION_PATH + f, year)
        # join population
        df_population = pd.concat([df_population, df_pop])
    
    # save population in db
    print(len(df_population))
    name_table = year + '_' + POPULATION_TABLE
    save_population(df_population, name_table)

# check integrity incomes
def check_integrity_population(year):
    table_name = year + '_' + POPULATION_TABLE
    msg = f'Integrity population {table_name}: '
    city_ok = db_int.integrity_city(table_name)
    ages_ok = db_int.integrity_age(table_name)
    gend_ok = db_int.integrity_gender(table_name)
    if city_ok and ages_ok and gend_ok:
        msg = msg + '\n   Ok'
    if not city_ok:
        msg = msg + '\n   Error cities'
    if not ages_ok:
        msg = msg + '\n   Error ages'
    if not gend_ok:
        msg = msg + '\n   Error genres'
    return msg
