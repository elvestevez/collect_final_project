from turtle import width
import streamlit as st
from PIL import Image
import pandas as pd
import matplotlib.pylab as plt
import seaborn as sns


URL_API = 'http://localhost:9080'
ICON_PATH = '/home/elvira/Documents/IHData/projects/final/icon.png'
OPTION_YES = 'Yes'
OPTION_NO = 'No'
ENCODE = 'utf-8'


# Home
def play_home():
    pass

# Population collect
def play_population():
    st.header('Population in Spain')
    st.text('get population')
    
# Incomes collect
def play_incomes():
    st.header('Incomes in Spain')
    st.text('get incomes')

# side bar
st.sidebar.title('Menu')
st.sidebar.text('Select info')

if st.sidebar.button('Home'):
    play_home()
if st.sidebar.button('Population'):
    play_population()
if st.sidebar.button('Incomes'):
    play_incomes()


# get dimensions
def get_api_years():
    d = [2018, 2019, 2020, 2021]
    return d

def get_api_cities():
    df = pd.read_json(URL_API + '/cities')
    d = df['City']
    return d

def get_api_provinces():
    df = pd.read_json(URL_API + '/provinces')
    d = df['Province']
    return d

def get_api_regions():
    df = pd.read_json(URL_API + '/regions')
    d = df['Region']
    return d


# get population
def get_api_population(year, age, gender):
    if age == OPTION_YES:
        gr_age = 'no'
    else:
        gr_age = 'yes'
    if gender == OPTION_YES:
        gr_gender = 'no'
    else:
        gr_gender = 'yes'
    st.write(URL_API + f'/population/{year}?gr_age={gr_age}&gr_gender={gr_gender}')
    df = pd.read_json(URL_API + f'/population/{year}?gr_age={gr_age}&gr_gender={gr_gender}')
    return df

def export_to_csv(df, encode):
    return df.to_csv().encode(encode)

def get_plot(df):
    fig, ax = plt.subplots(1, 1)
    sns.barplot(data=df.nlargest(5, 'Total'), 
                x='City', 
                y='Total').set(xlabel='')
    return fig


# page
st.image(Image.open(ICON_PATH), width=60)
st.title('Collects')
st.text('Datasets ready for use!')
st.text('')
st.text('')


# population
st.header('Population in Spain')
st.text('Get distribution of population by city')

# options
selected_year = st.slider('Year: ', 2018, 2021)
selected_age = st.radio('By age: ', [OPTION_NO, OPTION_YES], horizontal=True)
selected_gender = st.radio('By gender: ', [OPTION_NO, OPTION_YES], horizontal=True)
#select_cities = st.selectbox('City: ', get_api_cities())
#select_provin = st.selectbox('Province: ', get_api_provinces())
#select_region = st.selectbox('Region: ', get_api_regions())

# get data
df_pop = get_api_population(selected_year, selected_age, selected_gender)
f_csv = export_to_csv(df_pop, ENCODE)

# download
st.download_button(label="Download data as CSV",
                   data=f_csv,
                   file_name=f'population_{selected_year}.csv',
                   mime='text/csv')

# metadata
st.subheader('Metadata')
st.write(f'Number of rows: ', len(df_pop))
st.write(f'Number of columns: ', len(df_pop.columns))
st.write(df_pop.columns)

# data
st.subheader('Data')
st.write('First 10 rows')
st.write(df_pop.head(10))

# dashboard
st.subheader('Dashboard')
st.pyplot(get_plot(df_pop))
