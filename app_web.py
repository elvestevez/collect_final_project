import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
import time
import pandas as pd
import json
import configparser
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go


ICON_PATH = './img/icon.png'
CONFIG_FILE = 'datatype.properties'
OPTION_YES = 'Yes'
OPTION_NO = 'No'
ENCODE = 'utf-8'
URL_API = 'http://localhost:9080'
type_income_aeat = 'income_aeat'
type_income_ine = 'income_ine'
type_population_ine = 'population_ine'


# get dimensions
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

def get_api_indicators_incomes():
    df = pd.read_json(URL_API + '/indicators_income')
    d = df['Name_indicator']
    return d

# get population INE
#@st.cache(show_spinner=False)
def get_api_population_ine(year, age):
    if age == OPTION_NO:
        age = 'no'
    if age == OPTION_YES:
        age = 'yes'
    url = URL_API + f'/population_ine/{year}?age={age}'
    df = pd.read_json(url)
    return df

# get incomes INE
#@st.cache(show_spinner=False)
def get_api_incomes_ine(year):
    url = URL_API + f'/income_ine/{year}'
    df = pd.read_json(url)
    return df

# get incomes AEAT
#@st.cache(show_spinner=False)
def get_api_incomes_aeat(year):
    url = URL_API + f'/income_aeat/{year}'
    df = pd.read_json(url)
    return df

# get years incomes AEAT
def get_api_incomes_aeat_years():
    url = URL_API + f'/income_aeat/years'
    df = pd.read_json(url)
    if df.empty:
        #return 0, 1
        return []
    else:
        list_years = list(df['Year'])
        #min_y = min(list_years)
        #max_y = max(list_years)
        #return min_y, max_y
        list_years.sort(reverse=True)
        return list_years

# get years incomes INE
def get_api_incomes_ine_years():
    url = URL_API + f'/income_ine/years'
    df = pd.read_json(url)
    if df.empty:
        #return 0, 1
        return []
    else:
        list_years = list(df['Year'])
        #min_y = min(list_years)
        #max_y = max(list_years)
        #return min_y, max_y
        list_years.sort(reverse=True)
        return list_years

# get years population INE
def get_api_population_ine_years():
    url = URL_API + f'/population_ine/years'
    df = pd.read_json(url)
    if df.empty:
        #return 0, 1
        return []
    else:
        list_years = list(df['Year'])
        #min_y = min(list_years)
        #max_y = max(list_years)
        #return min_y, max_y
        list_years.sort(reverse=True)
        return list_years



# export to csv
def export_to_csv(df, encode):
    return df.to_csv(index=False, sep=";").encode(encode)



# get type and description of columns
def get_data_type(df, info):
    # get properties file
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

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
            dict_datatype['text'] = json.loads(config.get(info, c))['text']
            list_datatype.append(dict_datatype)
    
    # create final df
    df_dtype = pd.DataFrame(list_datatype)

    return df_dtype



# build graph income aeat
def build_graph_incomes_aeat(df):
    list_fig = []
    # graph 1
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Top 5 cities',  'Bottom 5 cities'))
    fig.add_trace(go.Bar(x=df.nlargest(5, 'Avg_gross_income')['City'], y=df.nlargest(5, 'Avg_gross_income')['Avg_gross_income']),
                  row=1, col=1
    )
    fig.add_trace(go.Bar(x=df.nsmallest(5, 'Avg_gross_income')['City'], y=df.nsmallest(5, 'Avg_gross_income')['Avg_gross_income']),
                  row=1, col=2
    )
    fig.update_layout(height=400, width=800, showlegend=False)
    list_fig.append(fig)

    return list_fig

# build graph pop ine
def build_graph_population_ine(df):
    list_fig = []
    # graph 1
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Top 5 cities female population',  'Top 5 cities male population'))
    fig.add_trace(go.Bar(x=df.nlargest(5, 'Total_F')['City'], y=df.nlargest(5, 'Total_F')['Total_F']),
                 row=1, col=1
    )
    fig.add_trace(go.Bar(x=df.nlargest(5, 'Total_M')['City'], y=df.nlargest(5, 'Total_M')['Total_M']),
                  row=1, col=2
    )
    fig.update_layout(height=400, width=800, showlegend=False)
    list_fig.append(fig)

    return list_fig

# build graph income ine
def build_graph_incomes_ine(df):
    list_fig = []
    # graph 1
    data = df.loc[(df['Id_indicator'] == 'RNMP') &
                  (df['Total_pop'] > 1000)]
    fig = make_subplots(rows=1, 
                        cols=2,
                        subplot_titles=('Top cities (population >1000)', 'Bottom cities (population >1000)'))
    fig.add_trace(go.Bar(x=data.sort_values(by='Total', ascending=False).head(5)['City'], 
                         y=data.sort_values(by='Total', ascending=False).head(5)['Total']),
                         row=1, col=1)
    fig.add_trace(go.Bar(x=data.sort_values(by='Total', ascending=False).tail(5)['City'], 
                         y=data.sort_values(by='Total', ascending=False).tail(5)['Total']),
                         row=1, col=2)
    fig.update_layout(height=400, 
                      width=800, 
                      showlegend=False, 
                      title_text='Average net income per person (EUR)', title_x=0.5)
    fig.update_yaxes(range=[0, 35000])
    list_fig.append(fig)
    
    # graph 2
    data = df.loc[df['Id_city'] == data.nlargest(1, 'Total').iloc[0]['Id_city']]
    fig = px.bar(data,
                x='City', 
                y='Total',
                color='Name_indicator',
                barmode='group')
    fig.update_layout(height=400, 
                      width=800, 
                      title_text='Top city (population >1000)',
                      legend_title_text='Indicator income (EUR)')
    list_fig.append(fig)

    return list_fig

# get graph
def get_plot(type, df):
    list_fig = []
    if df.empty:
        fig = make_subplots(rows=1, cols=1)
        list_fig.append(fig)
    else:
        if type == type_population_ine:
            list_fig = build_graph_population_ine(df)
        elif type ==type_income_ine:
            list_fig = build_graph_incomes_ine(df)
        elif type ==type_income_aeat:
            list_fig = build_graph_incomes_aeat(df)
        else:
            fig = make_subplots(rows=1, cols=1)
            list_fig.append(fig)
    return list_fig



# get url tableau
def get_url_tableau(type):
    if type == type_income_aeat:
        url = ''
    elif type == type_income_ine:
        url = 'https://public.tableau.com/app/profile/elvira8700/viz/IncomeINE/IncomeINE'
    elif type == type_population_ine:
        url = 'https://public.tableau.com/app/profile/elvira8700/viz/populationINE/Population'
    else:
        url = ''
    return url

# get html tableau
#def get_html_tableau(type):
#    if type == type_income_ine:
#        #get content from tableau (share)
#        html_tab = """<div class='tableauPlaceholder' id='viz1666686443128' style='position: relative'><noscript><a href='#'><img alt='Dashboard 1 ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;In&#47;IncomesINE&#47;Dashboard1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='IncomesINE&#47;Dashboard1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;In&#47;IncomesINE&#47;Dashboard1&#47;1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en-US' /></object></div>                <script type='text/javascript'>                    var divElement = document.getElementById('viz1666686443128');                    var vizElement = divElement.getElementsByTagName('object')[0];                    if ( divElement.offsetWidth > 800 ) { vizElement.style.minWidth='420px';vizElement.style.maxWidth='650px';vizElement.style.width='100%';vizElement.style.minHeight='587px';vizElement.style.maxHeight='887px';vizElement.style.height=(divElement.offsetWidth*0.75)+'px';} else if ( divElement.offsetWidth > 500 ) { vizElement.style.minWidth='420px';vizElement.style.maxWidth='650px';vizElement.style.width='100%';vizElement.style.minHeight='587px';vizElement.style.maxHeight='887px';vizElement.style.height=(divElement.offsetWidth*0.75)+'px';} else { vizElement.style.width='100%';vizElement.style.height='877px';}                     var scriptElement = document.createElement('script');                    scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);                </script>"""
#    else:
#        html_tab = ''
#    return html_tab



# Home
def play_home():
    pass

# display datatype
def write_datatype(df):
    text = ""
    for index, row in df.iterrows():
        if row['type'] == 'string':
            st.markdown(f"&emsp;🇹 ({row['type']}) {row['id']}: {row['text']}")
            #text = text + f"&emsp;🇹 {row['id']}: {row['text']}"
        if row['type'] == 'integer':
            st.markdown(f"&emsp;🔢 ({row['type']}) {row['id']}: {row['text']}")
            #text = text + f"&emsp;🔢 {row['id']}: {row['text']}"
        if row['type'] == 'float':
            st.markdown(f"&emsp;🔢 ({row['type']}) {row['id']}: {row['text']}")
            #text = text + f"&emsp;🔢 {row['id']}: {row['text']}"
        if row['type'] == 'date':
            st.markdown(f"&emsp;🗓️ ({row['type']}) {row['id']}: {row['text']}")
            #text = text + f"&emsp;D {row['id']}: {row['text']}"

# Population INE collect
def play_population_ine():
    # header
    st.header('Population in Spain')
    st.write('Get population distribution by city')
    st.markdown('Source data [Instituto Nacional de Estadística](https://www.ine.es/)')


    # options
    #min_y_p, max_y_p = get_api_population_ine_years()
    #selected_year_p = st.slider('Year: ', min_value=min_y_p, max_value=max_y_p, key='year_pop_ine')
    list_years_pop_i = get_api_population_ine_years()
    selected_year_p = st.radio('Year: ', list_years_pop_i, key='year_pop_ine', horizontal=True)
    selecter_age = st.radio('By age:', [OPTION_NO, OPTION_YES])
    #select_cities = st.selectbox('City: ', get_api_cities())
    
    # get data
    with st.spinner('Loading...'):
        df_pop = get_api_population_ine(selected_year_p, selecter_age)
        
        f_csv = export_to_csv(df_pop, ENCODE)

        # download
        st.download_button(
            label="Download data as CSV",
            data=f_csv,
            file_name=f'population_INE_{selected_year_p}.csv',
            mime='text/csv'
        )

        # description of data
        st.subheader('Data description')
        st.write(f'Number of rows: ', len(df_pop))
        st.write(f'Number of columns: ', len(df_pop.columns))
        df_pop_dtype = get_data_type(df_pop, type_population_ine)
        write_datatype(df_pop_dtype)
        #st.markdown(text_datatype)

        # data sample
        st.subheader('Data sample')
        st.write('First 5 rows')
        st.write(df_pop.head(5))

        # dashboard
        st.subheader('Data overview')
        # plot
        plots_pop = get_plot(type_population_ine, df_pop)
        for gr_pop in plots_pop:
            st.plotly_chart(gr_pop, use_container_width=True)
        # url tableau
        url_tab_p = get_url_tableau(type_population_ine)
        st.write(f'For more visualizations, you can view the [Tableau Dashboard]({url_tab_p})')
        # component tableau
        #html_tab_p = get_html_tableau(type_population_ine)
        #components.html(html_tab_p)


# Income INE collect
def play_income_ine():
    # header
    st.header('Income in Spain')
    st.write('Get income distribution by city')
    st.markdown('Source data [Instituto Nacional de Estadística](https://www.ine.es/)')
    
    # options
    #min_y_i, max_y_i = get_api_incomes_ine_years()
    #selected_year_i = st.slider('Year: ', min_value=min_y_i, max_value=max_y_i, key='year_incomes_ine')
    list_years_in_i = get_api_incomes_ine_years()
    selected_year_i = st.radio('Year: ', list_years_in_i, key='year_in_ine', horizontal=True)
    
    # get data
    with st.spinner('Loading...'):
        df_incomes_ine = get_api_incomes_ine(selected_year_i)

        f_csv = export_to_csv(df_incomes_ine, ENCODE)

        # download
        st.download_button(
            label="Download data as CSV",
            data=f_csv,
            file_name=f'incomes_INE_{selected_year_i}.csv',
            mime='text/csv'
        )

        # datatype
        st.subheader('Data description')
        st.write(f'Number of rows: ', len(df_incomes_ine))
        st.write(f'Number of columns: ', len(df_incomes_ine.columns))
        df_incomes_ine_dtype = get_data_type(df_incomes_ine, type_income_ine)
        write_datatype(df_incomes_ine_dtype)
        #st.markdown(text_datatype)

        # data
        st.subheader('Data sample')
        st.write('First 5 rows')
        st.write(df_incomes_ine.head(5))

        # dashboard
        st.subheader('Data overview')
        # plot
        plots_in_i = get_plot(type_income_ine, df_incomes_ine)
        for gr_in_i in plots_in_i:
            st.plotly_chart(gr_in_i, use_container_width=True)
        # url tableau
        url_tab_i = get_url_tableau(type_income_ine)
        st.write(f'For more visualizations, you can view the [Tableau Dashboard]({url_tab_i})')
        # component tableau
        #html_tab_i = get_html_tableau(type_income_ine)
        #components.html(html_tab_i)
    
# Income AEAT collect
def play_income_aeat():
    # header
    st.header('Income in Spain')
    st.write('Get income distribution by city')
    st.markdown('Source data [Agencia Estatal de Administración Tributaria](https://sede.agenciatributaria.gob.es/)')

    # options
    #min_y_i_aeat, max_y_i_aeat = get_api_incomes_aeat_years()
    #selected_year_i_aeat = st.slider('Year: ', min_value=min_y_i_aeat, max_value=max_y_i_aeat, key='year_incomes_aeat')
    list_years_in_aeat = get_api_incomes_aeat_years()
    selected_year_i_aeat = st.radio('Year: ', list_years_in_aeat, key='year_in_aeat', horizontal=True)
    
    # get data
    with st.spinner('Loading...'):
        df_incomes_aeat = get_api_incomes_aeat(selected_year_i_aeat)

        f_csv = export_to_csv(df_incomes_aeat, ENCODE)

        # download
        st.download_button(
            label="Download data as CSV",
            data=f_csv,
            file_name=f'incomes_AEAT_{selected_year_i_aeat}.csv',
            mime='text/csv'
        )

        # datatype
        st.subheader('Data description')
        st.write(f'Number of rows: ', len(df_incomes_aeat))
        st.write(f'Number of columns: ', len(df_incomes_aeat.columns))
        df_incomes_aeat_dtype = get_data_type(df_incomes_aeat, type_income_aeat)
        write_datatype(df_incomes_aeat_dtype)
        #st.markdown(text_datatype)

        # data
        st.subheader('Data sample')
        st.write('First 5 rows')
        st.write(df_incomes_aeat.head(5))

        # dashboard
        st.subheader('Data overview')
        # plot
        plots_in_a = get_plot(type_income_aeat, df_incomes_aeat)
        for gr_in_a in plots_in_a:
            st.plotly_chart(gr_in_a, use_container_width=True)
        # url tableau
        url_tab_ia = get_url_tableau(type_income_aeat)
        st.write(f'For more visualizations, you can view the [Tableau Dashboard]({url_tab_ia})')
        # component tableau
        #html_tab_ia = get_html_tableau(type_income_aeat)
        #components.html(html_tab_ia)



# page config
st.set_page_config(
    page_title='collect'#,
    #page_icon='',
    #layout="wide"
)



# page
st.image(Image.open(ICON_PATH), width=60)
st.title('Collects')
st.text('Datasets ready for use!')
st.text('')



tab_pop_ine, tab_income_ine, tab_income_aeat = st.tabs(['Population INE', 'Income INE', 'Income AEAT'])

with tab_pop_ine:
   play_population_ine()

with tab_income_ine:
   play_income_ine()

with tab_income_aeat:
   play_income_aeat()
