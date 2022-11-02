from flask import Flask
from flask import request
from flask import jsonify
from modules.get import get_dimensions as dim
from modules.get import get_income_ine as income_ine
from modules.get import get_population_ine as pop_ine
from modules.get import get_income_aeat as income_aeat

 
app = Flask(__name__)


# Ini
@app.route('/')
def ini():
    return 'Collect'



###############
### Dimensions
###############

# get cities
@app.route('/cities', methods = ['GET'])
def api_cities():
    ''' Get the available cities in Spain'''

    data = dim.get_cities()
    return data

# get provinces
@app.route('/provinces', methods = ['GET'])
def api_provinces():
    ''' Get the available provinces in Spain'''

    data = dim.get_provinces()
    return data

# get regions
@app.route('/regions', methods = ['GET'])
def api_regions():
    ''' Get the available regions in Spain'''

    data = dim.get_regions()
    return data

# get indicators income INE
@app.route('/indicators-income-ine', methods = ['GET'])
def api_indicators_income():
    ''' Get the available indicators income by INE'''
    
    data = dim.get_indicators_income()
    return data



###############
### Income AEAT
###############

# get years income aeat
@app.route('/income-aeat/years', methods = ['GET'])
def api_income_aeat_years():
    ''' Get the available years for income in Spain by AEAT'''

    # get data
    data = income_aeat.get_years()
    return data

# get income aeat for cities for a year
@app.route('/income-aeat/cities/year/<year>', methods = ['GET'])
def api_income_aeat_every_city(year):
    ''' Get income in Spain by AEAT for every available city in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_aeat.get_incomes(year=year)

    return data

# get income aeat for cities for a year
@app.route('/income-aeat/city/<id_city>/year/<year>', methods = ['GET'])
def api_income_aeat_city(year, id_city=None):
    ''' Get income in Spain by AEAT for specific city in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_aeat.get_incomes(year=year, id_city=id_city, normalized=id_normalized)

    return data

# get income aeat for cities for a year
@app.route('/income-aeat/province/<id_province>/cities/year/<year>', methods = ['GET'])
def api_income_aeat_city_pronvince(year, id_province=None):
    ''' Get income in Spain by AEAT for every available city for specific province in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_aeat.get_incomes(year=year, id_province=id_province, normalized=id_normalized)

    return data

# get income aeat for cities for a year
@app.route('/income-aeat/region/<id_region>/cities/year/<year>', methods = ['GET'])
def api_income_aeat_city_region(year, id_region=None):
    ''' Get income in Spain by AEAT for every available city for specific region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_aeat.get_incomes(year=year, id_region=id_region, normalized=id_normalized)

    return data



##############
### Income INE
##############

# get years income ine
@app.route('/income-ine/years', methods = ['GET'])
def api_incomes_ine_years():
    ''' Get the available years for income in Spain by INE'''
    
    # get data
    data = income_ine.get_years()
    return data

# get income ine for cities for a year
@app.route('/income-ine/cities/year/<year>', methods = ['GET'])
def api_incomes_ine_every_city(year):
    ''' Get income in Spain by INE for every available city in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_ine.get_incomes(year=year, normalized=id_normalized)
    json_data = jsonify(data)
    
    return json_data

# get income ine for cities for a year
@app.route('/income-ine/city/<id_city>/year/<year>', methods = ['GET'])
def api_incomes_ine_city(year, id_city=None):
    ''' Get income in Spain by INE for specific city in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_ine.get_incomes(year=year, id_city=id_city, normalized=id_normalized)

    return data

# get income ine for cities for a year
@app.route('/income-ine/province/<id_province>/cities/year/<year>', methods = ['GET'])
def api_incomes_ine_city_province(year, id_province=None):
    ''' Get income in Spain by INE for every available city for specific province in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_ine.get_incomes(year=year, id_province=id_province, normalized=id_normalized)

    return data

# get income ine for cities for a year
@app.route('/income-ine/region/<id_region>/cities/year/<year>', methods = ['GET'])
def api_incomes_ine_city_region(year, id_region=None):
    ''' Get income in Spain by INE for every available city for specific region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_ine.get_incomes(year=year, id_region=id_region, normalized=id_normalized)

    return data



##################
### Population INE
##################

# get years population ine
@app.route('/population-ine/years', methods = ['GET'])
def api_population_ine_years():
    ''' Get the available years for population in Spain by INE'''
    
    # get data
    data = pop_ine.get_years()
    return data

# get population ine for cities for a year
@app.route('/population-ine/cities/year/<year>', methods = ['GET'])
def api_population_ine_every_city(year):
    ''' Get population in Spain by INE for every available city in a specific year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, age=id_age, normalized=id_normalized)

    return data

# get population ine for cities for a year
@app.route('/population-ine/city/<id_city>/year/<year>', methods = ['GET'])
def api_population_ine_city(year, id_city=None):
    ''' Get population in Spain by INE for specific city in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_city=id_city, age=id_age, normalized=id_normalized)

    return data

# get population ine for cities for a year
@app.route('/population-ine/province/<id_province>/cities/year/<year>', methods = ['GET'])
def api_population_ine_city_province(year, id_province=None):
    ''' Get population in Spain by INE for every available city for specific province in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_province=id_province, age=id_age, normalized=id_normalized)

    return data

# get population ine for cities for a year
@app.route('/population-ine/region/<id_region>/cities/year/<year>', methods = ['GET'])
def api_population_ine_city_region(year, id_region=None):
    ''' Get population in Spain by INE for every available city for specific region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_region=id_region, age=id_age, normalized=id_normalized)

    return data

# get population ine for provinces for a year
@app.route('/population-ine/provinces/year/<year>', methods = ['GET'])
def api_population_ine_every_province(year):
    ''' Get population in Spain by INE for every available province in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, age=id_age, gr_province='yes', normalized=id_normalized)
    
    return data

# get population ine for provinces for a year
@app.route('/population-ine/province/<id_province>/year/<year>', methods = ['GET'])
def api_population_ine_province(year, id_province=None):
    ''' Get population in Spain by INE for specific province in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_province=id_province, age=id_age, gr_province='yes', normalized=id_normalized)
    
    return data

# get population ine for provinces for a year
@app.route('/population-ine/region/<id_region>/provinces/year/<year>', methods = ['GET'])
def api_population_ine_province_region(year, id_region=None):
    ''' Get population in Spain by INE for every available province for specific region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_region=id_region, age=id_age, gr_province='yes', normalized=id_normalized)
    
    return data

# get population ine for regions for a year
@app.route('/population-ine/regions/year/<year>', methods = ['GET'])
def api_population_ine_every_region(year):
    ''' Get population in Spain by INE for every available region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, age=id_age, gr_region='yes', normalized=id_normalized)

    return data

# get population ine for regions for a year
@app.route('/population-ine/region/<id_region>/year/<year>', methods = ['GET'])
def api_population_ine_region(year, id_region=None):
    ''' Get population in Spain by INE for specific region in a year'''
    
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_region=id_region, age=id_age, gr_region='yes', normalized=id_normalized)

    return data
