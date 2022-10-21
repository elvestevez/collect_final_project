from flask import Flask
from flask import request
from modules import get_dimensions as dim
from modules import get_incomes as income
from modules import get_population as pop

 
app = Flask(__name__)


# Ini
@app.route('/')
def ini():
    return 'Collect'

# get cities
@app.route('/cities')
def api_cities():
    data = dim.get_cities()
    return data

# get province
@app.route('/provinces')
def api_provinces():
    data = dim.get_provinces()
    return data

# get regions
@app.route('/regions')
def api_regions():
    data = dim.get_regions()
    return data

# get ages
@app.route('/ages')
def api_ages():
    data = dim.get_ages()
    return data

# get genres
@app.route('/genres')
def api_genres():
    data = dim.get_genres()
    return data

# get indicators income
@app.route('/indicators_incomes')
def api_indicators_incomes():
    data = dim.get_indicators_incomes()
    return data

# get incomes year for city by city, province or region
@app.route('/income/<year>')
@app.route('/income/<year>/city/<city>')
@app.route('/income/<year>/province/<province>')
@app.route('/income/<year>/region/<region>')
def api_incomes_city(year, city=None, province=None, region=None):
    # params
    id_indicator = request.args.get('id_indicator', None, type=str)
    id_normalized = request.args.get('id_normalized', 'no', type=str)
    
    # get data
    data = income.get_incomes(year=year, id_city=city, id_province=province, id_region=region, id_indicator=id_indicator, normalized=id_normalized)

    return data

# get population year for city by city, province or region
@app.route('/population/<year>')
@app.route('/population/<year>/city/<city>')
@app.route('/population/<year>/province/<province>')
@app.route('/population/<year>/region/<region>')
def api_population_city(year, city=None, province=None, region=None):
    # params
    gr_gender = request.args.get('gr_gender', 'no', type=str)
    gr_age = request.args.get('gr_age', 'no', type=str)
    id_normalized = request.args.get('id_normalized', 'no', type=str)
    
    # get data
    data = pop.get_population(year=year, id_city=city, id_province=province, id_region=region, gr_gender=gr_gender, gr_age=gr_age, normalized=id_normalized)

    return data

# get population year for provinces by province or region
@app.route('/population/<year>/provinces')
@app.route('/population/<year>/provinces/province/<province>')
@app.route('/population/<year>/provinces/region/<region>')
def api_population_province(year, province=None, region=None):
    # params
    gr_gender = request.args.get('gr_gender', 'no', type=str)
    gr_age = request.args.get('gr_age', 'no', type=str)
    id_normalized = request.args.get('id_normalized', 'no', type=str)
    
    # get data
    data = pop.get_population(year=year, id_province=province, id_region=region, gr_province='yes', gr_gender=gr_gender, gr_age=gr_age, normalized=id_normalized)
    
    return data

# get population year for regions by region
@app.route('/population/<year>/regions')
@app.route('/population/<year>/regions/region/<region>')
def api_population_region(year, region=None):
    # params
    gr_gender = request.args.get('gr_gender', 'no', type=str)
    gr_age = request.args.get('gr_age', 'no', type=str)
    id_normalized = request.args.get('id_normalized', 'no', type=str)
    
    # get data
    data = pop.get_population(year=year, id_region=region, gr_region='yes', gr_gender=gr_gender, gr_age=gr_age, normalized=id_normalized)

    return data


if __name__ == '__main__':
    app.run(port=9080)
