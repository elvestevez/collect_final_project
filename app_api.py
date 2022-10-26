from flask import Flask
from flask import request
from modules.get import get_dimensions as dim
from modules.get import get_incomes_ine as income_ine
from modules.get import get_population_ine as pop_ine
from modules.get import get_incomes_aeat as income_aeat

 
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

# get regions
@app.route('/indicators_incomes')
def api_indicators_incomes():
    data = dim.get_indicators_incomes()
    return data

# get years incomes aeat
@app.route('/incomes_aeat/years')
def api_incomes_aeat_years():
    # get data
    data = income_aeat.get_years()
    return data

# get incomes aeat year for city
@app.route('/incomes_aeat/<year>')
@app.route('/incomes_aeat/<year>/city/<city>')
@app.route('/incomes_aeat/<year>/province/<province>')
@app.route('/incomes_aeat/<year>/region/<region>')
def api_incomes_aeat_city(year, city=None, province=None, region=None):
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_aeat.get_incomes(year=year, id_city=city, id_province=province, id_region=region, normalized=id_normalized)

    return data

# get years incomes ine
@app.route('/incomes_ine/years')
def api_incomes_ine_years():
    # get data
    data = income_ine.get_years()
    return data

# get incomes ine year for city
@app.route('/incomes_ine/<year>')
@app.route('/incomes_ine/<year>/city/<city>')
@app.route('/incomes_ine/<year>/province/<province>')
@app.route('/incomes_ine/<year>/region/<region>')
def api_incomes_ine_city(year, city=None, province=None, region=None):
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    
    # get data
    data = income_ine.get_incomes(year=year, id_city=city, id_province=province, id_region=region, normalized=id_normalized)

    return data

# get years population ine
@app.route('/population_ine/years')
def api_population_ine_years():
    # get data
    data = pop_ine.get_years()
    return data

# get population year for city
@app.route('/population_ine/<year>')
@app.route('/population_ine/<year>/city/<city>')
@app.route('/population_ine/<year>/province/<province>')
@app.route('/population_ine/<year>/region/<region>')
def api_population_ine_city(year, city=None, province=None, region=None):
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_city=city, id_province=province, id_region=region, age=id_age, normalized=id_normalized)

    return data

# get population year for provinces
@app.route('/population_ine/<year>/provinces')
@app.route('/population_ine/<year>/provinces/province/<province>')
@app.route('/population_ine/<year>/provinces/region/<region>')
def api_population_ine_province(year, province=None, region=None):
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_province=province, id_region=region, age=id_age, normalized=id_normalized)
    
    return data

# get population year for regions
@app.route('/population_ine/<year>/regions')
@app.route('/population_ine/<year>/regions/region/<region>')
def api_population_ine_region(year, region=None):
    # params
    id_normalized = request.args.get('normalized', 'no', type=str)
    id_age = request.args.get('age', 'no', type=str)
    
    # get data
    data = pop_ine.get_population(year=year, id_region=region, age=id_age, normalized=id_normalized)

    return data


if __name__ == '__main__':
    app.run(port=9080)
