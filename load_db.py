import time
from modules import load_dimensions as dim
from modules import load_incomes as income
from modules import load_population as pop


def main():

    ### Dimensions
    print('\nLoad dimensions...')
    start = time.time()
    dim.load_dimensions()
    print(dim.check_integrity_dimensions())
    end = time.time()
    print(f'time: {round(end-start, 2)} seg')

    # Incomes 2018-2020
    ###print('\nLoad incomes...')
    ###start = time.time()
    ###income.load_incomes('2018')
    ###print(income.check_integrity_income('2018'))
    ###income.load_incomes('2019')
    ###print(income.check_integrity_income('2019'))
    ###income.load_incomes('2020')
    ###print(income.check_integrity_income('2020'))
    ###end = time.time()
    ###print(f'time: {round(end-start, 2)} seg')
    
    # Population 2018-2021
    ###print('\nLoad population...')
    ###start = time.time()
    ###pop.load_population('2018')
    ###print(pop.check_integrity_population('2018'))
    ###pop.load_population('2019')
    ###print(pop.check_integrity_population('2019'))
    ###pop.load_population('2020')
    ###print(pop.check_integrity_population('2020'))
    ###pop.load_population('2021')
    ###print(pop.check_integrity_population('2021'))
    ###end = time.time()
    ###print(f'time: {round(end-start, 2)} seg')
    

if __name__ == '__main__':
    main()
