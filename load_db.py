import time
from modules.load import load_dimensions as dim
from modules.load import load_incomes_aeat as in_aeat
from modules.load import load_incomes_ine as in_ine
from modules.load import load_population_ine as pop_ine


def main():

    # Dimensions
    print('\nLoad dimensions...')
    start = time.time()
    dim.load_dimensions()
    print(dim.check_integrity_dimensions())
    end = time.time()
    print(f'time: {round(end-start, 2)} seg')

    # Incomes 2019-2020
    ###print('\nLoad incomes AEAT...')
    ###start = time.time()
    ###in_aeat.load_incomes('2018')
    ###print(in_aeat.check_integrity_incomes('2018'))
    ###in_aeat.load_incomes('2019')
    ###print(in_aeat.check_integrity_incomes('2019'))
    ###in_aeat.load_incomes('2020')
    ###print(in_aeat.check_integrity_incomes('2020'))
    ###end = time.time()
    ###print(f'time: {round(end-start, 2)} seg')

    # Incomes INE 2018-2020
    print('\nLoad incomes INE...')
    start = time.time()
    in_ine.load_incomes('2018')
    print(in_ine.check_integrity_incomes('2018'))
    in_ine.load_incomes('2019')
    print(in_ine.check_integrity_incomes('2019'))
    in_ine.load_incomes('2020')
    print(in_ine.check_integrity_incomes('2020'))
    end = time.time()
    print(f'time: {round(end-start, 2)} seg')
    
    # Population INE 2018-2021
    ###print('\nLoad population INE...')
    ###start = time.time()
    ###pop_ine.load_population('2018')
    ###print(pop_ine.check_integrity_population('2018'))
    ###pop_ine.load_population('2019')
    ###print(pop_ine.check_integrity_population('2019'))
    ###pop_ine.load_population('2020')
    ###print(pop_ine.check_integrity_population('2020'))
    ###pop_ine.load_population('2021')
    ###print(pop_ine.check_integrity_population('2021'))
    ###end = time.time()
    ###print(f'time: {round(end-start, 2)} seg')
    

if __name__ == '__main__':
    main()
