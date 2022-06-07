from pathlib import Path
import numpy as np
from pandas import pd
from utils.io import import_file
import os
from typing import Union, Iterable


def filter_country_data(country_df, year: Union[str, Iterable[str]] = '2020'):
    if isinstance(year, str):
        required_fields = ['Country Code', year]
    elif isinstance(year, Iterable):
        required_fields = ['Country Code', *year]
    else:
        raise AttributeError("Wrong year input!")
    return country_df.loc[~country_df.loc[:, 'Country Code'].str.startswith("OWID_"), required_fields]


def get_latest_vaccination(vaccination_df):
    grouped_vaccination = vaccination_df.groupby("iso_code")['date'].max().reset_index()
    grouped_vaccination = grouped_vaccination.rename(columns={'date': 'max_date'})
    vaccination_df = pd.merge(vaccination_df, grouped_vaccination, how='left', on=['iso_code'])
    return vaccination_df.loc[vaccination_df['date'] == vaccination_df['max_date']].reset_index(drop=True)


def process_vaccination(vaccination_df, country_population_df):
    df = pd.merge(vaccination_df, country_population_df, left_on=['iso_code'], right_on=['Country Code'])
    df = df.fillna(0)
    df['percentage_vaccinated'] = 100 * df['people_fully_vaccinated'] / df['2020']
    return df


if __name__ == '__main__':
    country_population_path = Path(os.curdir) / 'data' / 'country_populations.csv'
    vaccination_path = Path(os.curdir) / 'data' / 'vaccinations.csv'
    full_data = pd.DataFrame(columns=["iso_code", ""])
    country_population = import_file(country_population_path, concat=True)
    country_population = filter_country_data(country_population)
    vaccination_df = import_file(vaccination_path, concat=True)
    vaccination_df = get_latest_vaccination(vaccination_df)
    processed_vaccination = process_vaccination(vaccination_df, country_population)
