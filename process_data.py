import os
from pathlib import Path
from typing import Union, Iterable

import pandas as pd
from utils.db import DatabaseConnector
from utils.io import import_file

db = DatabaseConnector('zadacha.db')


def filter_country_data(country_df: pd.DataFrame, year: Union[str, Iterable[str]] = '2020') -> pd.DataFrame:
    if isinstance(year, str):
        required_fields = ['Country Code', year]
    elif isinstance(year, Iterable):
        required_fields = ['Country Code', *year]
    else:
        raise AttributeError("Wrong year input!")
    return country_df.loc[~country_df.loc[:, 'Country Code'].str.startswith("OWID_"), required_fields]


def get_latest_vaccination(vaccination_df: pd.DataFrame) -> pd.DataFrame:
    grouped_vaccination = vaccination_df.groupby("iso_code")['date'].max().reset_index()
    grouped_vaccination = grouped_vaccination.rename(columns={'date': 'max_date'})
    vaccination_df = pd.merge(vaccination_df, grouped_vaccination, how='left', on=['iso_code'])
    return vaccination_df.loc[vaccination_df['date'] == vaccination_df['max_date']].reset_index(drop=True)


def process_vaccination(vaccination_df: pd.DataFrame, country_population_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(vaccination_df, country_population_df, left_on=['iso_code'], right_on=['Country Code'])
    df = df.fillna(0)
    df['percentage_vaccinated'] = 100 * df['people_fully_vaccinated'] / df['2020']
    df = df.rename(columns={'2020': 'population'})
    return df


def update_country_date(country_series: pd.Series) -> None:
    country_data = db.query(f"""
        SELECT * FROM countries
        WHERE iso_code = '{country_series["iso_code"]}'
    """)
    if country_data:
        db.query(f"""
                UPDATE countries
                SET population = {country_series["population"]},
                total_vaccinated = {country_series["people_fully_vaccinated"]},
                percentage_vaccinated = {country_series["percentage_vaccinated"]}
                WHERE iso_code = '{country_series["iso_code"]}'
            """, commit=True)
    else:
        db.query(f"""
                INSERT INTO countries
               (name, iso_code, population, total_vaccinated, percentage_vaccinated) VALUES
               ('{country_series["location"]}', 
                '{country_series["iso_code"]}', 
                 {country_series["population"]}, 
                 {country_series["people_fully_vaccinated"]},
                 {country_series["percentage_vaccinated"]})
            """, commit=True)


if __name__ == '__main__':
    country_population_path = Path(os.curdir) / 'data' / 'country_populations.csv'
    vaccination_path = Path(os.curdir) / 'data' / 'vaccinations.csv'
    full_data = pd.DataFrame(columns=["iso_code", ""])
    country_population = import_file(country_population_path, concat=True)
    country_population = filter_country_data(country_population)
    vaccination = import_file(vaccination_path, concat=True)
    vaccination = get_latest_vaccination(vaccination)
    processed_vaccination = process_vaccination(vaccination, country_population)
    for idx, row in processed_vaccination.iterrows():
        update_country_date(row)
