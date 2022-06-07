import os
from pathlib import Path
from typing import Union, Iterable

import pandas as pd
from utils.db import DatabaseConnector
from utils.io import import_file

db = DatabaseConnector('zadacha.db')


def filter_country_data(country_df: pd.DataFrame, year: Union[str, Iterable[str]] = '2020') -> pd.DataFrame:
    """
    Get from the country population data the ones that do not
    start from OWID and correspond the requested year
    :param country_df: DataFrame
    The dataframe that needs to be filtered
    :param year: str or List of str
    Years that needs to be returned, 2020 by default
    :return: DataFrame
    Filtered dataframe
    """
    if isinstance(year, str):
        required_fields = ['Country Code', year]
    elif isinstance(year, Iterable):
        required_fields = ['Country Code', *year]
    else:
        raise AttributeError("Wrong year input!")
    return country_df.loc[~country_df.loc[:, 'Country Code'].str.startswith("OWID_"), required_fields]


def get_latest_vaccination(vaccination_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters vaccination DataFrame to use latest date of data collected
    :param vaccination_df: DataFrame
    Data that needs to be filtered
    :return: DataFrame
    Filtered data
    """
    grouped_vaccination = vaccination_df.groupby("iso_code")['date'].max().reset_index()
    grouped_vaccination = grouped_vaccination.rename(columns={'date': 'max_date'})
    vaccination_df = pd.merge(vaccination_df, grouped_vaccination, how='left', on=['iso_code'])
    return vaccination_df.loc[vaccination_df['date'] == vaccination_df['max_date']].reset_index(drop=True)


def process_vaccination(vaccination_df: pd.DataFrame, country_population_df: pd.DataFrame,
                        year: str = '2020') -> pd.DataFrame:
    """
    Function to merge the vaccination data with country population and calculate the
    percentage of vaccination
    :param vaccination_df: DataFrame
    Vaccination data
    :param country_population_df: DataFrame
    Filtered country population data
    :param year: str
    Year of population that needs to be merged
    :return:
    """
    df = pd.merge(vaccination_df, country_population_df, left_on=['iso_code'], right_on=['Country Code'])
    df = df.fillna(0)
    df['percentage_vaccinated'] = 100 * df['people_fully_vaccinated'] / df[year]
    df = df.rename(columns={year: 'population'})
    return df


def update_country_date(country_series: pd.Series) -> None:
    """
    Function to upsert calculated vaccination data (inserts if there is no
    country in database, updates the data otherwise)
    :param country_series: Series
    Processed vaccination data
    :return: None
    """
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
