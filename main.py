import pandas as pd
import argparse
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import funcs as F


def get_stations():
    stations = f'stations'
    base_url = 'https://api.weather.gov'

    stations_url = f'{base_url}/{stations}'

    # Get all stations available
    stations_data = F.get_data(stations_url)
    stations_data = stations_data['observationStations']

    stations_data_dict: dict = {}
    stations_data_dict['Id_Station'] = [station.split('/')[-1] for station in stations_data]
    stations_data_dict['Txt_UrlStation'] = [station for station in stations_data]
    df_stations: pd.DataFrame = pd.DataFrame(stations_data_dict)

    # Save tables
    result = F.sql_save('Dim_Stations', 'Id_Station', df_stations)
    if not result:
        print('Nothing new in the horizon on Dim_Stations...')
    else:
        print('New data saving on Dim_Stations...')
    


def get_station_info(station_name: str=None):
    stations = f'stations'
    base_url = 'https://api.weather.gov'

    stations_url = f'{base_url}/{stations}'

    # Get station data
    station_url = f'{stations_url}/{station_name}'
    station_data = F.get_data(station_url)

    station_data_dict: dict = {}
    station_data_dict['Id_Station'] = [station_data['properties']['stationIdentifier']]
    station_data_dict['Txt_NameStation'] = [station_data['properties']['name']]
    station_data_dict['Date_TimeZone'] = [station_data['properties']['timeZone']]
    station_data_dict['Num_Latitude'] = [station_data['geometry']['coordinates'][0]]
    station_data_dict['Num_Longitude'] = [station_data['geometry']['coordinates'][1]]

    df_station: pd.DataFrame = pd.DataFrame(station_data_dict)

    # Save tables
    result = F.sql_save('Dim_StationInfo', 'Id_Station', df_station)
    if not result:
        print('Nothing new in the horizon on Dim_StationInfo...')
    else:
        print('New data saving on Dim_StationInfo...')

# 2024-08-03T00:00:00+04:00

def get_observations(station_name: str=None, delta_days: int=7):
    stations = f'stations'
    observations = f'observations'
    base_url = 'https://api.weather.gov'

    stations_url = f'{base_url}/{stations}'
    station_url = f'{stations_url}/{station_name}'

    # Get the current time
    current_time = datetime.now(tz=ZoneInfo('Chile/Continental'))
    start_date = current_time + timedelta(days= -delta_days)
    start_date = datetime(start_date.year, start_date.month, start_date.day, tzinfo=ZoneInfo('Chile/Continental'))

    current_time = current_time.strftime('%Y-%m-%dT%H:%M:%S%z').replace(':', '%3A')
    start_date = start_date.strftime('%Y-%m-%dT%H:%M:%S%z').replace(':', '%3A')

    query = f'?start={start_date}&end={current_time}'
    observations_url = f'{station_url}/{observations}{query}'

    observations_data = F.get_data(observations_url)['features']

    # observation_data_dict: dict = {}
    observation_data_list = []

    for observation in observations_data:
        temp_data_dict = {}
        temp_data_dict['Id_Observation'] = observation['id']
        temp_data_dict['Num_Latitude'] = observation['geometry']['coordinates'][0]
        temp_data_dict['Num_Longitude'] = observation['geometry']['coordinates'][1]

        # Get into properties, because all items bellow are in this collection
        observation = observation['properties']

        temp_data_dict['Date_Time'] = observation['timestamp']
        
        temperature_unit_code = observation['temperature']['unitCode']
        temperature_value = observation['temperature']['value']
        if temperature_unit_code == 'wmoUnit:degC':
            temp_data_dict['Num_Temperature'] = F.round_na(temperature_value, 2)
        
        # Assume any other unit to fahrenheit (in WMO Code Registry doesn't appear fahrenheit)
        else:
            temperature_celsius = (temperature_value - 32) / 1.8
            temp_data_dict['Num_Temperature'] = F.round_na(temperature_celsius, 2)
        
        temp_data_dict['Num_WindSpeed'] = F.round_na(observation['windSpeed']['value'], 2)
        temp_data_dict['Num_Humidity'] = F.round_na(observation['relativeHumidity']['value'], 2)

        observation_data_list.append(temp_data_dict)

    df_observations: pd.DataFrame = pd.DataFrame(observation_data_list)

    # Save tables
    result = F.sql_save('Fact_Observations', 'Id_Observation', df_observations)
    if not result:
        print('Nothing new in the horizon on Fact_Observations...')
    else:
        print('New data saving on Fact_Observations...')


def get_all_tables_data(station_name: str='0007W'):
    """
    Data to get
    ~ Station Id
    ~ Station name
    ~ Station Timezone
    ~ Latitude/Longitude
    ~ Observation timestamp
    ~ Temperature (round to two decimal places)
    ~ Wind Speed (round to two decimal places)
    ~ Humidity (round to two decimal places)
    """
    get_stations()
    get_station_info(station_name)
    get_observations(station_name)


if __name__=='__main__':
    program_name = 'NationalWater DataBase'
    desc = 'Extract data, random or not, from NWS and save as a DB'
    parser = argparse.ArgumentParser(prog=program_name, description=desc)
    parser_observations = parser.add_mutually_exclusive_group(required=False)

    help_delta = 'Se debe ingresar número de días que se requiere desde fecha actual'
    parser_observations.add_argument('-d', '--delta_days', type=int, default=1, help=help_delta)
    parser_observations.add_argument('-l', '--load_all', dest='load_all', action='store_true')
    # random_parser.add_argument('-r', '--random', dest='random', action='store_true')
    # random_parser.add_argument('-nr', '--no-random', dest='random', action='store_false')
    help_station = 'Se debe ingresar el nombre de la estación a capturar'
    parser.add_argument('-s', '--station_name', type=str, default='0007W', help=help_station)
    # parser.set_defaults(random=True)

    args = parser.parse_args()

    if args.load_all:
        get_all_tables_data(args.station_name)
    else:
        if not isinstance(args.delta_days, int):
            raise ValueError('El parámetro debe ser un número')
        get_observations(args.station_name, args.delta_days)
    # 000PG
