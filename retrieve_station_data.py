"""
Author........... Gabriel Böhnke
University....... UCLouvain, Faculty of bioscience engineering
Email............ gabriel.bohnke@student.uclouvain.be

Description...... retrieve weather station data for a geolocation, using Google Earth Engine API
Version.......... 1.00
Last changed on.. 18.07.2022
"""

import ee
import os
from util.google_earth_engine_util import get_gee_data
import pandas as pd
import numpy as np
from util.performance_util import start_time_measure, end_time_measure
import datetime
from dateutil.relativedelta import relativedelta


def save_single_cli_file(df_cli, file_name):
    weather_station_directory = 'SWAT_INPUT_DATA/WEATHER_STATIONS'
    file_path = weather_station_directory + '/' + file_name
    df_cli.to_csv(file_path, encoding='utf-8', index=False, header=False)
    print(file_path + ' saved')


def save_all_cli_files():
    save_single_cli_file(pd.DataFrame(data=pcp_cli_file_list), 'pcp.cli')
    save_single_cli_file(pd.DataFrame(data=tmp_cli_file_list), 'tmp.cli')
    save_single_cli_file(pd.DataFrame(data=wnd_cli_file_list), 'wnd.cli')
    save_single_cli_file(pd.DataFrame(data=hmd_cli_file_list), 'hmd.cli')
    save_single_cli_file(pd.DataFrame(data=slr_cli_file_list), 'slr.cli')


def update_cli_file_list(file_extension, file_name):
    if file_extension == 'pcp':
        pcp_cli_file_list.append(file_name)
    elif file_extension == 'tmp':
        tmp_cli_file_list.append(file_name)
    elif file_extension == 'wnd':
        wnd_cli_file_list.append(file_name)
    elif file_extension == 'hmd':
        hmd_cli_file_list.append(file_name)
    elif file_extension == 'slr':
        slr_cli_file_list.append(file_name)


def add_header_and_save(df_out, station_name, file_extension):
    if df_out is not None:
        # insert 3rd row
        # station dictionary uses station name as key
        station_details = station_dict[station_name]
        # example of station dictionary value: [1, 'station_001', 36.4759, 9.4573, 114, 2]
        # indexes are as follows:
        # 0: station ID
        # 1: station name
        # 2: lat
        # 3: lon
        # 4: elev
        # 5: rain years
        df_third_row = pd.DataFrame(
            [[station_details[5], 0, station_details[2], station_details[3], station_details[4]]],
            columns=['col1', 'col2', 'col3', 'col4', 'col5'])
        df_out = pd.concat([df_third_row, df_out])

        # insert 2nd row
        df_second_row = pd.DataFrame([['NBYR', 'TSTEP', 'LAT', 'LONG', 'ELEV']],
                                     columns=['col1', 'col2', 'col3', 'col4', 'col5'])
        df_out = pd.concat([df_second_row, df_out])

        target_filename = station_name + '.' + file_extension

        # insert 1st row
        df_first_row = pd.DataFrame([[target_filename]], columns=['col1'])
        df_out = pd.concat([df_first_row, df_out])

        # dataframe to CSV
        weather_station_directory = 'SWAT_INPUT_DATA/WEATHER_STATIONS'
        file_path = weather_station_directory + '/' + target_filename
        # pandas.DataFrame.to_csv
        # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
        df_out.to_csv(file_path, columns=['col1', 'col2', 'col3', 'col4', 'col5'], encoding='utf-8', index=False,
                      header=False, sep=' ')
        print(file_path + ' saved')
        print('\n')

        # update CLI-file list
        update_cli_file_list(file_extension, target_filename)


# def get_daily_precipitation(collection, list_of_bands, station_name, file_extension):
#     # interval_size_in_days = 3000: no memory issues of GEE with this band
#     df_result = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 3000, scale, file_extension)
#
#     if df_result is not None:
#         # change unit
#         df_result[[*list_of_bands]] = df_result[[*list_of_bands]] * 10 ** 3  # m to mm
#         df_result[[*list_of_bands]] = df_result[[*list_of_bands]].round(decimals=0)  # no decimals!
#
#         print(target_filename)
#         print(df_result.head())
#
#         # keep columns with bands
#         df_out = df_result[[*list_of_bands]]
#
#         # add FROM-date to single column of first row
#         add_header_and_save(df_out, [from_date_string], list_of_bands, target_directory, target_filename)
#
#         return df_result


def get_daily_precipitation_imerg(collection, list_of_bands, station_name, file_extension):
    # interval_size_in_days = 180: data retrieval by chunks, to bypass memory issues of GEE
    df_half_hourly = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 180, scale,
                                  file_extension)
    df_daily = df_half_hourly.copy(deep=True)
    df_daily.rename(columns={'precipitationCal': 'total_precipitation'}, inplace=True)

    if df_half_hourly is not None:
        # change unit
        df_half_hourly[['precipitationCal']] = df_half_hourly[
                                                   ['precipitationCal']] / 2  # mm/hr to mm/half-hour
        df_daily[['total_precipitation']] = df_daily[
                                                ['total_precipitation']] / 2  # mm/hr to mm/half-hour

        # one measure every half-hour: calculate daily sum
        df_daily['date'] = pd.to_datetime(df_daily['datetime']).dt.date
        df_daily = df_daily.groupby(['date'], as_index=False).sum()
        df_daily[['total_precipitation']] = df_daily[['total_precipitation']].round(decimals=0)  # no decimals!

        print(station_name + '.' + file_extension)
        print(df_daily.head())

        # add columns for csv output
        df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year

        # how to reset a counter every new day using pandas and numpy?
        # https://stackoverflow.com/questions/59486551/how-to-reset-a-counter-every-new-day-using-pandas-and-numpy
        df_daily['step'] = 1
        # increment step for all days of year, and reset to 1 at change of year
        df_daily['step'] = df_daily[['step', 'year']].groupby('year').transform(lambda x: x.cumsum())

        # deep copy, to prevent SettingWithCopyWarning during column renaming
        df_out = df_daily[['year', 'step', 'total_precipitation']].copy(deep=True)

        # rename columns, because save method uses generic column names
        df_out.rename(columns={'year': 'col1', 'step': 'col2', 'total_precipitation': 'col3'}, inplace=True)
        df_out['col4'] = ''
        df_out['col5'] = ''

        # save weather file
        add_header_and_save(df_out, station_name, file_extension)

        return df_half_hourly, df_daily


def get_daily_temperature(collection, list_of_bands, station_name, file_extension):
    # interval_size_in_days = 3000: no memory issues of GEE with this band
    df_result = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 3000, scale,
                             file_extension)

    if df_result is not None:
        # change unit
        df_result[[*list_of_bands]] = df_result[[*list_of_bands]] - 273.15  # Kelvin to Celsius

        print(station_name + '.' + file_extension)
        print(df_result.head())

        # add columns for csv output
        df_result['year'] = pd.to_datetime(df_result['datetime']).dt.year

        df_result['step'] = 1
        # increment step for all days of year, and reset to 1 at change of year
        df_result['step'] = df_result[['step', 'year']].groupby('year').transform(lambda x: x.cumsum())

        # deep copy, to prevent SettingWithCopyWarning during column renaming
        df_out = df_result[['year', 'step', 'maximum_2m_air_temperature', 'minimum_2m_air_temperature']].copy(deep=True)

        # rename columns, because save method uses generic column names
        df_out.rename(columns={'year': 'col1', 'step': 'col2', 'maximum_2m_air_temperature': 'col3',
                               'minimum_2m_air_temperature': 'col4'}, inplace=True)
        df_out['col5'] = ''

        # save weather file
        add_header_and_save(df_out, station_name, file_extension)

        return df_result


def get_daily_wind_speed(collection, list_of_bands, station_name, file_extension):
    # interval_size_in_days = 3000: no memory issues of GEE with this band
    df_result = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 3000, scale,
                             file_extension)

    if df_result is not None:
        # derive wind speed from U and V component: vectorized solution
        df_result['wind_speed'] = df_result['u_component_of_wind_10m'] ** 2 + df_result['v_component_of_wind_10m'] ** 2
        df_result['wind_speed'] = df_result['wind_speed'] ** (1 / 2)

        print(station_name + '.' + file_extension)
        print(df_result.head())

        # add columns for csv output
        df_result['year'] = pd.to_datetime(df_result['datetime']).dt.year

        df_result['step'] = 1
        # increment step for all days of year, and reset to 1 at change of year
        df_result['step'] = df_result[['step', 'year']].groupby('year').transform(lambda x: x.cumsum())

        # deep copy, to prevent SettingWithCopyWarning during column renaming
        df_out = df_result[['year', 'step', 'wind_speed']].copy(deep=True)

        # rename columns, because save method uses generic column names
        df_out.rename(columns={'year': 'col1', 'step': 'col2', 'wind_speed': 'col3'}, inplace=True)
        df_out['col4'] = ''
        df_out['col5'] = ''

        # save weather file
        add_header_and_save(df_out, station_name, file_extension)

        return df_result


def get_daily_relative_humidity(collection, list_of_bands, station_name, file_extension):
    # interval_size_in_days = 30: data retrieval by chunks, to bypass memory issues of GEE
    df_result = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 30, scale,
                             file_extension)

    if df_result is not None:
        # several measures per day: calculate daily mean
        df_result['date'] = pd.to_datetime(df_result['datetime']).dt.date
        df_result = df_result.groupby(['date'], as_index=False).mean()

        # change unit
        df_result[[*list_of_bands]] = df_result[[*list_of_bands]] / 100

        print(station_name + '.' + file_extension + ' - daily mean')
        print(df_result.head())

        # add columns for csv output
        df_result['year'] = pd.to_datetime(df_result['date']).dt.year

        df_result['step'] = 1
        # increment step for all days of year, and reset to 1 at change of year
        df_result['step'] = df_result[['step', 'year']].groupby('year').transform(lambda x: x.cumsum())

        # deep copy, to prevent SettingWithCopyWarning during column renaming
        df_out = df_result[['year', 'step', 'relative_humidity_2m_above_ground']].copy(deep=True)

        # rename columns, because save method uses generic column names
        df_out.rename(columns={'year': 'col1', 'step': 'col2', 'relative_humidity_2m_above_ground': 'col3'},
                      inplace=True)
        df_out['col4'] = ''
        df_out['col5'] = ''

        # save weather file
        add_header_and_save(df_out, station_name, file_extension)

        return df_result


def get_daily_solar_radiation(collection, list_of_bands, station_name, file_extension):
    # interval_size_in_days = 180: data retrieval by chunks, to bypass memory issues of GEE
    df_result = get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, 180, scale,
                             file_extension)

    if df_result is not None:
        # several measures per day: calculate daily mean
        df_result['date'] = pd.to_datetime(df_result['datetime']).dt.date
        df_result = df_result.groupby(['date'], as_index=False).mean()

        # change unit
        df_result[[*list_of_bands]] = df_result[[*list_of_bands]] / 10 ** 6  # divide by 10**6

        print(station_name + '.' + file_extension + ' - daily mean')
        print(df_result.head())

        # add columns for csv output
        df_result['year'] = pd.to_datetime(df_result['date']).dt.year

        df_result['step'] = 1
        # increment step for all days of year, and reset to 1 at change of year
        df_result['step'] = df_result[['step', 'year']].groupby('year').transform(lambda x: x.cumsum())

        # deep copy, to prevent SettingWithCopyWarning during column renaming
        df_out = df_result[['year', 'step', 'surface_net_solar_radiation']].copy(deep=True)

        # rename columns, because save method uses generic column names
        df_out.rename(columns={'year': 'col1', 'step': 'col2', 'surface_net_solar_radiation': 'col3'},
                      inplace=True)
        df_out['col4'] = ''
        df_out['col5'] = ''

        # save weather file
        add_header_and_save(df_out, station_name, file_extension)

        return df_result


def get_generator_columns(wgn_id, df_half_hourly_precipitation, df_daily_precipitation, df_daily_temperature,
                          df_daily_wind_speed,
                          df_daily_solar_radiation):
    # https://stackoverflow.com/questions/13784192/creating-an-empty-pandas-dataframe-then-filling-it
    df_generator_data = pd.DataFrame()
    df_generator_data['id'] = range((wgn_id - 1) * 12 + 1, (wgn_id - 1) * 12 + 13)  # range increases by periods of 12
    df_generator_data['wgn_id'] = wgn_id

    if df_daily_temperature is not None:
        # temperature 'month' column
        df_daily_temperature['month'] = pd.to_datetime(df_daily_temperature['datetime']).dt.month

        # temperature monthly mean
        df_monthly_tmp_mean = df_daily_temperature.groupby(['month'], as_index=False).mean()
        # month
        df_generator_data['month'] = df_monthly_tmp_mean['month']
        # tmp_max_ave
        df_generator_data['tmp_max_ave'] = df_monthly_tmp_mean['maximum_2m_air_temperature']
        # tmp_min_ave
        df_generator_data['tmp_min_ave'] = df_monthly_tmp_mean['minimum_2m_air_temperature']

        # temperature monthly standard deviation
        df_monthly_tmp_std = df_daily_temperature.groupby(['month'], as_index=False).std()
        # tmp_max_sd
        df_generator_data['tmp_max_sd'] = df_monthly_tmp_std['maximum_2m_air_temperature']
        # tmp_min_sd
        df_generator_data['tmp_min_sd'] = df_monthly_tmp_std['minimum_2m_air_temperature']

    if df_daily_precipitation is not None:

        if is_precipitation_data_source_imerg:

            # precipitation 'year' column
            df_daily_precipitation['year'] = pd.to_datetime(
                df_daily_precipitation['date']).dt.year  # source column is ['date']

            # precipitation 'month' column
            df_daily_precipitation['month'] = pd.to_datetime(
                df_daily_precipitation['date']).dt.month  # source column is ['date']

        else:

            # precipitation 'year' column
            df_daily_precipitation['year'] = pd.to_datetime(
                df_daily_precipitation['datetime']).dt.year  # source column is ['datetime']

            # precipitation 'month' column
            df_daily_precipitation['month'] = pd.to_datetime(
                df_daily_precipitation['datetime']).dt.month  # source column is ['datetime']

        # rolling comparison with a moving window size of 2 observations
        wet_following_dry = lambda x: (x[-1] == 0 and x[0] > 0)  # current = x[-1], previous = x[0]
        # compare = lambda x: (
        #             x.values[-1] > 0 and x.values[0] > 0)  # call lambda with raw=False: parameters passed as series
        wet_following_wet = lambda x: (
                x[-1] > 0 and x[0] > 0)  # call lambda with raw=True: parameters passed as ndarray objects
        df_daily_precipitation['wet_dry'] = df_daily_precipitation['total_precipitation'].rolling(2).apply(
            wet_following_dry, raw=True).replace({np.nan: 0.0})
        df_daily_precipitation['wet_wet'] = df_daily_precipitation['total_precipitation'].rolling(2).apply(
            wet_following_wet, raw=True).replace({np.nan: 0.0})

        # sum of precipitation-sequences by period (year + month)
        df_period_pcp_seq_sum = df_daily_precipitation.groupby(['year', 'month'], as_index=False).sum()

        # precipitation monthly average
        df_monthly_pcp_mean = df_period_pcp_seq_sum.groupby(['month'], as_index=False).mean()
        # pcp_ave
        df_generator_data['pcp_ave'] = df_monthly_pcp_mean['total_precipitation']

        # precipitation monthly standard deviation
        df_monthly_pcp_std = df_period_pcp_seq_sum.groupby(['month'], as_index=False).std()
        # pcp_sd
        df_generator_data['pcp_sd'] = df_monthly_pcp_std['total_precipitation']

        # precipitation monthly skew
        df_monthly_pcp_skew = df_period_pcp_seq_sum.groupby(['month'], as_index=False).skew()
        # pcp_skew
        df_generator_data['pcp_skew'] = df_monthly_pcp_skew['total_precipitation']

        # sum of precipitation-sequences by period (year + month)
        df_daily_precipitation['row_counter'] = 1
        df_monthly_pcp_seq_sum = df_daily_precipitation.groupby(['month'], as_index=False).sum()

        # wet_dry
        df_generator_data['wet_dry'] = df_monthly_pcp_seq_sum['wet_dry'] / df_monthly_pcp_seq_sum['row_counter']

        # wet_wet
        df_generator_data['wet_wet'] = df_monthly_pcp_seq_sum['wet_wet'] / df_monthly_pcp_seq_sum['row_counter']

        # pcp_days
        df_period_pcp_nonzero = df_daily_precipitation.groupby(['year', 'month'], as_index=False)[
            'total_precipitation'].agg(np.count_nonzero)
        df_monthly_pcp_nonzero = df_period_pcp_nonzero.groupby(['month'], as_index=False).mean()
        df_generator_data['pcp_days'] = df_monthly_pcp_nonzero['total_precipitation']

        if is_precipitation_data_source_imerg:

            # half-hourly precipitation 'year' column
            df_half_hourly_precipitation['year'] = pd.to_datetime(
                df_half_hourly_precipitation['datetime']).dt.year  # source column is ['datetime']

            # half-hourly precipitation 'month' column
            df_half_hourly_precipitation['month'] = pd.to_datetime(
                df_half_hourly_precipitation['datetime']).dt.month  # source column is ['datetime']

            # pcp_hhr: data granularity is already half-hour
            df_period_pcp_max = df_half_hourly_precipitation.groupby(['year', 'month'], as_index=False)[
                'precipitationCal'].max()
            df_monthly_pcp_max = df_period_pcp_max.groupby(['month'], as_index=False).mean()
            df_generator_data['pcp_hhr'] = df_monthly_pcp_max['precipitationCal']

        else:

            # pcp_hhr: assumption that half-hour of interest has received 1/2 of pcp of day with max rainfall
            df_period_pcp_max = df_daily_precipitation.groupby(['year', 'month'], as_index=False)[
                                    'total_precipitation'].max() / 2  # <-- assumption: 1 hour of rain on that max-day
            df_monthly_pcp_max = df_period_pcp_max.groupby(['month'], as_index=False).mean()
            df_generator_data['pcp_hhr'] = df_monthly_pcp_max['total_precipitation']

    if df_daily_solar_radiation is not None:
        # solar radiation 'month' column
        df_daily_solar_radiation['month'] = pd.to_datetime(df_daily_solar_radiation['date']).dt.month
        # solar radiation monthly mean
        df_monthly_slr_mean = df_daily_solar_radiation.groupby(['month'], as_index=False).mean()
        # slr_ave
        df_generator_data['slr_ave'] = df_monthly_slr_mean['surface_net_solar_radiation']

    # get dewpoint daily values (not fetched yet)
    df_daily_dewpoint = get_gee_data(lon, lat, 'ECMWF/ERA5/DAILY', ['dewpoint_2m_temperature'], from_date_string,
                                     to_date_string, 3000, scale, 'dew')
    if df_daily_dewpoint is not None:
        df_daily_dewpoint[['dewpoint_2m_temperature']] = df_daily_dewpoint[
                                                             ['dewpoint_2m_temperature']] - 273.15  # Kelvin to Celsius
        # dewpoint 'month' column
        df_daily_dewpoint['month'] = pd.to_datetime(df_daily_dewpoint['datetime']).dt.month
        # dewpoint monthly mean
        df_monthly_dewpoint_mean = df_daily_dewpoint.groupby(['month'], as_index=False).mean()
        # dew_ave
        df_generator_data['dew_ave'] = df_monthly_dewpoint_mean['dewpoint_2m_temperature']

    if df_daily_wind_speed is not None:
        # wind speed 'month' column
        df_daily_wind_speed['month'] = pd.to_datetime(df_daily_wind_speed['datetime']).dt.month
        # wind speed monthly mean
        df_monthly_wind_speed_mean = df_daily_wind_speed.groupby(['month'], as_index=False).mean()
        # wnd_ave
        df_generator_data['wnd_ave'] = df_monthly_wind_speed_mean['wind_speed']

    return df_generator_data


def process_single_weather_station(wgn_id):
    df_half_hourly_precipitation = None

    # weather station name
    weather_station_name = 'station_' + str(wgn_id).zfill(3)  # 7 -> station_007

    weather_station_total_time = start_time_measure(
        ">>> " + weather_station_name + " - starting data retrieval...")
    print("\n")

    # if is_precipitation_data_source_imerg:

    # half-hourly / daily: precipitation IMERG
    df_half_hourly_precipitation, df_daily_precipitation = get_daily_precipitation_imerg('NASA/GPM_L3/IMERG_V06',
                                                                                         ['precipitationCal'],
                                                                                         weather_station_name,
                                                                                         'pcp')

    # else:
    #     # daily: precipitation ERA5
    #     df_daily_precipitation = get_daily_precipitation('ECMWF/ERA5/DAILY', ['total_precipitation'],
    #                                                      weather_station_name,
    #                                                      'pcp')

    # daily: temperature
    df_daily_temperature = get_daily_temperature('ECMWF/ERA5/DAILY',
                                                 ['maximum_2m_air_temperature', 'minimum_2m_air_temperature'],
                                                 weather_station_name,
                                                 'tmp')
    #
    # daily: wind speed
    df_daily_wind_speed = get_daily_wind_speed('ECMWF/ERA5/DAILY',
                                               ['u_component_of_wind_10m', 'v_component_of_wind_10m'],
                                               weather_station_name,
                                               'wnd')

    # daily: relative humidity
    df_daily_relative_humidity = get_daily_relative_humidity('NOAA/GFS0P25', ['relative_humidity_2m_above_ground'],
                                                             weather_station_name,
                                                             'hmd')

    # daily: solar radiation
    df_daily_solar_radiation = get_daily_solar_radiation('ECMWF/ERA5_LAND/HOURLY', ['surface_net_solar_radiation'],
                                                         weather_station_name,
                                                         'slr')

    # get generator data
    df_generator_data = get_generator_columns(wgn_id, df_half_hourly_precipitation, df_daily_precipitation,
                                              df_daily_temperature, df_daily_wind_speed,
                                              df_daily_solar_radiation)
    print('\n')
    end_time_measure(weather_station_total_time, ">>> " + weather_station_name + " - data retrieval time: ")

    # check for existence of directory SWAT_INPUT_DATA/OPTIONAL_XLSX_FILES
    optional_directory = 'SWAT_INPUT_DATA/OPTIONAL_XLSX_FILES'
    if not os.path.exists(optional_directory):
        os.makedirs(optional_directory)

    # dataframe to Excel
    file_path = optional_directory + '/' + 'WGEN_' + weather_station_name + '_mon.xlsx'
    df_generator_data.to_excel(file_path, encoding='utf-8', index=False, header=True)
    print('\n')
    print(file_path + ' saved')
    print('\n')
    print(
        '==============================================================================================================================================')
    print('\n')

    return df_generator_data


def create_station_file(weather_stations):
    delta = relativedelta(datetime.datetime.strptime(to_date_string, '%Y-%m-%d').date(),
                          datetime.datetime.strptime(from_date_string, '%Y-%m-%d').date())
    # example of delta: relativedelta(years=+5, months=+6, days=+9)

    # How to convert 'false' to 0 and 'true' to 1?
    # https://stackoverflow.com/questions/20840803/how-to-convert-false-to-0-and-true-to-1
    delta_years_rounded_up = delta.years + int(delta.months > 0 or delta.days > 0)
    print('delta years (rounded-up):', delta_years_rounded_up)
    print('\n')

    columns = ['lon', 'lat', 'elev']
    rows = range(1, len(weather_stations) + 1)
    df_stations = pd.DataFrame(data=weather_stations, index=rows, columns=columns)

    df_stations['id'] = df_stations.index

    df_stations.insert(0, 'name', range(1, len(weather_stations) + 1))

    # Add Leading Zeros to Strings in Pandas Dataframe
    # https://stackoverflow.com/questions/23836277/add-leading-zeros-to-strings-in-pandas-dataframe
    # df_stations['name'] = df_stations['name'].astype(str).str.zfill(3)  # 7 -> 007
    df_stations['name'] = df_stations['name'].apply(lambda x: 'station_' + str(x).zfill(3))  # 7 -> station_007

    df_stations['rain_yrs'] = delta_years_rounded_up

    # move column in pandas dataframe
    # https://stackoverflow.com/questions/35321812/move-column-in-pandas-dataframe
    df_stations = df_stations[['id', 'name', 'lat', 'lon', 'elev', 'rain_yrs']]

    print(df_stations.head(len(weather_stations)))
    print('\n')

    if df_stations is not None:
        # dataframe to CSV
        file_path = 'SWAT_INPUT_DATA' + '/' + 'WGEN_Siliana_stat.csv'
        # pandas.DataFrame.to_csv
        # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
        df_stations.to_csv(file_path, encoding='utf-8', index=False, header=True)
        # # dataframe to Excel
        # file_path = 'SWAT_INPUT_DATA' + '/' + 'WGEN_Siliana_stat.xlsx'
        # df_stations.to_excel(file_path, encoding='utf-8', index=False, header=True)
        print(file_path + ' saved')

    # keep station details available as dictionary
    for station_details in df_stations.values.tolist():
        # example of station_details: [1, 'station_001', 36.4759, 9.4573, 114, 2]
        # station name is used as dictionary key; dictionary value contains all fields
        station_dict[station_details[1]] = station_details


def main(weather_stations):
    # # authenticate on GEE, using web page + paste of token
    # ee.Authenticate(auth_mode='paste')
    # authenticate on GEE, using gcloud
    ee.Authenticate()

    # set global scope for a list of chosen variables
    global lon, lat, from_date_string, to_date_string, is_precipitation_data_source_imerg, scale, station_dict, \
        pcp_cli_file_list, tmp_cli_file_list, wnd_cli_file_list, hmd_cli_file_list, slr_cli_file_list

    # check for existence of directory SWAT_INPUT_DATA
    swat_input_data_directory = 'SWAT_INPUT_DATA'
    if not os.path.exists(swat_input_data_directory):
        os.makedirs(swat_input_data_directory)

    # check for existence of directory SWAT_INPUT_DATA/WEATHER_STATIONS
    weather_station_directory = 'SWAT_INPUT_DATA/WEATHER_STATIONS'
    if not os.path.exists(weather_station_directory):
        os.makedirs(weather_station_directory)

    # check for existence of directory SWAT_INPUT_DATA/GEE_RAW_DATA
    gee_raw_data_directory = 'GEE_RAW_DATA'
    if not os.path.exists(gee_raw_data_directory):
        os.makedirs(gee_raw_data_directory)

    # 1) create station csv file: WGEN_Siliana_stat.csv
    create_station_file(weather_stations)

    # How To Stop Python Script From Execution
    # https://appdividend.com/2022/07/14/how-to-stop-python-script-from-execution/
    # exit()

    # 2) create monthly values csv file: WGEN_Siliana_mon.csv
    df_aggregated_generator = None

    # process all weather stations
    for index, weather_station in enumerate(weather_stations):
        lon = weather_station[0]
        lat = weather_station[1]
        df_delta_generator = process_single_weather_station(
            wgn_id=index + 1)  # index starts at 0, weather station ID starts at 1

        if df_delta_generator is not None:
            if df_aggregated_generator is not None:
                # axis=0: concatenate along rows
                # ignore_index=True: a continuous index value is maintained across the rows in the concatenated data frame
                df_aggregated_generator = pd.concat([df_aggregated_generator, df_delta_generator], axis=0,
                                                    ignore_index=True)
            else:
                df_aggregated_generator = df_delta_generator

    if df_aggregated_generator is not None:
        # dataframe to CSV
        file_path = 'SWAT_INPUT_DATA' + '/' + 'WGEN_Siliana_mon.csv'
        df_aggregated_generator.to_csv(file_path, encoding='utf-8', index=False, header=True)
        # # dataframe to Excel
        # file_path = 'SWAT_INPUT_DATA' + '/' + 'WGEN_Siliana_mon.xlsx'
        # df_aggregated_generator.to_excel(file_path, encoding='utf-8', index=False, header=True)
        print(file_path + ' saved')
        print('\n')

    # 3) save all CLI-files
    save_all_cli_files()


if __name__ == '__main__':
    lon = 0.0
    lat = 0.0

    from_date_string = '2015-01-01'  # adapt value
    to_date_string = '2020-07-10'  # adapt value

    # get elevation online for lon, lat
    # https://www.randymajors.org/elevation-on-google-maps?x=9.5130000&y=36.4450000&cx=9.5130000&cy=36.4450000&zoom=7&counties=show

    weather_station_list = [
        [9.4573, 36.4759, 114],
        [9.5405, 36.5038, 249],
        [9.3626, 36.3874, 422],
        [9.4366, 36.4124, 201],
        [9.5367, 36.4134, 311],
        [9.6222, 36.3970, 265],
        [9.2549, 36.2903, 363],
        [9.3405, 36.3143, 262],
        [9.4386, 36.3153, 341],
        [9.5357, 36.3124, 364],
        [9.5944, 36.3441, 274],
        [9.2433, 36.2153, 452],
        [9.3414, 36.2172, 340],
        [9.4376, 36.2153, 457],
        [9.5357, 36.2153, 522],
        [9.6030, 36.2057, 549],
        [9.2635, 36.1172, 552],
        [9.3376, 36.1143, 451],
        [9.4376, 36.1143, 533],
        [9.4991, 36.0787, 655],
        [9.2453, 36.0123, 717],
        [9.3385, 36.0162, 668],
        [9.4357, 36.0162, 465],
        [9.5241, 36.0172, 642],
        [9.5953, 36.0229, 717],
        [9.1635, 35.8816, 957],
        [9.2424, 35.9133, 756],
        [9.3395, 35.9200, 663],
        [9.4395, 35.9248, 842],
        [9.5241, 35.9316, 974],
        [9.5838, 35.9681, 788],
        [9.1404, 35.8200, 996],
        [9.2424, 35.8210, 862],
        [9.3347, 35.8566, 843],
        [9.3924, 35.8575, 987]
    ]

    station_dict = {}

    # first 2 rows of all CLI-files
    # <ext>.cli
    # FILENAME
    pcp_cli_file_list = ['pcp.cli', 'FILENAME']
    tmp_cli_file_list = ['tmp.cli', 'FILENAME']
    wnd_cli_file_list = ['wnd.cli', 'FILENAME']
    hmd_cli_file_list = ['hmd.cli', 'FILENAME']
    slr_cli_file_list = ['slr.cli', 'FILENAME']

    # precipitation data source: IMERG vs ERA5
    is_precipitation_data_source_imerg = True

    # scale in meters
    scale = 30  # can keep this value

    main(weather_station_list)
