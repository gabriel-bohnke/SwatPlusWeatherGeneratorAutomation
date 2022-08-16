"""
Author........... Gabriel Böhnke
University....... UCLouvain, Faculty of bioscience engineering
Email............ gabriel.bohnke@student.uclouvain.be

Description...... Google Earth Engine util functions
Version.......... 1.00
Last changed on.. 18.07.2022
"""

import os
import ee  # requires package earthengine-api
import pandas as pd
import datetime
from util.performance_util import start_time_measure, end_time_measure


def get_raw_data_file_path(lon, lat, date_from, date_to, category):
    # lon/lat part: iiiddddd_iiiddddd
    # example: 00939000_03616579 for lon=9.39 and lat=36.165789

    # https://stackoverflow.com/questions/455612/limiting-floats-to-two-decimal-points
    # https://stackoverflow.com/questions/34688196/how-to-add-trailing-zeroes-to-an-integer
    # integer part: if fewer than 3 integer positions, add leading zeroes
    # decimal part: if more than 5 decimals: rounding on 5th one; if fewer, add trailing zeroes
    lon_lat_part = str(lon).split('.')[0].zfill(3) + '{:<05}'.format(
        str(float("{:.5f}".format(lon))).split('.')[1]) + '_' + str(lat).split('.')[0].zfill(3) + '{:<05}'.format(
        str(float("{:.5f}".format(lat))).split('.')[1])

    # example file name: 00939000_03616579_2015-01-01_2016-03-01_tmp.csv
    file_name = lon_lat_part + '_' + date_from + '_' + date_to + '_' + category + '.csv'

    gee_raw_data_directory = 'GEE_RAW_DATA'
    file_path = gee_raw_data_directory + '/' + file_name

    return file_path


# inspired by: https://developers.google.com/earth-engine/tutorials/community/intro-to-python-api
def ee_array_to_df(arr, list_of_bands):
    """Transforms client-side ee.Image.getRegion array to pandas.DataFrame."""
    df = pd.DataFrame(arr)

    # rearrange header.
    headers = df.iloc[0]
    df = pd.DataFrame(df.values[1:], columns=headers)

    # remove rows without data inside.
    df = df[['longitude', 'latitude', 'time', *list_of_bands]].dropna()

    # convert data to numeric values.
    for band in list_of_bands:
        df[band] = pd.to_numeric(df[band], errors='coerce')

    # convert time field into a datetime.
    df['datetime'] = pd.to_datetime(df['time'], unit='ms')

    # keep columns of interest.
    df = df[['datetime', *list_of_bands]]

    return df


def call_cloud_service(point_of_interest, collection, list_of_bands, date_from, date_to, scale):
    # selection of appropriate bands and dates
    selection = collection.select(list_of_bands).filterDate(date_from.strftime('%Y-%m-%d'),
                                                            date_to.strftime('%Y-%m-%d'))

    try:
        # get data for the pixel intersecting point of interest
        data = selection.getRegion(point_of_interest, scale).getInfo()
        df = ee_array_to_df(data, list_of_bands)
    except ee.ee_exception.EEException:
        df = None

    return df


# Parameter 'interval_size_in_days' cuts retrieval into chunks, to bypass memory issues of GEE.
# For band 'relative_humidity_2m_above_ground', set 90. For band 'surface_net_solar_radiation', set 180.
# Otherwise set a high number, e.g. 3000
def get_gee_data(lon, lat, collection, list_of_bands, from_date_string, to_date_string, interval_size_in_days, scale,
                 category):
    file_path = get_raw_data_file_path(lon, lat, from_date_string, to_date_string, category)

    if not os.path.exists(file_path):

        ee.Initialize()

        cloud_retrieval_time = start_time_measure(">>> " + " ".join(list_of_bands) + " - starting cloud retrieval...")

        point_of_interest = ee.Geometry.Point(lon, lat)
        image_collection = ee.ImageCollection(collection)

        # FROM-date (included)
        from_date = datetime.datetime.strptime(from_date_string, '%Y-%m-%d').date()

        # TO-date (excluded)
        to_date = datetime.datetime.strptime(to_date_string, '%Y-%m-%d').date()

        # initialize variables for WHILE-loop
        lower_date_boundary = from_date
        upper_date_boundary = from_date + datetime.timedelta(days=interval_size_in_days)

        df_result = None

        while upper_date_boundary < to_date:

            df_delta = call_cloud_service(point_of_interest, image_collection, list_of_bands, lower_date_boundary,
                                          upper_date_boundary, scale)
            if df_delta is not None:
                delta_size = len(df_delta)
                if df_result is not None:
                    # axis=0: concatenate along rows
                    # ignore_index=True: a continuous index value is maintained across the rows in the concatenated data frame
                    df_result = pd.concat([df_result, df_delta], axis=0, ignore_index=True)
                else:
                    df_result = df_delta
            else:
                delta_size = 0

            print("period from", lower_date_boundary, "to", upper_date_boundary, "records found:", delta_size)
            lower_date_boundary = upper_date_boundary
            upper_date_boundary += datetime.timedelta(days=interval_size_in_days)

        # any remaining interval chunk to be processed?
        if lower_date_boundary < to_date:

            df_delta = call_cloud_service(point_of_interest, image_collection, list_of_bands, lower_date_boundary, to_date,
                                          scale)
            if df_delta is not None:
                delta_size = len(df_delta)
                if df_result is not None:
                    df_result = pd.concat([df_result, df_delta], axis=0, ignore_index=True)
                else:
                    df_result = df_delta
            else:
                delta_size = 0
            print("period from", lower_date_boundary, "to", to_date, "records found:", delta_size)

        end_time_measure(cloud_retrieval_time, ">>> " + " ".join(list_of_bands) + " - retrieval time: ")

        # save raw data in csv format
        df_result.to_csv(file_path, encoding='utf-8', index=False, header=True)

    else:

        print(">>> " + " ".join(list_of_bands) + " - retrieving data from " + file_path)
        df_result = pd.read_csv(file_path)

    if df_result is not None:
        result_size = len(df_result)
    else:
        result_size = 0

    print('\n')
    print("total records found:", result_size)

    return df_result
