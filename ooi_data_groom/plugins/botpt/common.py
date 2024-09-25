import logging
import datetime
import pandas as pd
from ion_functions.data.prs_functions import (prs_botsflu_time15s, prs_botsflu_meanpres, prs_botsflu_meandepth,
                                              prs_botsflu_predtide, prs_botsflu_5minrate, prs_botsflu_10minrate,
                                              prs_botsflu_time24h, prs_botsflu_daydepth, prs_botsflu_4wkrate,
                                              prs_botsflu_8wkrate, prs_botsflu_predtide,
                                              prs_botsflu_daydepth_from_15s_meandepth,
                                              prs_botsflu_4wkrate_from_daydepth, prs_botsflu_8wkrate_from_daydepth)
import numpy as np
import ntplib
import scipy.interpolate as interpolate

log = logging.getLogger()


def make_15s(df, predicted_tide_dataframe):
    """
    Accepts a pandas dataframe or xarray dataset containing (at a minimum) time and bottom_pressure
        and a pandas dataframe containing predicted tide
    Returns a pandas dataframe containing all parameters in the botpt_nano_sample_15s stream.
    :param df:
    :return:
    """
    times = df.time.values
    pressures = df.bottom_pressure.values

    time15s = prs_botsflu_time15s(times, pressures)

    botsflu_predtide = get_botsflu_predtide(time15s, predicted_tide_dataframe)
    data = {
        'time': time15s,
        'botsflu_time15s': time15s,
        'botsflu_meanpres': prs_botsflu_meanpres(times, pressures),
        'botsflu_meandepth': prs_botsflu_meandepth(times, pressures, botsflu_predtide),
        'botsflu_5minrate': prs_botsflu_5minrate(times, pressures, botsflu_predtide),
        'botsflu_10minrate': prs_botsflu_10minrate(times, pressures, botsflu_predtide),
        'botsflu_predtide': botsflu_predtide
    }
    return pd.DataFrame(data)


def make_24h(df):
    """
    Accepts a pandas dataframe or xarray dataset containing (at a minimum) botsflu_time15s and botsflu_meandepth
    Returns a pandas dataframe containing all parameters in the botpt_nano_sample_24h stream.
    :param df:
    :return:
    """
    time15s = df.time.values
    meandepth = df.botsflu_meandepth.values

    time24h = prs_botsflu_time24h(time15s)
    botsflu_daydepth = prs_botsflu_daydepth_from_15s_meandepth(time15s, meandepth)
    botsflu_4wkrate = prs_botsflu_4wkrate_from_daydepth(botsflu_daydepth)
    botsflu_8wkrate = prs_botsflu_8wkrate_from_daydepth(botsflu_daydepth)
    data = {
        'time': time24h,
        'botsflu_time24h': time24h,
        'botsflu_daydepth': botsflu_daydepth,
        'botsflu_4wkrate': botsflu_4wkrate,
        'botsflu_8wkrate': botsflu_8wkrate,
    }
    return pd.DataFrame(data)


def get_botsflu_predtide(time15s, predicted_tide_dataframe):
    # Even though the predicted timestamps in the raw data should already
    # be at 15 second intervals, interpolate them to the 15 second timestamp
    # just in case a predicted tide value is missing or duplicated.

    timestamp_predtide = predicted_tide_dataframe.time.values
    predtide = predicted_tide_dataframe.predicted_tide.values

    # Note: we do not use arg fill_value="extrapolate" so the function will throw a
    # value error if time15s is not within the time range of the predicted tide dataframe
    f = interpolate.interp1d(timestamp_predtide, predtide,
                             kind='linear', axis=0, copy=False)
    return f(time15s)


def ntp_to_datetime(ntp_time):
    try:
        ntp_time = float(ntp_time)
        unix_time = ntplib.ntp_to_system_time(ntp_time)
        dt = datetime.datetime.utcfromtimestamp(unix_time)
        return dt
    except (ValueError, TypeError):
        return None


class DataNotFoundException(Exception):
    """Raise this when data could not be found in postgres or cassandra"""
