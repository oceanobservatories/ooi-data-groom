import logging

import pandas as pd
from ion_functions.data.prs_functions import (prs_botsflu_time15s, prs_botsflu_meanpres, prs_botsflu_meandepth,
                                              prs_botsflu_predtide, prs_botsflu_5minrate, prs_botsflu_10minrate,
                                              prs_botsflu_time24h, prs_botsflu_daydepth, prs_botsflu_4wkrate,
                                              prs_botsflu_8wkrate)


log = logging.getLogger()


def make_15s(df):
    """
    Accepts a pandas dataframe or xarray dataset containing (at a minimum) time and bottom_pressure
    Returns a pandas dataframe containing all parameters in the botpt_nano_sample_15s stream.
    :param df:
    :return:
    """
    times = df.time.values
    pressures = df.bottom_pressure.values
    time15s = prs_botsflu_time15s(times)
    data = {
        'time': time15s,
        'botsflu_time15s': time15s,
        'botsflu_meanpres': prs_botsflu_meanpres(times, pressures),
        'botsflu_meandepth': prs_botsflu_meandepth(times, pressures),
        'botsflu_5minrate': prs_botsflu_5minrate(times, pressures),
        'botsflu_10minrate': prs_botsflu_10minrate(times, pressures),
        'botsflu_predtide': prs_botsflu_predtide(time15s)
    }
    return pd.DataFrame(data)


def make_24h(df):
    """
    Accepts a pandas dataframe or xarray dataset containing (at a minimum) botsflu_time15s and botsflu_meanpres
    Returns a pandas dataframe containing all parameters in the botpt_nano_sample_24h stream.
    :param df:
    :return:
    """
    times = df.botsflu_time15s.values
    pressures = df.botsflu_meanpres.values
    time24h = prs_botsflu_time24h(times)
    data = {
        'time': time24h,
        'botsflu_time24h': time24h,
        'botsflu_daydepth': prs_botsflu_daydepth(times, pressures),
        'botsflu_4wkrate': prs_botsflu_4wkrate(times, pressures),
        'botsflu_8wkrate': prs_botsflu_8wkrate(times, pressures),
    }
    return pd.DataFrame(data)
