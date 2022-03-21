import pandas as pd
import geopandas as gpd
import re
import os

co_count = [8001,8005,8013,8014,8031,8035,8059,8123]

df = pd.read_csv(r'property_point_all\CO_propertypoint_data.csv')

for county in co_count:
    tdf = df.query('countyfp == @county')
    fp = '0{0}'.format(str(county))
    if not os.path.exists(fp):
        os.mkdir(fp)
    else:
        pass

    tdf.to_csv('{0}/0{1}_propertypoints.csv'.format(fp, str(county)))
