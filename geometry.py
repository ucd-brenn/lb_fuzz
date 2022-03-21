import pandas as pd
import geopandas as gpd
import numpy as np

epsg = 32613

def gdf_reader(idx):
    num, name, year, par = co_count[idx]

    filepath = "0{0}/{1}_County{2}".format(num, name, year)
    gdf = gpd.read_file(filepath)
    gdf.to_crs(epsg = epsg, inplace = True)

    gdf.rename(columns = {par : 'parcel_id'}, inplace = True)

    return gdf

def id_maker(tdf):

    num, name, year, par = co_count[i]

    id_df = tdf['geometry'].to_frame().drop_duplicates().reset_index(drop=True).reset_index().copy()
    id_df.rename(columns = {'index' : 'geom_id'}, inplace = True)
    id_df.geom_id = id_df.geom_id.apply(lambda x: "{0}-{1}".format(name[:2].upper(), x))
    geom_df = tdf.merge(id_df, on = 'geometry', how = 'left')
    if len(geom_df) != len(tdf):
        print('Something got hecked')

    df_list.append(geom_df)
    unique_list.append(id_df)

co_count = [(8001, 'Adams',     2020, 'PARCELNB'),
            (8005, 'Arapahoe',  2020, 'PARCEL_ID'),
            (8013, 'Boulder',   2020, 'PARCEL_NO'),
            (8014, 'Broomfield',2020, 'PARCELNUMB'),
            (8031, 'Denver',    2020, 'SCHEDNUM'),
            (8035, 'Douglas',   2020, 'PARCELS__1'),
            (8059, 'Jefferson', 2020, 'PIN'),
            (8123, 'Weld',      2020, 'PARCEL')]

add_cols = ['situs', 'st_num', 'st_dir', 'st_nam', 'st_suf', 'st_post', 'st_unit', 'situs_city']

df_list  = []
unique_list = []

## Adams
i = 0
gdf = gdf_reader(i)

g_cols = ['STREETNO', 'STREETDIR', 'STREETNAME', 'STREETSUF', 'STREETPOST', 'STREETALP']
gdf[g_cols] = gdf[g_cols].fillna('')
gdf['situs'] = gdf.apply(lambda r: ' '.join([c for c in [r.STREETNO, r.STREETDIR, r.STREETNAME, r.STREETSUF, r.STREETPOST, r.STREETALP] if c != None]), axis = 1)
gdf['situs_city'] = ''

ngdf = gdf[['parcel_id', 'situs'] + g_cols + ['situs_city', 'geometry']].copy()
ngdf.columns = ['parcel_id'] + add_cols + ['geometry']

id_maker(ngdf)

## Arapahoe
i = 1
gdf = gdf_reader(i)
gdf.rename(columns = {'Situs_Addr' : 'situs', 'Situs_City' : 'situs_city'}, inplace = True)
gdf[['st_num', 'st_dir', 'st_nam', 'st_suf', 'st_post', 'st_unit']] = ''
gdf = gdf.dissolve('parcel_id').reset_index()
ngdf = gdf[['parcel_id'] + add_cols + ['geometry']].copy()
id_maker(ngdf)

## Boulder
i = 2
gdf = gdf_reader(i)
i
gdf = gdf[['parcel_id', 'geometry']].copy()
gdf = gdf.dissolve('parcel_id').reset_index()

bo_add = gpd.read_file('08013/Addresses/Address_Points.shp')
bo_add['st_unit'] = bo_add.apply(lambda r: ' '.join([c for c in [r.UNITTYPE, r.UNIT] if c != None]), axis = 1)
bo_add['st_nam'] = bo_add.apply(lambda r: ' '.join([c for c in [r.PRETYPE, r.STREETNAME] if c != None]), axis = 1)
bo_add['situs_city'] = bo_add.apply(lambda r: ' '.join([c for c in [r.CITY, 'CO', r.ZIPCODE] if c != None]), axis = 1)

bo_add.rename(columns = {'PARCEL_NUM' : 'parcel_id', 'STREET_N_1' : 'st_num', 'PREFIX' : 'st_dir', 'STREETTYPE' : 'st_suf'}, inplace = True)
bo_add['situs'] = bo_add.apply(lambda r: ' '.join([c for c in [r.st_num, r.st_dir, r.st_nam, r.st_suf, r.st_unit] if c != None]), axis = 1)

bo_add = bo_add[['parcel_id', 'situs', 'st_num', 'st_nam', 'st_dir', 'st_suf', 'st_unit', 'situs_city']].copy()
bo_add['st_post'] = ''

bo_add = bo_add.dropna(subset = 'parcel_id').copy()

bo_add.parcel_id = bo_add.parcel_id.replace('0', np.nan)
bo_add.parcel_id = bo_add.parcel_id.replace('999999999999', np.nan)

bo_add.dropna(subset = 'parcel_id', inplace = True)

gdf['parcel_id'] = gdf.parcel_id.astype(str).str.zfill(12)
bo_add['parcel_id'] = bo_add.parcel_id.astype(str).str.zfill(12)

jgdf = gdf.merge(bo_add, on = 'parcel_id', how = 'left')

ngdf = jgdf[['parcel_id'] + add_cols + ['geometry']].copy()
len(jgdf)
id_maker(ngdf)

# Broomfield
i = 3
gdf = gdf_reader(i)

bo_adds = ['SITUS_FULL', 'SITUS_AD_1', 'SITUS_ST_1', 'SITUS_ST_3', 'SITUS_ST_4', 'SITUS_ST_5','SITUS_UNIT']
gdf['situs_city'] = ''

gdf = gdf.dissolve('parcel_id').reset_index()

ngdf = gdf[['parcel_id'] + bo_adds + ['situs_city', 'geometry']].copy()
ngdf.columns = ['parcel_id'] + add_cols + ['geometry']

id_maker(ngdf)

## Denver
i = 4
gdf = gdf_reader(i)

den_cos = ['SITUS_AD_1', 'SITUS_AD_3', 'SITUS_STR1', 'SITUS_ST_2', 'SITUS_ST_3', 'SITUS_ST_4', 'SITUS_UNIT']
gdf['situs_city'] = gdf.apply(lambda r: ' '.join([c for c in [r.SITUS_CITY, r.SITUS_STAT, r.SITUS_ZIP] if c != None]), axis = 1)

ngdf = gdf[['parcel_id'] + den_cos + ['situs_city', 'geometry']].copy()
ngdf.columns = ['parcel_id'] + add_cols + ['geometry']

id_maker(ngdf)

# Douglas
i = 5
gdf = gdf_reader(i)

dou_cos = ['ACCOUNT__9', 'ACCOUNT_18', 'ACCOUNT_20', 'ACCOUNT_21', 'ACCOUNT_22', 'ACCOUNT_23', 'ACCOUNT_16']
gdf['situs_city'] = gdf.apply(lambda r: ' '.join([c for c in [r.ACCOUNT_12, r.ACCOUNT_13, r.ACCOUNT_25] if c != None]), axis = 1)
gdf = gdf.dissolve('parcel_id').reset_index()
ngdf = gdf[['parcel_id'] + dou_cos + ['situs_city', 'geometry']].copy()
ngdf.columns = ['parcel_id'] + add_cols + ['geometry']
id_maker(ngdf)


# Jefferson
i = 6
gdf = gdf_reader(i)

j_cos = ['PRPADDRESS', 'PRPSTRNUM', 'PRPSTRDIR', 'PRPSTRNAM', 'PRPSTRSFX', 'PRPSTRUNT']
gdf['situs_city'] = gdf.apply(lambda r: ' '.join([c for c in [r.PRPCTYNAM, r.PRPSTENAM, r.PRPZIP5] if c != None]), axis = 1)
gdf['st_post'] = ''
gdf = gdf.dissolve('parcel_id').reset_index()
ngdf = gdf[['parcel_id'] + j_cos + ['st_post', 'situs_city', 'geometry']].copy()
ngdf.columns = ['parcel_id'] + add_cols + ['geometry']
len(ngdf)
ngdf.parcel_id.nunique()
id_maker(ngdf)

# Weld
i = 7
gdf = gdf_reader(i)

j_cos = ['SITUS', 'STREETNO', 'STREETDIR', 'STREETNAME', 'STREETSUF', 'STREETALP']
gdf['situs_city'] = gdf.LOCCITY
gdf['st_post'] = ''
gdf = gdf.dissolve('parcel_id').reset_index()
gdf['length_addr'] = gdf.SITUS.str.len()
gdf['length_cit'] = gdf.situs_city.str.len()
gdf['SITUS'] = gdf.apply(lambda r: r.SITUS[: int(r.length_addr) - int(r.length_cit) - 2] if type(r.SITUS) == str and type(r.situs_city) == str else None, axis = 1)

ngdf = gdf[['parcel_id'] + j_cos + ['st_post', 'situs_city', 'geometry']].copy()

ngdf.columns = ['parcel_id'] + add_cols + ['geometry']
id_maker(ngdf)

fgdf = pd.concat(df_list)
u_fgdf = pd.concat(unique_list)

fgdf.to_file('parcel_geometries')
fgdf.to_csv('parcel_geometries.csv')
u_fgdf.to_file('unique_parcel_geometries')
