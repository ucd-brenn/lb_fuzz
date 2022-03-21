import pandas as pd

county_code = '08123'
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)

p_add_cols = ['site_addr','site_carrier_code','site_city', 'site_direction', 'site_house_number', 'site_mode', 'site_quadrant', 'site_state', 'site_street_name', 'site_unit_number', 'site_unit_prefix', 'site_zip', 'site_plus_4']

df = pd.read_csv('parcel_geometries.csv')
df['county_id'] = df.geom_id.apply(lambda x: x[:2])
c_df = df.query('county_id == "WE"').copy()

c_df['situs'] = c_df.situs.str.replace('CR', 'COUNTY ROAD')
c_df['st_nam'] = c_df.st_nam.str.replace('CR', 'COUNTY ROAD')

c_df['situs'] = c_df.situs.str.replace('HWY', 'HIGHWAY')
c_df['st_nam'] = c_df.st_nam.str.replace('HWY', 'HIGHWAY')

o_df = pd.read_csv('OutFilesIndiv/WE.csv')
o_ids = o_df.uid.tolist()
p_df.query('uid not in @o_ids')[p_add_cols]
p_df.query('uid not in @o_ids')['site_street_name'].value_counts()
p_df[p_df.site_addr.str.contains('DRY FORK') == True]

p_df.site_addr.str.contains('HIGHWAY').sum()
c_df.situs.str.contains('HWY').sum()
c_df[c_df.situs.str.contains('CEDAR') == True]
