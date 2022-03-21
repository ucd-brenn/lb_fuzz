import pandas as pd
import usaddress
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import os

def parce_return(r):
    addr = r.situs
    try:
        c1, c2 = usaddress.tag(addr)
        try:
            num = c1['AddressNumber']
        except:
            num = ''
        try:
            dir = c1['StreetNamePreDirectional']
        except:
            dir = ''
        try:
            nam = c1['StreetName']
        except:
            nam = ''
        try:
            suf = c1['StreetNamePostType']
        except:
            suf = ''
        try:
            unt_pre = c1['OccupancyType']
        except:
            unt_pre = ''
        try:
            unt_num = c1['OccupancyIdentifier']
        except:
            unt_num = ''
        unt = ' '.join([unt_pre, unt_num])
        return num, dir, nam, suf, unt
    except:
        return '', '', '', '', ''

def fuzz_match(r, option_list):
    site_address = ' '.join([c for c in [r.site_direction, r.site_street_name, r.site_mode, r.site_unit_prefix, r.site_unit_number] if c != None])
    site_address = site_address.upper()
    matches = process.extract(site_address, option_list, scorer=fuzz.partial_ratio)
    return matches[0], matches[1], matches[2], matches[3], matches[4]

def joiner(p_df, c_df, county_code, county_letters, spatial_pre = False):

    join_list = []

    if spatial_pre == True:
        spat = p_df[p_df.geom_id.notnull() == True].copy()
        spat['merge_type'] = 'spatial'
        spat['_merge'] = 'both'

        p_df = p_df[p_df.geom_id.isna() == True].copy()
        p_df.drop(columns = ['geom_id'], inplace = True)
        print('Spatially Joined: {0}'.format(len(spat)))

        join_list.append(spat[c_columns + ['uid', 'geom_id', 'merge_type', '_merge']])


    ## Parcel Join
    print('Parcel Join')
    mdf = p_df.merge(c_df, left_on = ['parcel_apn'], right_on = ['parcel_id'], indicator = True, how = 'left')
    # mdf._merge.value_counts()
    mdf['merge_type'] = 'parcel apn'

    join_list.append(mdf[c_columns + ['uid', 'geom_id', 'merge_type', '_merge']])
    ## Address Join
    a_df = mdf.query('_merge == "left_only"').copy()[c_columns]

    if len(a_df) > 0:
        print('Address Join')
        if c_df.situs.nunique() < 5:
             c_df['situs'] = c_df.apply(lambda r: ' '.join([c for c in [r.st_num, r.st_dir, r.st_nam, r.st_suf, r.st_unit] if c != None]), axis = 1)

        if c_df.st_num.nunique() < 5:
            c_df[['st_num', 'st_dir', 'st_nam', 'st_suf', 'st_unit']] = c_df.apply(parce_return, axis = 1, result_type='expand')


        # a_df.site_addr = a_df.site_addr.str.upper()
        # c_df.situs = c_df.situs.str.upper()

        a_mdf = a_df.merge(c_df, left_on = ['site_addr'], right_on = ['situs'], indicator = True, how = 'left')
        # a_mdf._merge.value_counts()
        a_mdf['merge_type'] = 'situs'
        join_list.append(a_mdf[c_columns + ['uid', 'geom_id', 'merge_type', '_merge']])

        ## Fuzzy Join
        f_df = a_mdf.query('_merge == "left_only"').copy()[c_columns]

        if len(f_df) > 0:
            print('Fuzzy Join')
            c_df['match_id'] = c_df.apply(lambda r: ' '.join([c for c in [str(r.st_dir), str(r.st_nam), str(r.st_suf), str(r.st_unit)] if c != 'None' and c != 'nan']), axis = 1)
            c_df.match_id = c_df.match_id.str.upper()
            j_options = c_df.match_id.unique().tolist()

            f_df[p_add_cols] = f_df[p_add_cols].fillna('')
            print('Fuzzy Length: ')
            print(len(f_df))
            f_df[['match_res_0', 'match_res_1', 'match_res_2', 'match_res_3', 'match_res_4']] = f_df.apply(fuzz_match, args = [j_options], axis = 1, result_type = 'expand')
            for i in [0,1,2,3,4]:
                if len(f_df) > 0:
                    print('Iteration: {0} - checking {1}'.format(i, len(f_df)))
                    f_df[['match_id', 'match_ratio']] = f_df.apply(lambda r: r['match_res_{0}'.format(i)], axis = 1, result_type = 'expand')
                    f_df['match_id'] = f_df.apply(lambda r: r.match_id if r.match_ratio >= 80 else "DO_NOT_MATCH", axis = 1)

                    f_df['site_house_number'] = f_df.site_house_number.apply(lambda x: str(int(float(x))) if pd.notnull(x) and x != '' and x != 'nan'  else '')
                    c_df['st_num'] = c_df.st_num.apply(lambda x: str(int(float(x))) if pd.notnull(x) and x != '' and x != 'nan' else '')

                    f_mdf = f_df.merge(c_df, left_on = ['site_house_number', 'match_id'], right_on = ['st_num', 'match_id'], indicator = True, how = 'left')
                    # f_mdf._merge.value_counts()
                    f_mdf['merge_type'] = 'fuzzy_{0}'.format(i)
                    join_list.append(f_mdf[c_columns + ['uid', 'geom_id', 'merge_type', '_merge']])

                    f_df = f_mdf.query('_merge == "left_only"').copy().drop(columns = ['geom_id', 'merge_type', '_merge'])

    comb_mdf = pd.concat(join_list)
    out_mdf = comb_mdf.query("_merge == 'both'").copy()
    dd_out_mdf = out_mdf.drop_duplicates().copy()
    dd_out_mdf.to_csv('OutFilesIndiv/{0}.csv'.format(county_letters))

    print('Output')
    print('Length of Input (Unique): {0}'.format(len(p_df)))
    print('Length of Output Unique: {0}'.format(dd_out_mdf.uid.nunique()))
    print('Non Coded: {0}'.format(len(p_df) - dd_out_mdf.uid.nunique()))
    print(dd_out_mdf.merge_type.value_counts())

p_add_cols = ['site_addr','site_carrier_code','site_city', 'site_direction', 'site_house_number', 'site_mode', 'site_quadrant', 'site_state', 'site_street_name', 'site_unit_number', 'site_unit_prefix', 'site_zip', 'site_plus_4']

df = pd.read_csv('parcel_geometries.csv')
df.drop(columns = ['Unnamed: 0'], inplace = True)

df['county_id'] = df.geom_id.apply(lambda x: x[:2])
counties = [('08005', 'AR'),
            ('08013', 'BO'),
            ('08014', 'BR'),
            ('08035', 'DO'),
            ('08123', 'WE'),
            ('08001', 'AD'),
            ('08031', 'DE'),
            ('08059', 'JE')]

## arapahoe
county_code, county_letters = counties[0]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()

c_df['parcel_id'] = c_df.parcel_id.str.zfill(16)

p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)
p_df['parcel_apn'] = p_df.parcel_apn.str.zfill(16)

c_columns = p_df.columns.tolist()

joiner(p_df, c_df, county_code, county_letters)

## boulder
county_code, county_letters = counties[1]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)


p_df.parcel_apn = p_df.parcel_apn.astype(str).str.zfill(12)
c_df.parcel_id = c_df.parcel_id.astype(str).str.zfill(12)

c_columns = p_df.columns.tolist()
joiner(p_df, c_df, county_code, county_letters)

## broomfield
county_code, county_letters = counties[2]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
c_df['parcel_id'] = c_df.parcel_id.astype(str).str.zfill(12)

p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)

p_df['parcel_apn'] = p_df.parcel_apn.apply(lambda x: str(int(x)).zfill(12) if pd.notnull(x) else '')
c_columns = p_df.columns.tolist()
joiner(p_df, c_df, county_code, county_letters)

## douglas
county_code, county_letters = counties[3]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)


p_df.parcel_apn = p_df.parcel_apn.astype('str').str.zfill(12)
c_df.parcel_id = c_df.parcel_id.astype('str').str.zfill(12)
c_columns = p_df.columns.tolist()
joiner(p_df, c_df, county_code, county_letters)

## weld
county_code, county_letters = counties[4]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)


p_df.parcel_apn = p_df.parcel_apn.astype('str').str.zfill(12)
c_df.parcel_id = c_df.parcel_id.astype('str').str.zfill(12)
c_df['situs'] = c_df.situs.str.replace('CR', 'COUNTY ROAD')
c_df['st_nam'] = c_df.st_nam.str.replace('CR', 'COUNTY ROAD')

c_df['situs'] = c_df.situs.str.replace('HWY', 'HIGHWAY')
c_df['st_nam'] = c_df.st_nam.str.replace('HWY', 'HIGHWAY')

c_columns = p_df.columns.tolist()
joiner(p_df, c_df, county_code, county_letters)

## Adams
county_code, county_letters = counties[5]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)

p_df.parcel_apn = p_df.parcel_apn.astype('str').str.zfill(13)
c_df.parcel_id = c_df.parcel_id.astype('str').str.zfill(13)
c_columns = p_df.columns.tolist()
joiner(p_df, c_df, county_code, county_letters)

## Denver
county_code, county_letters = counties[6]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)
p_df.rename(columns = {'index' : 'uid'}, inplace = True)


p_df.columns = [c.lower() for c in p_df.columns]
p_df.parcel_apn = p_df.parcel_apn.astype('str').str.zfill(13)
c_df.parcel_id = c_df.parcel_id.astype('str').str.zfill(13)

c_columns = p_df.columns.tolist()
c_columns.remove('geom_id')

joiner(p_df, c_df, county_code, county_letters, spatial_pre=True)

p_df.uid.nunique()
p_df.uid.nunique() - 235060

## Jefferson
county_code, county_letters = counties[7]
print(county_letters)

c_df = df.query('county_id == @county_letters').copy()
p_df = pd.read_csv('{0}/{0}_propertypoints.csv'.format(county_code))
p_df.reset_index(inplace = True, drop = True)
p_df.reset_index(inplace = True)

p_df.rename(columns = {'index' : 'uid'}, inplace = True)

p_df.columns = [c.lower() for c in p_df.columns]
p_df.parcel_apn = p_df.parcel_apn.astype('str').str.zfill(13)
c_df.parcel_id = c_df.parcel_id.astype('str').str.zfill(13)

c_columns = p_df.columns.tolist()
c_columns.remove('geom_id')

joiner(p_df, c_df, county_code, county_letters, spatial_pre=True)
p_df.uid.nunique()
p_df.uid.nunique() - 224140
