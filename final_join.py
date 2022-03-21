import pandas as pd
import numpy as np
import geopandas as gpd

clist = ['AD', 'AR', 'BO', 'BR', 'DE', 'DO', 'JE', 'WE']

df_list = []
geom_map = []

cols = ['propertypoint_id', 'property_dmp_id', 'property_id', 'parcel_dmp_id', 'address_id', 'countyfp', 'fips_muni_code', 'statefp', 'geometry_source', 'addr_score', 'cdfp', 'placefp', 'cousubfp', 'tractfp', 'zcta5ce', 'blockce', 'site_addr', 'site_carrier_code', 'site_city', 'site_direction', 'site_house_number', 'site_mode', 'site_quadrant', 'site_state', 'site_street_name', 'site_unit_number', 'site_unit_prefix', 'site_zip', 'site_plus_4', 'use_code_std_ctgr_lps', 'use_code_std_lps', 'acreage', 'aggr_acreage', 'aggr_group', 'aggr_lot_count', 'air_conditioning', 'alternate_taxapn', 'asmt_doc_type', 'asmt_mail_care_of_name', 'asmt_mail_care_of_name_indicator', 'asmt_mail_care_of_name_type', 'asmt_mail_carrier_code', 'asmt_mail_direction', 'asmt_mail_house_number', 'asmt_mail_mode', 'asmt_mail_quadrant', 'asmt_mail_street_name', 'asmt_owner_occupied', 'asmt_prior_sale_date_transfer', 'asmt_prior_sale_sale_code', 'asmt_prior_sale_val_transfer', 'asmt_rcdrs_date_transfer', 'asmt_sale_code', 'asmt_val_transfer', 'asmt_version_id', 'asmt_year', 'assessee_mail_addr', 'assessee_mail_city', 'assessee_mail_state', 'assessee_mail_unit_number', 'assessee_mail_unit_prefix', 'assessee_mail_zip', 'assessee_mail_zip4', 'assessee_owner_1_indicator', 'assessee_owner_1_name_type', 'assessee_owner_2_indicator', 'assessee_owner_2_name_type', 'assessee_owner_name_1', 'assessee_owner_name_2', 'assessee_owner_vesting_code', 'associate_property_count', 'assr_acreage', 'assr_map_ref', 'assr_sqft', 'avm_value', 'bedrooms', 'bldg_class', 'bldg_number', 'block_number', 'bsmt_1_code', 'building_sqft', 'buyer_name', 'ca_home_owners_exempt', 'cal_acreage', 'cal_sqft', 'cbsa_code', 'census_block_group', 'census_tract', 'city_section', 'company_flag', 'construction_code', 'county', 'date_assr_tape_cut', 'date_cert', 'date_noval_transfer', 'date_transfer', 'district', 'doc_type_desc_lps', 'doc_type_lps', 'doc_type_noval', 'dupe_taxapn_seq', 'elect_avail_code', 'exterior_wall_type', 'fireplace_type', 'foundation_type', 'garage_carport_type', 'heating_type', 'imprv_pct', 'imprv_per_sqft', 'int_rate_1', 'int_rate_type_1', 'land_lot', 'land_per_sqft', 'land_sqft', 'last_market_sale_buyer_1_code', 'last_market_sale_buyer_1_first_mid', 'last_market_sale_buyer_1_last', 'last_market_sale_buyer_2_code', 'last_market_sale_buyer_2_first_mid', 'last_market_sale_buyer_2_last', 'last_market_sale_buyer_name', 'last_market_sale_buyer_vesting_code', 'last_market_sale_loan_due_date', 'last_market_sale_rcdrs_map_ref', 'last_market_sale_seller_1_code', 'last_market_sale_seller_1_first_mid', 'last_market_sale_seller_1_last', 'last_market_sale_seller_2_first_mid', 'last_market_sale_seller_2_last', 'last_market_sale_seller_name', 'last_market_sale_source', 'last_mrkt_sale_src_code', 'last_sale_buyer_1_code', 'last_sale_buyer_1_first_mid', 'last_sale_buyer_1_last', 'last_sale_buyer_2_code', 'last_sale_buyer_2_first_mid', 'last_sale_buyer_2_last', 'last_sale_buyer_name', 'last_sale_date_transfer', 'last_sale_doc_type', 'last_sale_owner_occupied', 'last_sale_sale_code', 'last_sale_seller_1_code', 'last_sale_seller_1_first_mid', 'last_sale_seller_1_last', 'last_sale_seller_2_first_mid', 'last_sale_seller_2_last', 'last_sale_seller_name', 'last_sale_val_transfer', 'last_sale_vesting_code', 'legal_1', 'legal_full', 'legal_sec_twp_rng', 'legal_unit', 'lender_code_1', 'loan_due_date_1', 'loan_type_1', 'lot_code', 'lot_depth', 'lot_number', 'lot_size_area', 'lot_size_area_orgn', 'lot_size_area_unit', 'lot_width', 'mail_addr', 'mail_care_of_name', 'mail_city', 'mail_crrt', 'mail_direction', 'mail_house_number', 'mail_house_number_suffix', 'mail_mode', 'mail_plus_4', 'mail_quadrant', 'mail_state', 'mail_street_name', 'mail_unit_number', 'mail_unit_prefix', 'mail_zip', 'master_parcel_apn', 'msa_code', 'mult_port_code', 'multi_taxapn_flag', 'municipality', 'no_val_date_transfer', 'owner_1_first', 'owner_1_last', 'owner_1_mid', 'owner_2_first', 'owner_2_last', 'owner_2_mid', 'owner_name', 'owner_name_1', 'owner_name_2', 'owner_occupied', 'owner_src_code', 'ownership_status_code', 'parcel_apn', 'parking_spaces', 'partial_baths', 'phase_nbr', 'pool_indicator', 'price_per_acre', 'price_per_sqft', 'prior_market_sale_buyer_1_code', 'prior_market_sale_buyer_1_first_mid', 'prior_market_sale_buyer_1_last', 'prior_market_sale_buyer_2_code', 'prior_market_sale_buyer_2_first_mid', 'prior_market_sale_buyer_2_last', 'prior_market_sale_date_transfer', 'prior_market_sale_doc_type', 'prior_market_sale_loan_due_date', 'prior_market_sale_rcdrs_map_ref', 'prior_market_sale_seller_1_code', 'prior_market_sale_seller_1_first_mid', 'prior_market_sale_seller_1_last', 'prior_market_sale_seller_2_first_mid', 'prior_market_sale_seller_2_last', 'prior_market_sale_source', 'prior_sale_date_transfer', 'prior_sale_doc_type', 'prior_sale_sale_code', 'prior_sale_val_transfer', 'range', 'roof_cover_type', 'sale_code', 'section', 'seller_name', 'stories_number', 'style_type', 'subdivision', 'tax_acct_nbr', 'tax_amount', 'tax_exempt_codes', 'tax_rate_code_area', 'tax_src_code', 'tax_year', 'tax_year_delinq', 'taxapn', 'taxapn_unf', 'topography', 'total_baths', 'total_baths_calculated', 'total_rooms', 'township', 'tract_number', 'tran_type', 'unique_taxapn', 'units_number', 'use_code_muni', 'use_code_muni_desc', 'val_assd', 'val_assd_imprv', 'val_assd_land', 'val_market', 'val_mrkt_imprv', 'val_mrkt_land', 'val_transfer', 'yr_blt', 'yr_blt_effect', 'yr_mrkt_val', 'zoning', 'blkgrpce', 'geom_id', 'uid', 'merge_type']

for cl in clist:
    df = pd.read_csv('OutFilesIndiv/{0}.csv'.format(cl))
    curr_cols = df.columns.tolist()
    col_add_count = 0
    for col in cols:
        if col not in curr_cols:
            df[col] = np.nan
            col_add_count += 1
    df = df[cols].copy()
    df['geom_flag'] = ''
    if not len(df) == df.uid.nunique():
        df.query('site_addr.notnull() or merge_type not in ["situs", "fuzzy_0", "fuzzy_1", "fuzzy_2", "fuzzy_3"]', inplace = True)
        agg_col = df.groupby('uid').geom_id.first().reset_index()
        tdf = df.merge(agg_col, on = 'uid')
        exp = tdf[['geom_id_x', 'geom_id_y']].copy()
        geom_map.append(exp)

        tdf['geom_flag'] = tdf.apply(lambda r: 'flag' if r.geom_id_x != r.geom_id_y else '', axis = 1)
        tdf.drop(columns = 'geom_id_x', inplace = True)
        tdf.rename(columns = {'geom_id_y' : 'geom_id'}, inplace = True)
        df = tdf.drop_duplicates().copy()
    print('{0}: {1}\nColumns Added: {2}'.format(cl, (len(df) == df.uid.nunique()), col_add_count))
    df_list.append(df)

fdf = pd.concat(df_list)
e_file = pd.concat(geom_map)
fdf.geom_flag.value_counts()

fdf.to_csv('FinalLightBox/final_light_box_parcels.csv')
e_file.to_csv('FinalLightBox/geom_mappings.csv')
