import pandas as pd
from pandas.io import sql
import os
import time
from datetime import datetime
from urllib2 import urlopen, URLError, HTTPError
from sqlalchemy import create_engine
import subprocess
import urllib
import MySQLdb
import re

############################################################################################################################
# The purpose of this document...
#   The bls produces A monthly survey of the payroll records of business establishments provides data on employment, hours, and earnings of workers at the National level.
#   as well individual states and metropolitan areas.
#
#   In this document we access the data via this site: 'http://download.bls.gov/pub/time.series/' where we access subdirectories 'sm' and 'ce'
#
#   In each of these subdirectories you will find an 'sm %or% ce.txt' file which describes what data is being represented in-depth.
#
#   Every month, after the survey is posted, we are to run this script, which extracts the files in the 'files' list and downloads them as .txt files
#
#   We take these txt files and the mapping files in the job_growth2 database and merge them to create complete datasets "sm_table_final" & "ce_table_final" for calculations



## Create the SQL alchemy engine for the PANDAS query calls
engine = create_engine("mysql+mysqldb://job_growthmaster:jobgrowthpass@job-growth.cotzely14ram.us-west-2.rds.amazonaws.com/job_growth2?charset=utf8&use_unicode=0")

## Files for extraction, lets keep the prefix subdirectory for simplicity in the download
files = ['sm/sm.data.1.AllData',
         'ce/ce.data.0.AllCESSeries',
         'ce/ce.data.07.TotMinConAECurr',
         'ce/ce.data.08.ManufactureAECurr',
         'ce/ce.data.09.ServProvTradeAECurr',
         'ce/ce.data.10.TransWhUtsAECurr',
         'ce/ce.data.11.InfoAECurr',
         'ce/ce.data.12.FinActAECurr',
         'ce/ce.data.13.ProfBusAECurr',
         'ce/ce.data.14.EducHealthAECurr',
         'ce/ce.data.15.LeisHospAECurr',
         'ce/ce.data.16.OtherServicesAECurr',
         'ce/ce.data.17.GovtAECurr']

ce_files = ['ce/ce.data.07.TotMinConAECurr',
          'ce/ce.data.08.ManufactureAECurr',
          'ce/ce.data.09.ServProvTradeAECurr',
          'ce/ce.data.10.TransWhUtsAECurr',
          'ce/ce.data.11.InfoAECurr',
          'ce/ce.data.12.FinActAECurr',
          'ce/ce.data.13.ProfBusAECurr',
          'ce/ce.data.14.EducHealthAECurr',
          'ce/ce.data.15.LeisHospAECurr',
          'ce/ce.data.16.OtherServicesAECurr',
          'ce/ce.data.17.GovtAECurr']



## Relative filepaths, downloaded data goes to the 'data' subdirectory of this file
dir = os.path.abspath(os.path.dirname(__file__))
datadir = dir + "\\data"
os.chdir(datadir)


## the name of the url for downloading data
data_hostname = "http://download.bls.gov/pub/time.series/"
current_filesystem = datadir


############################################################################################################################
# The purpose of this section...
#
#   Here we are iterating through the files object, creating file names and using urllib library to retrieving the page.
#


for filename in files: # Loop through the files in files dictonary
    filename_extension = filename[3:] + ".txt" # Filename munge
    data_location = data_hostname + "" + filename # file name location
    full_filepath = current_filesystem + "/" + filename_extension # full location
    print "downloading from: " + data_location
    urllib.urlretrieve(data_location, full_filepath) # grab that shit
    print "download path: " + full_filepath

print "Finished Downloading Data"

## Stripping text of the
def strip(text):
    try:
        return text.strip()
    except AttributeError():
        return text


## Special field here to update the month for the current year to date, this will be deprecated. The information can be retrieved in any of
## the tables

date = datetime.now()

year_to_date = date.month - 1
year = date.year

year_to_date = str(year_to_date)
year = str(year)

d = {'month' : [year_to_date],
     'year'  : [year]}

date_frame = pd.DataFrame(d)

date_frame.to_sql('date_ref_t', engine, flavor = 'mysql', if_exists = 'replace')

year_to_date = int(year_to_date)
year = int(year)

############################################################################################################################
# The purpose of this section...
#
#   Read in all of the mapping files and the main sm.data.1.AllData.txt file to create a complete sm_table_final which describes all
#   data related to the state and metro current employment statistics
#
#   sm_state is the mapping for the state field
#   sm_supersector is the mapping for the supersector codes
#   sm_series   is the mapping that describes the series objects
#   *series are special concatenated codes that describe the set of observed data
#   sm_industry is the mapping for the industry codes
#   sm_datatype is the mapping for the data types
#   sm_area is the mapping file for the areas (metropolitan)


try:

    sm_state = pd.read_sql_query('SELECT * FROM sm_state', engine)
    sm_state['state_name'].replace('\\r','')

    sm_supersector = pd.read_sql_query('SELECT * FROM sm_supersector', engine)
    sm_supersector['supersector_code'] = sm_supersector['supersector_code'].astype(str)
    sm_supersector['supersector_code'] = sm_supersector['supersector_code'].apply( lambda x: x.zfill(8))
    sm_supersector['supersector_name'] = sm_supersector['supersector_name'].replace('\\r','')


    sm_series = pd.read_sql_query('SELECT * FROM sm_series', engine)
    sm_series['area_code'] = sm_series['area_code'].astype(str)
    sm_series['area_code'] = sm_series['area_code'].apply(lambda x: x.zfill(5))
    sm_series['industry_code'] = sm_series['industry_code'].astype(str)
    sm_series['industry_code'] = sm_series['industry_code'].apply(lambda x: x.zfill(8))
    sm_series['supersector_code'] = sm_series['supersector_code'].astype(str)
    sm_series['supersector_code'] = sm_series['supersector_code'].apply(lambda x: x.zfill(8))


    sm_industry = pd.read_sql_query('SELECT * FROM sm_industry', engine)
    sm_industry['industry_code'] = sm_industry['industry_code'].astype(str)
    sm_industry['industry_code'] = sm_industry['industry_code'].apply(lambda x: x.zfill(8))


    sm_datatype = pd.read_sql_query('SELECT * FROM sm_data_type', engine)

    sm_area = pd.read_sql_query('SELECT * FROM sm_area', engine)
    sm_area['area_code'] = sm_area['area_code'].str.replace('\n','')

    ## Read in the actual data, this step will take the longest, this is a big file
    sm_table = pd.read_table("sm.data.1.AllData.txt",
                             converters = {'series_id' : strip,
                                        'year' : strip,
                                        'value' : strip,
                                        'footnote_codes': strip},
                              dtype = {'series_id' : object,
                                        'year' : object})


    ## Merge in the mapping files so the table is read friendly, delete unused tables as we go to preserve memory
    sm_table_v1 = pd.merge(sm_table, sm_series, on = 'series_id')
    del sm_table
    sm_table_v2 = pd.merge(sm_table_v1, sm_industry, on = 'industry_code')
    del sm_table_v1
    sm_table_v3 = pd.merge(sm_table_v2, sm_supersector, on = 'supersector_code')
    del sm_table_v2
    sm_table_v4 = pd.merge(sm_table_v3, sm_state, on = 'state_code')
    del sm_table_v3
    sm_table_v5 = pd.merge(sm_table_v4, sm_area, on = 'area_code')
    del sm_table_v4

    print "sm_table_v5 *************************************";

    ## Rename columns
    sm_table_v5.rename(columns={'state_name_x':'state_name'}, inplace=True)

    ## Select the columns we wish to keep
    sm_table_final = sm_table_v5[['area_name','state_name','supersector_name','industry_name','period','year','value','data_type_code','seasonal']]
    del sm_table_v5


    sm_table_final['value'] = sm_table_final['value'].convert_objects(convert_numeric = True)
    sm_table_final['period'] = sm_table_final['period'].astype(str)


    print("sm_table_final is completed")



    ################################################################################################
    # Deal with MSA peculiarities
    #   Here we deal with the interesting issue of NECTA and Metropolitan definitions. We exclude the MSAs with definitions like
    #   "Metropolitan Division" and "Necta Division"
    #
    #   We keep the MSAs with just "NECTA" although we take "NECTA" out of the string for displaying purposes
    #   We keep the MSAs with just "Metropolitan Statistical Area" although we remove that part of the string for display
    #
    #   We then split the sm_table_final table into two based on the 'seasonal' value, seasonal calculations are for month over month, non seasonal are for everything else
    #
    #   v2 = not seasonal
    #   v3 = seasonal


    sm_table_final = sm_table_final[(~sm_table_final.area_name.str.contains('Metropolitan Division'))]
    sm_table_final = sm_table_final[(~sm_table_final.area_name.str.contains('NECTA Division'))]

    sm_table_final['area_name'] = sm_table_final['area_name'].str.replace(' NECTA','')
    sm_table_final['area_name'] = sm_table_final['area_name'].str.replace('Metropolitan Statistical Area','')

    sm_table_final['state_name'] = sm_table_final['state_name'].str.replace('\r','')
    sm_table_final['supersector_name'] = sm_table_final['supersector_name'].str.replace('\r','')
    sm_table_final['data_type_code'] = sm_table_final['data_type_code'].astype(str)
    sm_table_final['data_type_code'] = sm_table_final['data_type_code'].apply(lambda x: x.zfill(2))


    ## Exclude District of Columbia, Virgin Islands, and Puerto Rico, M13 is an average. We want not seasonal data
    sm_table_final_v2 = sm_table_final.query('seasonal == "U" and data_type_code == "01" and\
                                    state_name != "District of Columbia" \
                                    and state_name != "Virgin Islands"\
                                    and state_name != "Puerto Rico"\
                                    and period != "M13"')


    ## Exclude District of Columbia, Virgin Islands, and Puerto Rico, M13 is an average. We want seasonal data
    sm_table_final_v3 = sm_table_final.query('seasonal == "S" and data_type_code == "01" and\
                                    state_name != "District of Columbia" \
                                    and state_name != "Virgin Islands"\
                                    and state_name != "Puerto Rico"\
                                    and period != "M13"')


    # Change column name from period to Month and remove 'M'
    sm_table_final_v3.rename(columns = {'period':'Month'}, inplace = True)
    sm_table_final_v3['Month'] = sm_table_final_v3['Month'].apply(lambda x: re.sub('M', '',x))

    sm_table_final_v2.rename(columns = {'period':'Month'}, inplace = True)
    sm_table_final_v2['Month'] = sm_table_final_v2['Month'].apply(lambda x: re.sub('M', '',x))


    ############################################################################################################################
    # The purpose of this section...
    #
    #   This section mirrors the previous section, but now we do it for national data
    #
    #
    #
    #

    #ce files
    ce_supersector = pd.read_sql_query('SELECT * FROM ce_supersector', engine)
    ce_supersector['supersector_code'] = ce_supersector['supersector_code'].astype(str)
    ce_supersector['supersector_code'] = ce_supersector['supersector_code'].apply(lambda x: x.zfill(8))


    ce_datatype = pd.read_sql_query('SELECT * FROM ce_data_type', engine)


    ce_industry = pd.read_sql_query('SELECT * FROM ce_industry', engine)
    ce_industry['industry_code'] = ce_industry['industry_code'].astype(str)
    ce_industry['industry_code'] = ce_industry['industry_code'].apply(lambda x: x.zfill(8))


    ce_period = pd.read_sql_query('SELECT * FROM ce_period', engine)


    ce_series = pd.read_sql_query('SELECT * FROM ce_series', engine)
    ce_series['series_id'] = ce_series['series_id'].astype(str)
    ce_series['series_id'] = ce_series['series_id'].apply(strip)
    ce_series['supersector_code'] = ce_series['supersector_code'].astype(str)
    ce_series['supersector_code'] = ce_series['supersector_code'].apply(lambda x: x.zfill(8))
    ce_series['industry_code'] = ce_series['industry_code'].astype(str)
    ce_series['industry_code'] = ce_series['industry_code'].apply(lambda x: x.zfill(8))

    ce_table_final = pd.DataFrame()

    # Creation of CE table from all of the individual files
    ce_table = pd.DataFrame()


    for table in ce_files:
      filename_extension = table[3:] + ".txt" # Filename munge
      print filename_extension
      temp_table = pd.read_table(filename_extension,
                                converters = {'series_id' : strip,
                                              'year'     : strip,
                                              'footnote_codes' : strip},
                                dtype = {'series_id' : object,
                                         'year'      : object})

      ce_table = ce_table.append(temp_table)

    print "Finished ce_table"

    ce_table_v1 = pd.merge(ce_table, ce_series, on = 'series_id')
    del ce_table
    ce_table_v2 = pd.merge(ce_table_v1, ce_industry, on = 'industry_code')
    del ce_table_v1
    ce_table_v3 = pd.merge(ce_table_v2, ce_supersector, on = 'supersector_code')
    del ce_table_v2

    ce_table_final = ce_table_final.append(ce_table_v3, ignore_index = True)
    ce_table_final = ce_table_final[['supersector_name','industry_name','period','year','value','data_type_code','seasonal']]
    del ce_table_v3


    ce_table_final_v2 = ce_table_final.query('seasonal == "U" and period != "M13"')
    ce_table_final_v3 = ce_table_final.query('seasonal == "S" and period != "M13"')


    ce_table_final_v3.rename(columns = {'period':'Month'}, inplace = True)
    ce_table_final_v3['Month'] = ce_table_final_v3['Month'].apply(lambda x: re.sub('M', '',x))

    ce_table_final_v2.rename(columns = {'period':'Month'}, inplace = True)
    ce_table_final_v2['Month'] = ce_table_final_v2['Month'].apply(lambda x: re.sub('M', '',x))

    print("************************  Created Final Tables: SM & CE")


    ############################################################################################################################
    # The purpose of this section...
    #
    #   Now we have arrived at the national calculation section. Here we are organizing the data in order to create columns calculated from existing columns
    #   in the dataset
    #
    #
    #
    #

    def sums_nat_f():

        ## grouping the data to normalize month over month (hence v3)
        group_nat = ce_table_final_v3.groupby(['supersector_name','industry_name','year','Month'])
        sums_by_period = group_nat['value'].sum()
        sum_nat_mom = pd.DataFrame(sums_by_period).reset_index()
        sum_nat_mom.columns = ['supersector_name','industry_name','year','Month','value_mom']

        ## grouping the data to normalize for all others (hence v2)
        group_nat = ce_table_final_v2.groupby(['supersector_name','industry_name','year','Month'])
        sums_by_period = group_nat['value'].sum()
        sum_nat = pd.DataFrame(sums_by_period).reset_index()

        ## Merge the datasets so now I have one set with columns "value" and "value_mom" (value month over month)
        sum_nat = pd.merge(sum_nat, sum_nat_mom, on = ['supersector_name','industry_name','year','Month'])

        ## ytd stands for year to date
        sum_nat['value_ytd_avg'] = pd.rolling_mean(sum_nat['value'],year_to_date)
        sum_nat['pct_change_ytd'] = sum_nat['value_ytd_avg'].pct_change(12)
        ## no need to rank, only one nation
        sum_nat['rank_ytd'] = ""
        sum_nat['job_growth_ytd'] = sum_nat['value_ytd_avg'].shift(12)*sum_nat['pct_change_ytd']
        sum_nat['pct_change_ytd'] = (sum_nat['pct_change_ytd']*100).round(2)
        sum_nat['job_growth_ytd'] =  sum_nat['job_growth_ytd'].round(2)

        ## ann stands for annual average
        sum_nat['value_ann_avg'] = pd.rolling_mean(sum_nat['value'],12)
        sum_nat['pct_change_ann'] = sum_nat['value_ann_avg'].pct_change(12)
        sum_nat['rank_ann'] = ""
        sum_nat['job_growth_ann'] = sum_nat['value_ann_avg'].shift(12)*sum_nat['pct_change_ann']
        sum_nat['pct_change_ann'] = (sum_nat['pct_change_ann']*100).round(2)
        sum_nat['job_growth_ann'] =  sum_nat['job_growth_ann'].round(2)

        ## no prefix means we are using a standard year over year calculation
        sum_nat['rank'] = ""
        sum_nat['pct_change'] = sum_nat['value'].pct_change(12)
        sum_nat['job_growth'] = sum_nat['value'].shift(12)*sum_nat['pct_change']
        sum_nat['pct_change'] = (sum_nat['pct_change']*100).round(2)
        sum_nat['job_growth'] = sum_nat['job_growth'].round(2)
        sum_nat['area_name'] = "United States"
        sum_nat['state_name'] = "United States"

        ## mom is month over month
        sum_nat['pct_change_mom'] = sum_nat['value_mom'].pct_change(1)
        sum_nat['rank_mom'] = ""
        sum_nat['job_growth_mom'] = sum_nat['value_mom'].shift(1)*sum_nat['pct_change_mom']
        sum_nat['pct_change_mom'] = (sum_nat['pct_change_mom']*100).round(2)
        sum_nat['job_growth_mom'] = sum_nat['job_growth_mom'].round(2)

        sum_nat = sum_nat.astype(object).where(pd.notnull(sum_nat), None)
        sum_nat['industry_name'] = sum_nat['industry_name'].str.replace('\r','')
        sum_nat['supersector_name'] = sum_nat['supersector_name'].str.replace('\r','')

        sum_nat['industry_name'] = sum_nat['industry_name'].str.replace('Goods-Producing','Goods Producing')
        sum_nat['supersector_name'] = sum_nat['supersector_name'].str.replace('Goods-Producing','Goods Producing')
        print("************************  Created Final Tables: sum_nat")
        return(sum_nat)

    ############################################################################################################################
    # The purpose of this section...
    #   Here we are calculating the states section. In this we look specifically at states and create all of the metrics needed for the site
    #
    #
    #
    #
    #

    def sums_states_f():

        ## We are interested in states, so include them
        sums_states_mom = sm_table_final_v3.query('area_name == "Statewide"')
        group_g2 = sums_states_mom.groupby(['state_name','supersector_name','industry_name','year','Month'])
        sums_by_state = group_g2['value'].sum()
        sums_states_mom = pd.DataFrame(sums_by_state).reset_index()
        sums_states_mom.columns = ['state_name','supersector_name','industry_name','year','Month','value_mom']

        sums_states = sm_table_final_v2.query('area_name == "Statewide"')
        group_g2 = sums_states.groupby(['state_name','supersector_name','industry_name','year','Month'])
        sums_by_state = group_g2['value'].sum()
        sums_states = pd.DataFrame(sums_by_state).reset_index()

        sums_states = pd.merge(sums_states, sums_states_mom, on = ["state_name","supersector_name","industry_name","year","Month"], how = "left")

        sums_states['pct_change_mom'] = sums_states['value_mom'].pct_change(1)
        sums_states['rank_mom']       = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False, method = 'first')
        sums_states['job_growth_mom'] = sums_states['value_mom'].shift(1)*sums_states['pct_change_mom']
        sums_states['pct_change_mom'] = (sums_states['pct_change_mom']*100).round(2)
        sums_states['job_growth_mom'] = sums_states['job_growth_mom'].round(2)

        sums_states['value_ytd_avg'] = pd.rolling_mean(sums_states['value'], year_to_date)
        sums_states['pct_change_ytd'] = sums_states['value_ytd_avg'].pct_change(12)
        sums_states['rank_ytd'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_states['job_growth_ytd'] = sums_states['value_ytd_avg'].shift(12)*sums_states['pct_change_ytd']
        sums_states['pct_change_ytd'] = (sums_states['pct_change_ytd']*100).round(2)
        sums_states['job_growth_ytd'] =  sums_states['job_growth_ytd'].round(2)

        sums_states['value_ann_avg'] = pd.rolling_mean(sums_states['value'], 12)
        sums_states['pct_change_ann'] = sums_states['value_ann_avg'].pct_change(12)
        sums_states['rank_ann'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_states['job_growth_ann'] = sums_states['value_ann_avg'].shift(12)*sums_states['pct_change_ann']
        sums_states['pct_change_ann'] = (sums_states['pct_change_ann']*100).round(2)
        sums_states['job_growth_ann'] =  sums_states['job_growth_ann'].round(2)

        sums_states['pct_change'] = sums_states['value'].pct_change(12)
        sums_states['rank'] = sums_states.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_states['job_growth'] = sums_states['value'].shift(12)*sums_states['pct_change']
        sums_states['pct_change'] = (sums_states['pct_change']*100).round(2)
        sums_states['job_growth'] = sums_states['job_growth'].round(2)

        print("************************  Created Final Tables: sums_states")
        return(sums_states)

    ############################################################################################################################
    # The purpose of this section...
    #
    #   Here we are calculating the msas
    #
    #
    #
    #
    #

    def sums_msa_f():

        ## here we only want msas, lets exclude states and keep areas
        sums_msa_mom = sm_table_final_v3.query('area_name != "Statewide"')
        group_g2 = sums_msa_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_by_msa = group_g2['value'].sum()
        sums_msa_mom = pd.DataFrame(sums_by_msa).reset_index()
        sums_msa_mom.columns = ['area_name','supersector_name','industry_name','year','Month','value_mom']

        sums_msa = sm_table_final_v2.query('area_name != "Statewide"')
        group_g3 = sums_msa.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_by_msa = group_g3['value'].sum()
        sums_msa = pd.DataFrame(sums_by_msa).reset_index()


        #Left merge needs to happen here. Need to include all industries from sums_msa because sums_msa_mom only includes Total Nonfarm
        sums_msa = pd.merge(sums_msa, sums_msa_mom, on = ['area_name','supersector_name','industry_name','year','Month'], how = "left")

        sums_msa['pct_change_mom'] = sums_msa['value_mom'].pct_change(1)
        sums_msa['rank_mom']       = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_mom'] = sums_msa['value_mom'].shift(1)*sums_msa['pct_change_mom']
        sums_msa['pct_change_mom'] = (sums_msa['pct_change_mom']*100).round(2)
        sums_msa['job_growth_mom'] = sums_msa['job_growth_mom'].round(2)

        sums_msa['value_ytd_avg'] = pd.rolling_mean(sums_msa['value'], year_to_date)
        sums_msa['pct_change_ytd'] = sums_msa['value_ytd_avg'].pct_change(12)
        sums_msa['rank_ytd'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_ytd'] = sums_msa['value_ytd_avg'].shift(12)*sums_msa['pct_change_ytd']
        sums_msa['pct_change_ytd'] = (sums_msa['pct_change_ytd']*100).round(2)
        sums_msa['job_growth_ytd'] =  sums_msa['job_growth_ytd'].round(2)

        sums_msa['value_ann_avg'] = pd.rolling_mean(sums_msa['value'], 12)
        sums_msa['pct_change_ann'] = sums_msa['value_ann_avg'].pct_change(12)
        sums_msa['rank_ann'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa['job_growth_ann'] = sums_msa['value_ann_avg'].shift(12)*sums_msa['pct_change_ann']
        sums_msa['pct_change_ann'] = (sums_msa['pct_change_ann']*100).round(2)
        sums_msa['job_growth_ann'] =  sums_msa['job_growth_ann'].round(2)

        sums_msa['pct_change'] = sums_msa['value'].pct_change(12)
        sums_msa['rank'] = sums_msa.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa['job_growth'] = sums_msa['value'].shift(12)*sums_msa['pct_change']
        sums_msa['pct_change'] = (sums_msa['pct_change']*100).round(2)
        sums_msa['job_growth'] = sums_msa['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa")
        return(sums_msa)

############################################################################################################################
# The purpose of this section...
#
#   here we are querying for MSAs which have a total nonfarm job base of 1,000,000 workers and then performing our calculatings
#
#
#
#

    def sums_msa_over_f():
        ## we need to figure out which MSAs are over 1000000
        sums_msa_over_mom = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value > 1000)')
        ## lets get a list of the names
        msas_over = list(sums_msa_over_mom['area_name'].unique())
        ## now lets filter the table down for only those MSAs
        sums_msa_over_mom = sm_table_final_v3[sm_table_final_v3['area_name'].isin(msas_over)]
        group_msas = sums_msa_over_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_over_mom = group_msas['value'].sum()
        sums_msa_over_mom = pd.DataFrame(sums_msa_over_mom).reset_index()
        sums_msa_over_mom.columns = ['area_name','supersector_name','industry_name','year','Month', 'value_mom']


        sums_msa_over = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value > 1000)')
        msas_over = list(sums_msa_over['area_name'].unique())
        sums_msa_over = sm_table_final_v3[sm_table_final_v3['area_name'].isin(msas_over)]
        group_msas = sums_msa_over.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_over = group_msas['value'].sum()
        sums_msa_over = pd.DataFrame(sums_msa_over).reset_index()

        sums_msa_over = pd.merge(sums_msa_over, sums_msa_over_mom,  on = ['area_name','supersector_name','industry_name','year','Month'], how = "left")


        sums_msa_over['pct_change_mom'] = sums_msa_over['value_mom'].pct_change(1)
        sums_msa_over['rank_mom'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False)
        sums_msa_over['job_growth_mom'] = sums_msa_over['value_mom'].shift(1)*sums_msa_over['pct_change_mom']
        sums_msa_over['pct_change_mom'] = (sums_msa_over['pct_change_mom']*100).round(2)
        sums_msa_over['job_growth_mom'] = sums_msa_over['job_growth_mom'].round(2)

        sums_msa_over['value_ytd_avg'] = pd.rolling_mean(sums_msa_over['value'],year_to_date)
        sums_msa_over['pct_change_ytd'] = sums_msa_over['value_ytd_avg'].pct_change(12)
        sums_msa_over['rank_ytd'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth_ytd'] = sums_msa_over['value_ytd_avg'].shift(12)*sums_msa_over['pct_change_ytd']
        sums_msa_over['pct_change_ytd'] = (sums_msa_over['pct_change_ytd']*100).round(2)
        sums_msa_over['job_growth_ytd'] =  sums_msa_over['job_growth_ytd'].round(2)

        sums_msa_over['value_ann_avg'] = pd.rolling_mean(sums_msa_over['value'], 12)
        sums_msa_over['pct_change_ann'] = sums_msa_over['value_ann_avg'].pct_change(12)
        sums_msa_over['rank_ann'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth_ann'] = sums_msa_over['value_ann_avg'].shift(12)*sums_msa_over['pct_change_ann']
        sums_msa_over['pct_change_ann'] = (sums_msa_over['pct_change_ann']*100).round(2)
        sums_msa_over['job_growth_ann'] =  sums_msa_over['job_growth_ann'].round(2)

        sums_msa_over['pct_change'] = sums_msa_over['value'].pct_change(12)
        sums_msa_over['rank'] = sums_msa_over.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa_over['job_growth'] = sums_msa_over['value'].shift(12)*sums_msa_over['pct_change']
        sums_msa_over['pct_change'] = (sums_msa_over['pct_change']*100).round(2)
        sums_msa_over['job_growth'] = sums_msa_over['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa_over")
        return(sums_msa_over)


############################################################################################################################
# The purpose of this section...
#
#   Here we are querying for MSAs with total nonfarm under 1,000,000 jobs
#
#
#
#

    def sums_msa_under_f():
        sums_msa_under_mom = sm_table_final_v3.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value < 1000)')
        group_msas = sums_msa_under_mom.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_under_mom = group_msas['value'].sum()
        sums_msa_under_mom = pd.DataFrame(sums_msa_under_mom).reset_index()
        sums_msa_under_mom.columns = ['area_name','supersector_name', 'industry_name', 'year', 'Month', 'value_mom']

        sums_msa_under = sm_table_final_v2.query('area_name != "Statewide" and (industry_name == "Total Nonfarm" and value < 1000)')
        group_msas = sums_msa_under.groupby(['area_name','supersector_name','industry_name','year','Month'])
        sums_msa_under = group_msas['value'].sum()
        sums_msa_under = pd.DataFrame(sums_msa_under).reset_index()


        sums_msa_under = pd.merge(sums_msa_under, sums_msa_under_mom, how = "left", on = ['area_name','supersector_name', 'industry_name', 'year', 'Month'])


        sums_msa_under['pct_change_mom'] = sums_msa_under['value_mom'].pct_change(1)
        sums_msa_under['rank_mom'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_mom'].rank(ascending = False)
        sums_msa_under['job_growth_mom'] = sums_msa_under['value_mom'].shift(1)*sums_msa_under['pct_change_mom']
        sums_msa_under['pct_change_mom'] = (sums_msa_under['pct_change_mom']*100).round(2)
        sums_msa_under['job_growth_mom'] = sums_msa_under['job_growth_mom'].round(2)

        sums_msa_under['value_ytd_avg'] = pd.rolling_mean(sums_msa_under['value'],year_to_date)
        sums_msa_under['pct_change_ytd'] = sums_msa_under['value_ytd_avg'].pct_change(12)
        sums_msa_under['rank_ytd'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ytd'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth_ytd'] = sums_msa_under['value_ytd_avg'].shift(12)*sums_msa_under['pct_change_ytd']
        sums_msa_under['pct_change_ytd'] = (sums_msa_under['pct_change_ytd']*100).round(2)
        sums_msa_under['job_growth_ytd'] =  sums_msa_under['job_growth_ytd'].round(2)

        sums_msa_under['value_ann_avg'] = pd.rolling_mean(sums_msa_under['value'], 12)
        sums_msa_under['pct_change_ann'] = sums_msa_under['value_ann_avg'].pct_change(12)
        sums_msa_under['rank_ann'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change_ann'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth_ann'] = sums_msa_under['value_ann_avg'].shift(12)*sums_msa_under['pct_change_ann']
        sums_msa_under['pct_change_ann'] = (sums_msa_under['pct_change_ann']*100).round(2)
        sums_msa_under['job_growth_ann'] =  sums_msa_under['job_growth_ann'].round(2)

        sums_msa_under['pct_change'] = sums_msa_under['value'].pct_change(12)
        sums_msa_under['rank'] = sums_msa_under.groupby(['supersector_name','industry_name','year','Month'])['pct_change'].rank(ascending = False, method = 'first')
        sums_msa_under['job_growth'] = sums_msa_under['value'].shift(12)*sums_msa_under['pct_change']
        sums_msa_under['pct_change'] = (sums_msa_under['pct_change']*100).round(2)
        sums_msa_under['job_growth'] = sums_msa_under['job_growth'].round(2)

        print("************************  Created Final Tables: sums_msa_under")
        return(sums_msa_under)


############################################################################################################################
# The purpose of this section...
#
#   All of our calculation sections are in functions so they can be turned on and off. This allows one to run just the states for instance
#   or just the MSAs. This can be very valuable when you are troubleshooting and debugging
#
#
#
#
#

    sum_nat = sums_nat_f()
    sums_states = sums_states_f()
    sums_msa = sums_msa_f()
    sums_msa_over = sums_msa_over_f()
    sums_msa_under = sums_msa_under_f()

# ###########################################################################################
# # and the data goes off to the database, where we pull for the visualization on the site

    # combine the states and the MSAs into the states table for the Archive section
    sums_msa.rename(columns = {'area_name':'state_name'}, inplace = True)
    sums_states_v2 = sums_states.append(sums_msa)

    print("************************  Shipping Final Tables to the Database: National Data")
    sum_nat.to_sql('national_rankings_t', engine, flavor='mysql', if_exists='replace', chunksize=10000)

    print("************************  Shipping Final Tables to the Database: State Data")
    sums_states_v2.to_sql('state_rankings', engine, flavor='mysql', if_exists='replace', chunksize=20000)

    print("************************  Shipping Final Tables to the Database: MSA Data")
    sums_msa.to_sql('msa_rankings', engine, flavor='mysql', if_exists='replace', chunksize=20000)

    print("************************  Shipping Final Tables to the Database: MSA Under 1,000,000")
    sums_msa_under.to_sql('msa_rankings_under', engine, flavor='mysql', if_exists='replace', chunksize=20000)

    print("************************  Shipping Final Tables to the Database: MSA Over 1,000,000")
    sums_msa_over.to_sql('msa_rankings_over', engine, flavor='mysql', if_exists='replace', chunksize=20000)

    del sum_nat
    del sums_states
    del sums_msa
    del sums_msa_over
    del sums_msa_under

finally:
    filepath = os.path.join( os.path.dirname( __file__ ), '..' )
    os.chdir(filepath)