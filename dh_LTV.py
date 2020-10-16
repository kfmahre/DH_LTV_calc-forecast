# -*- coding: utf-8 -*-
"""
Created on Tue Sep  1 11:41:29 2020

@author: kfmah
"""

from __future__ import division
from datetime import datetime, timedelta, date
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.schema import DropTable
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
#import re
# always keep these below around for TS decomp
'''
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import acf, pacf
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
'''
import pmdarima as pm
#from pmdarima.arima.utils import ndiffs

#%% security

email = XXXX
secret_password = XXXX

#%%

# Specify the DB and SCHEMA

ctx = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SINGULAR_PROD",
    schema="SINGULAR"
    )
cs = ctx.cursor()

#sql = 'show roles;'

role = 'use role PROD_ADMIN;'

sql = "select CUSTOM_USER_IDS, PLATFORM, CAMPAIGN_NAME, SUB_CAMPAIGN_NAME, EVENT_TIMESTAMP, CAST(EVENT_TIMESTAMP AS date), PARTNER from USER_MARKETING_DATA"

try:
    cs.execute(role)
    SQL_Query = cs.execute(sql)
    data_USER_MARKETING = pd.DataFrame(SQL_Query, columns=['CUSTOM_USER_IDS', 'PLATFORM', 'CAMPAIGN_NAME', 'SUB_CAMPAIGN_NAME', 'EVENT_TIMESTAMP', 'SESSION_DATE', 'PARTNER'])
finally:
    cs.close()
ctx.close()


#%%

# Specify the DB and SCHEMA
'''
ctx1 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD",
    schema="DH_REPORTING_PROD"
    )
cs = ctx1.cursor()

role = 'use role PROD_ADMIN;'

sql1 = "select to_date(timestamp) as DT, context_device_type as PLATFORM, count(distinct user_id) as DAU from SEGMENT_EVENTS_PROD.DIMENSION_HOP_ANDROID_PRODUCTION.APPLICATION_OPENED where to_date(timestamp) >= '2020-03-01' and split_part(context_locale, '-', 2) in ('NZ', 'AU') group by 1, 2 union all select to_date(timestamp) as dt, context_device_type as platform, count(distinct user_id) as dau from SEGMENT_EVENTS_PROD.DIMENSION_HOP_IOS_PRODUCTION.APPLICATION_OPENED where to_date(timestamp) >= '2020-03-01' and split_part(context_locale, '-', 2) in ('NZ', 'AU') group by 1, 2;"

try:
    cs.execute(role)
    SQL_Query1 = cs.execute(sql1)
    data_DAU = pd.DataFrame(SQL_Query1, columns=['DT', 'PLATFORM', 'DAU'])
finally:
    cs.close()
ctx1.close()
'''
#%% Android open data

# Specify the DB and SCHEMA

ctx2 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD",
    schema="DIMENSION_HOP_ANDROID_PRODUCTION"
    )
cs = ctx2.cursor()

role = 'use role PROD_ADMIN;'

# ('ID', 'VN', 'MY', 'PH', 'SG', 'AU')

#sql2 = "select distinct USER_ID, CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_OPENED where to_date(timestamp) >= '2020-07-21' and split_part(context_locale, '-', 2) in ('NZ', 'AU')"
sql2 = "select distinct USER_ID, CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_OPENED where to_date(timestamp) >= '2020-07-21'"

try:
    cs.execute(role)
    SQL_Query2 = cs.execute(sql2)
    #data_ANDROID_OPENS = pd.DataFrame(SQL_Query2, columns=['TIMESTAMP','USER_ID'])
    data_ANDROID_OPENS = pd.DataFrame(SQL_Query2, columns=['USER_ID','CONTEXT_DEVICE_ID','TIMESTAMP','DATA_TS','CONTEXT_LOCALE'])
finally:
    cs.close()
ctx2.close()

#%% IOS open data

# Specify the DB and SCHEMA

ctx3 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD",
    schema="DIMENSION_HOP_IOS_PRODUCTION"
    )
cs = ctx3.cursor()

role = 'use role PROD_ADMIN;'

#sql3 = "select distinct USER_ID, CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_OPENED where to_date(timestamp) >= '2020-07-21' and split_part(context_locale, '-', 2) in ('NZ', 'AU')"
sql3 = "select distinct USER_ID, CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_OPENED where to_date(timestamp) >= '2020-07-21'"

try:
    cs.execute(role)
    SQL_Query3 = cs.execute(sql3)
    data_IOS_OPENS = pd.DataFrame(SQL_Query3, columns=['USER_ID','CONTEXT_DEVICE_ID','TIMESTAMP','DATA_TS','CONTEXT_LOCALE'])
finally:
    cs.close()
ctx3.close()

#%% Android install data

# Specify the DB and SCHEMA

ctx4 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD",
    schema="DIMENSION_HOP_ANDROID_PRODUCTION"
    )
cs = ctx4.cursor()

#sql = 'show roles;'

role = 'use role PROD_ADMIN;'

#sql4 = "select distinct CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_INSTALLED where to_date(timestamp) >= '2020-07-21' and split_part(context_locale, '-', 2) in ('NZ', 'AU')"
sql4 = "select distinct CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_INSTALLED where to_date(timestamp) >= '2020-07-21'"

try:
    cs.execute(role)
    SQL_Query4 = cs.execute(sql4)
    data_ANDROID_INSTALLS = pd.DataFrame(SQL_Query4, columns=['CONTEXT_DEVICE_ID','TIMESTAMP','DATA_TS','CONTEXT_LOCALE'])
finally:
    cs.close()
ctx4.close()

#%% IOS install data

# Specify the DB and SCHEMA

ctx5 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD",
    schema="DIMENSION_HOP_IOS_PRODUCTION"
    )
cs = ctx5.cursor()

role = 'use role PROD_ADMIN;'

#sql5 = "select distinct CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_INSTALLED where to_date(timestamp) >= '2020-07-21' and split_part(context_locale, '-', 2) in ('NZ', 'AU')"
sql5 = "select distinct CONTEXT_DEVICE_ID, CAST(TIMESTAMP AS date), TIMESTAMP, CONTEXT_LOCALE from APPLICATION_INSTALLED where to_date(timestamp) >= '2020-07-21'"

try:
    cs.execute(role)
    SQL_Query5 = cs.execute(sql5)
    data_IOS_INSTALLS = pd.DataFrame(SQL_Query5, columns=['CONTEXT_DEVICE_ID','TIMESTAMP','DATA_TS','CONTEXT_LOCALE'])
finally:
    cs.close()
ctx5.close()

#%%

ctx6 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD"
    )
cs = ctx6.cursor()

role = 'use role PROD_ADMIN;'

#sql6 = "select to_date(r.timestamp) as dt, case when r.context_platform = 'Android' then 'android' when r.context_platform = 'IPhonePlayer' then 'ios' end as platform, r.user_id, r.price_in_usd from SEGMENT_EVENTS_PROD.DIMENSION_HOP_BACKENDSERVICE_PRODUCTION.REVENUE_IAP r join (select distinct id, context_device_id, context_locale from SEGMENT_EVENTS_PROD.DIMENSION_HOP_ANDROID_PRODUCTION.USERS where split_part(context_locale, '-', 2) in ('NZ', 'AU') union all select distinct id, context_device_id, context_locale from SEGMENT_EVENTS_PROD.DIMENSION_HOP_IOS_PRODUCTION.USERS where split_part(context_locale, '-', 2) in ('NZ', 'AU')) u on r.user_id = u.id"
sql6 = "select to_date(r.timestamp) as dt, case when r.context_platform = 'Android' then 'android' when r.context_platform = 'IPhonePlayer' then 'ios' end as platform, r.user_id, r.price_in_usd, u.context_locale from SEGMENT_EVENTS_PROD.DIMENSION_HOP_BACKENDSERVICE_PRODUCTION.REVENUE_IAP r join (select distinct id, context_device_id, context_locale from SEGMENT_EVENTS_PROD.DIMENSION_HOP_ANDROID_PRODUCTION.USERS union all select distinct id, context_device_id, context_locale from SEGMENT_EVENTS_PROD.DIMENSION_HOP_IOS_PRODUCTION.USERS) u on r.user_id = u.id"


try:
    cs.execute(role)
    SQL_Query6 = cs.execute(sql6)
    #data_USER_REV = pd.DataFrame(SQL_Query6, columns=['USER_ID','TIMESTAMP', 'PRICE_IN_USD'])
    data_USER_REV = pd.DataFrame(SQL_Query6, columns=['TIMESTAMP', 'PLATFORM','USER_ID','PRICE_IN_USD','CONTEXT_LOCALE'])
finally:
    cs.close()
ctx6.close()

#%%

ctx7 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD"
    )
cs = ctx7.cursor()

#sql = 'show roles;'

role = 'use role PROD_ADMIN;'

sql7 = 'select CONTEXT_DEVICE_ID, USER_ID, PLATFORM FROM SEGMENT_EVENTS_PROD.DH_REPORTING_PROD.USERID_DIM'

try:
    cs.execute(role)
    SQL_Query7 = cs.execute(sql7)
    data_USER_DEVICE_MAP = pd.DataFrame(SQL_Query7, columns=['CONTEXT_DEVICE_ID','USER_ID','PLATFORM'])
finally:
    cs.close()
ctx7.close()

#%%

ctx8 = snowflake.connector.connect(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="SEGMENT_EVENTS_PROD"
    )
cs = ctx8.cursor()

#sql = 'show roles;'

role = 'use role PROD_ADMIN;'

sql8 = 'SELECT CAST(revenue."DT" AS date) AS "revenue.dt_date", COALESCE(SUM(revenue."USD" ), 0) AS "revenue.usd" FROM SEGMENT_EVENTS_PROD.DH_REPORTING_PROD.REVENUE AS revenue GROUP BY TO_DATE(revenue."DT" )'

try:
    cs.execute(role)
    SQL_Query8 = cs.execute(sql8)
    data_REV_ByDay = pd.DataFrame(SQL_Query8, columns=['SESSION_DATE','PRICE_IN_USD'])
finally:
    cs.close()
ctx8.close()

#%% handles the country codes in the dataset at the outset

droid_opens_cl = data_ANDROID_OPENS["CONTEXT_LOCALE"].str.split("-", n = 1, expand = True)
data_ANDROID_OPENS["LANGUAGE"] = droid_opens_cl[0] 
data_ANDROID_OPENS["COUNTRY"] = droid_opens_cl[1] 
data_ANDROID_OPENS.drop(columns =["CONTEXT_LOCALE"], inplace = True)

apple_opens_cl = data_IOS_OPENS["CONTEXT_LOCALE"].str.split("-", n = 1, expand = True)
data_IOS_OPENS["LANGUAGE"] = apple_opens_cl[0] 
data_IOS_OPENS["COUNTRY"] = apple_opens_cl[1] 
data_IOS_OPENS.drop(columns =["CONTEXT_LOCALE"], inplace = True)

droid_installs_cl = data_ANDROID_INSTALLS["CONTEXT_LOCALE"].str.split("-", n = 1, expand = True)
data_ANDROID_INSTALLS["LANGUAGE"] = droid_installs_cl[0] 
data_ANDROID_INSTALLS["COUNTRY"] = droid_installs_cl[1] 
data_ANDROID_INSTALLS.drop(columns =["CONTEXT_LOCALE"], inplace = True)

apple_installs_cl = data_IOS_INSTALLS["CONTEXT_LOCALE"].str.split("-", n = 1, expand = True)
data_IOS_INSTALLS["LANGUAGE"] = apple_installs_cl[0] 
data_IOS_INSTALLS["COUNTRY"] = apple_installs_cl[1] 
data_IOS_INSTALLS.drop(columns =["CONTEXT_LOCALE"], inplace = True)

rev_cl = data_USER_REV["CONTEXT_LOCALE"].str.split("-", n = 1, expand = True)
data_USER_REV["LANGUAGE"] = rev_cl[0] 
data_USER_REV["COUNTRY"] = rev_cl[1] 
data_USER_REV.drop(columns =["CONTEXT_LOCALE"], inplace = True)

#%% concats the android/ios opens data

df_android_opens = pd.DataFrame(data_ANDROID_OPENS, columns=['USER_ID','CONTEXT_DEVICE_ID','TIMESTAMP','COUNTRY'])
df_android_opens.rename(columns = {'TIMESTAMP':'SESSION_DATE'}, inplace = True)
#df_android_opens = df_android_opens.dropna(subset=['USER_ID']) # in case you want to drop opens without user IDs
df_android_opens = df_android_opens.drop_duplicates().reset_index(drop=True)


df_ios_opens = pd.DataFrame(data_IOS_OPENS, columns=['USER_ID','CONTEXT_DEVICE_ID','TIMESTAMP','COUNTRY'])
df_ios_opens.rename(columns = {'TIMESTAMP':'SESSION_DATE'}, inplace = True)
#df_ios_opens = df_ios_opens.dropna(subset=['USER_ID']) # in case you want to drop opens without user IDs
df_ios_opens = df_ios_opens.drop_duplicates().reset_index(drop=True)

df_sessions = pd.concat([df_android_opens,df_ios_opens])
df_sessions = df_sessions.drop_duplicates(keep='first')
df_users_comp = pd.merge(df_sessions,data_USER_DEVICE_MAP, on=['USER_ID','CONTEXT_DEVICE_ID'])

#%% this df was made to confirm DAU calculation was same as a complex SQL pull

df_dau_test = df_users_comp.groupby('SESSION_DATE')['USER_ID'].nunique().reset_index()

#%% concats the android/ios install data & merge with session data

df_android_installs = pd.DataFrame(data_ANDROID_INSTALLS, columns=['CONTEXT_DEVICE_ID','TIMESTAMP','COUNTRY'])
df_android_installs.rename(columns = {'TIMESTAMP':'INSTALL_DATE'}, inplace = True)
df_android_installs = df_android_installs.drop_duplicates().reset_index(drop=True)

df_ios_installs = pd.DataFrame(data_IOS_INSTALLS, columns=['CONTEXT_DEVICE_ID','TIMESTAMP','COUNTRY'])
df_ios_installs.rename(columns = {'TIMESTAMP':'INSTALL_DATE'}, inplace = True)
df_ios_installs = df_ios_installs.drop_duplicates().reset_index(drop=True)

df_installs = pd.concat([df_android_installs,df_ios_installs])
df_installs = df_installs.sort_values(by='INSTALL_DATE',ascending=True).reset_index(drop=True)
df_installs = df_installs.drop_duplicates(subset=['CONTEXT_DEVICE_ID'], keep='first').reset_index(drop=True)
df_installs_comp = pd.merge(df_installs,data_USER_DEVICE_MAP, on=['CONTEXT_DEVICE_ID'])

#%% merges the install data with the session data

df_user_sessions = pd.merge(df_users_comp,df_installs_comp, on=['USER_ID','CONTEXT_DEVICE_ID','PLATFORM','COUNTRY'])
df_user_sessions = df_user_sessions.drop_duplicates(keep='first').reset_index(drop=True)

#%% establishes the user revenue data & merges with user device data             

df_user_rev = data_USER_REV.copy()
df_user_rev = df_user_rev.drop_duplicates().reset_index(drop=True)
df_user_rev.rename(columns = {'TIMESTAMP':'SESSION_DATE'}, inplace = True)
df_user_rev['PURCHACE_DATE'] = df_user_rev['SESSION_DATE']

df_user_rev = pd.merge(df_user_rev,data_USER_DEVICE_MAP, on=['USER_ID','PLATFORM'])

#%% merges user revenue data on the user session data

df_users = pd.merge(df_user_sessions,df_user_rev, how='left', on=['USER_ID','CONTEXT_DEVICE_ID','SESSION_DATE','PLATFORM','COUNTRY'])
#df_users = df_users.drop_duplicates().reset_index(drop=True)
df_users_rev_check = df_users.groupby(['SESSION_DATE'])['PRICE_IN_USD'].sum().reset_index()

df_users['PRICE_IN_USD'].fillna(0, inplace=True)

#%% clean/prep marketing data

df_user_marketing = pd.DataFrame(data_USER_MARKETING, columns=['CUSTOM_USER_IDS','CAMPAIGN_NAME','SUB_CAMPAIGN_NAME','SESSION_DATE','PARTNER'])
#df_user_marketing.rename(columns = {'CUSTOM_USER_IDS':'USER_ID', 'EVENT_TIMESTAMP':'AD_TIMESTAMP'}, inplace = True) 
df_user_marketing.rename(columns = {'CUSTOM_USER_IDS':'USER_ID'}, inplace = True) 
df_user_marketing['USER_ID'] = df_user_marketing['USER_ID'].str.strip('"')
df_user_marketing['CAMPAIGN_NAME'] = df_user_marketing['CAMPAIGN_NAME'].str.upper()
df_user_marketing['SUB_CAMPAIGN_NAME'] = df_user_marketing['SUB_CAMPAIGN_NAME'].str.lower()

#%% explores campaign data

ad_camps = df_user_marketing['CAMPAIGN_NAME'].dropna().unique().tolist()
ad_camps.remove('CampaignName'.upper())
camp_sums = []

def count_str_in_col(c,s):
    appearances = sum(c == s)
    return appearances

for i in range(len(ad_camps)):
    sumval = count_str_in_col(df_user_marketing['CAMPAIGN_NAME'],ad_camps[i])
    camp_sums.append(sumval)

camp_dict = {'CAMPAIGN_NAME':ad_camps, 'PURCHACES':camp_sums}

df_purchaces_per_campaign = pd.DataFrame(camp_dict)

# plots
'''
plt.bar(df_purchaces_per_campaign['CAMPAIGN_NAME'],df_purchaces_per_campaign['PURCHACES'])
plt.title('Purchaces by campaign')
plt.show()
'''

#%% counts by SUB_CAMPAIGN_NAME

sub_camps = df_user_marketing['SUB_CAMPAIGN_NAME'].dropna().unique().tolist()
sub_camps.remove('AdGroupName'.lower())
sub_camp_sums = []

for i in range(len(sub_camps)):
    sumval = count_str_in_col(df_user_marketing['SUB_CAMPAIGN_NAME'],sub_camps[i])
    sub_camp_sums.append(sumval)

sub_camp_dict = {'SUB_CAMPAIGN_NAME':sub_camps, 'PURCHACES':sub_camp_sums}

df_purchaces_per_sub_camp = pd.DataFrame(sub_camp_dict)

# plots
'''
plt.bar(df_purchaces_per_sub_camp['SUB_CAMPAIGN_NAME'],df_purchaces_per_sub_camp['PURCHACES'])
plt.title('Purchaces by sub-campaign')
plt.show()
'''

#%% chose a campaign or subcampaign to calculate the LTV for, or analyse all the data not based on marketing

country_list = ['AU','NZ']

df_campaign = df_users.copy()

#df_campaign = df_user_marketing[df_user_marketing['SUB_CAMPAIGN_NAME'] == 'Mobile Gamers - Action']
#df_campaign = df_user_marketing[df_user_marketing['USER_ID'].notna()]

campaign_start_date = df_campaign.SESSION_DATE.min()
campaign_latest_date = df_campaign.SESSION_DATE.max()

if campaign_latest_date > date.today():
    campaign_latest_date = date.today()

numdays = campaign_latest_date - campaign_start_date

numdays = numdays / pd.Timedelta(1, unit='D')
campaign_dates = pd.date_range(campaign_start_date, periods = numdays+1).tolist()
for i in range(len(campaign_dates)):
    campaign_dates[i] = datetime.date(campaign_dates[i])

COHORT_by_date = pd.DataFrame()
COHORT_by_date['SESSION_DATE'] = campaign_dates

COHORT_by_player_day = pd.DataFrame()

campaign_size = df_campaign.USER_ID.nunique() # establishes the size of the marketing cohort
df_campaign = df_campaign.dropna(subset=['USER_ID']) # drops the rows without a USER_ID to match with the user/session/revenue data

campaign_users = df_campaign.USER_ID.unique() # establishes which users are in the marketing cohort

df_campaign_users = df_users[df_users['USER_ID'].isin(campaign_users)] # makes a dataframe with the users from df_users that match the marketing data 
# df_users

#%%

df_campaign_cohort = df_users[df_users['COUNTRY'].isin(country_list)] # df_users.copy() <- in case you want to pull all the users
#df_campaign_cohort = pd.merge(df_campaign_users,df_campaign, on=['USER_ID','SESSION_DATE']).reset_index(drop=True) # merges the user and marketing data into a single dataframe
df_campaign_cohort = df_campaign_cohort.sort_values(by='SESSION_DATE',ascending=True).reset_index(drop=True) # sorts by session_date
df_campaign_cohort['PLAYER_DAY'] = df_campaign_cohort['SESSION_DATE'] - df_campaign_cohort['INSTALL_DATE'] # calculates player day
df_campaign_cohort = df_campaign_cohort[df_campaign_cohort['PLAYER_DAY'] >= pd.Timedelta('0 days')] # exclude data where the number of player days is negative
df_campaign_cohort = df_campaign_cohort[df_campaign_cohort['PLAYER_DAY'] <= pd.Timedelta(str(numdays)+' days')] # excude data that calculates more days than possible to have been played
#df_campaign_cohort = df_campaign_cohort.drop_duplicates() 

#%% calculations for REVENUE, DAU, RetentionRate, and ARPDAU, based on chosen data

numdays = df_campaign_cohort['PLAYER_DAY'].max().days # establishes the max days played in the cohort
player_day_list = [timedelta(days=x) for x in range(0,numdays+1)] # makes a timedelta list from 0 to max days
COHORT_by_player_day['PLAYER_DAY'] = player_day_list # creates a player days column to map the data onto

REV_by_player_day = df_campaign_cohort.groupby(['PLAYER_DAY'])['PRICE_IN_USD'].sum().reset_index()
REV_by_player_day.rename(columns = {'PRICE_IN_USD':'REVENUE'}, inplace = True)
DAU_by_player_day = df_campaign_cohort.groupby(['PLAYER_DAY'])['USER_ID'].nunique().reset_index()
DAU_by_player_day.rename(columns = {'USER_ID':'DAU'}, inplace = True)
    
COHORT_by_player_day = pd.merge(COHORT_by_player_day,DAU_by_player_day,how='left',on='PLAYER_DAY')
COHORT_by_player_day = pd.merge(COHORT_by_player_day,REV_by_player_day,how='left',on='PLAYER_DAY')
for i in range(len(COHORT_by_player_day)):
    COHORT_by_player_day['PLAYER_DAY'][i] = COHORT_by_player_day['PLAYER_DAY'][i].days
COHORT_by_player_day['RetentionRate'] = COHORT_by_player_day['DAU']/COHORT_by_player_day['DAU'][0]
COHORT_by_player_day['ARPDAU'] = COHORT_by_player_day['REVENUE']/COHORT_by_player_day['DAU']
COHORT_by_player_day['LTV'] = COHORT_by_player_day['RetentionRate']*COHORT_by_player_day['ARPDAU']
COHORT_by_player_day['LTV'] = COHORT_by_player_day['LTV'].fillna(0)
COHORT_by_player_day = COHORT_by_player_day.fillna(0)
COHORT_by_player_day_CUMLTV = sum(COHORT_by_player_day['LTV'])
#COHORT_by_player_day['pandas_SMA_3'] = COHORT_by_player_day.iloc[:,1].rolling(window=2).mean()

REV_by_date = df_campaign_cohort.groupby(['SESSION_DATE'])['PRICE_IN_USD'].sum().reset_index()
REV_by_date.rename(columns = {'PRICE_IN_USD':'REVENUE'}, inplace = True)
DAU_by_date = df_campaign_cohort.groupby(['SESSION_DATE'])['USER_ID'].nunique().reset_index()
DAU_by_date.rename(columns = {'USER_ID':'DAU'}, inplace = True)

COHORT_by_date = pd.merge(COHORT_by_date,REV_by_date, how='outer', on='SESSION_DATE')
COHORT_by_date = pd.merge(COHORT_by_date,DAU_by_date, how='outer', on='SESSION_DATE')
COHORT_by_date['RetentionRate'] = COHORT_by_date['DAU']/campaign_size
COHORT_by_date['ARPDAU'] = COHORT_by_date['REVENUE']/COHORT_by_date['DAU']
COHORT_by_date['LTV'] = COHORT_by_date['RetentionRate']*COHORT_by_date['ARPDAU']
COHORT_by_date['LTV'] = COHORT_by_date['LTV'].fillna(0)
COHORT_by_date.loc[:,COHORT_by_date.columns!='SESSION_DATE'] = COHORT_by_date.loc[:,COHORT_by_date.columns!='SESSION_DATE'].fillna(0)
COHORT_by_date_CUMLTV = sum(COHORT_by_date['LTV'])
COHORT_by_date = COHORT_by_date.sort_values(by=['SESSION_DATE']).reset_index(drop=True)

    
#%%

DAU_COHORT_ARIMA = pm.auto_arima(np.log(COHORT_by_player_day['DAU']+1), start_p=1, start_q=1,
                             #test='adf',
                             max_p=12, max_q=12,
                             m=7,
                             seasonal=True,
                             start_P=0,
                             D=1,
                             trace=True,
                             error_action='ignore',  
                             suppress_warnings=True, 
                             stepwise=True)

print(DAU_COHORT_ARIMA.summary())

#DAU_COHORT_ARIMA.plot_diagnostics(figsize=(7,5))

# Forecast
n_periods = 181-len(COHORT_by_player_day['DAU'])
fc, confint = DAU_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
index_of_fc = np.arange(len(COHORT_by_player_day['DAU']), len(COHORT_by_player_day['DAU'])+n_periods)

# make series for plotting purpose
fc_series = pd.Series(fc, index=index_of_fc)
lower_series = pd.Series(confint[:, 0], index=index_of_fc)
upper_series = pd.Series(confint[:, 1], index=index_of_fc)

fc_series = np.exp(fc_series)-1

# Plot
plt.plot(COHORT_by_player_day['DAU'])
plt.plot(fc_series, color='darkgreen')
plt.fill_between(lower_series.index, 
                 lower_series, 
                 upper_series, 
                 color='k', alpha=.15)

plt.title("DAU Forcast")
plt.show()

DAU_COHORT_pred = fc_series

print(COHORT_by_player_day['DAU'])
print(DAU_COHORT_pred)

#%%

REV_COHORT_ARIMA = pm.auto_arima(np.log(COHORT_by_player_day['REVENUE']+1), start_p=1, start_q=1,
                             #test='adf',
                             max_p=12, max_q=12,
                             m=7,
                             seasonal=True,
                             start_P=0,
                             D=1,
                             trace=True,
                             error_action='ignore',  
                             suppress_warnings=True, 
                             stepwise=True)

print(REV_COHORT_ARIMA.summary())

#REV_COHORT_ARIMA.plot_diagnostics(figsize=(7,5))

# Forecast
n_periods = 181-len(COHORT_by_player_day['REVENUE'])
fc, confint = REV_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
index_of_fc = np.arange(len(COHORT_by_player_day['REVENUE']), len(COHORT_by_player_day['REVENUE'])+n_periods)

# make series for plotting purpose
fc_series = pd.Series(fc, index=index_of_fc)
lower_series = pd.Series(confint[:, 0], index=index_of_fc)
upper_series = pd.Series(confint[:, 1], index=index_of_fc)

fc_series = np.exp(fc_series)-1

# Plot
plt.plot(COHORT_by_player_day['REVENUE'])
plt.plot(fc_series, color='darkgreen')
plt.fill_between(lower_series.index, 
                 lower_series, 
                 upper_series, 
                 color='k', alpha=.15)

plt.title("REVENUE Forcast")
plt.show()

REV_COHORT_pred = fc_series

print(COHORT_by_player_day['REVENUE'])
print(REV_COHORT_pred)

#%% make forcast DF

# find the date of the last day in the ts
ts_last_day = int(COHORT_by_player_day['PLAYER_DAY'][-1:])

forcast_days = []
for i in range(len(DAU_COHORT_pred+1)):
    forcast_days.append(ts_last_day + i + 1)

DAU_COHORT_pred = DAU_COHORT_pred.reset_index(drop=True)
REV_COHORT_pred = REV_COHORT_pred.reset_index(drop=True)

df_forcast = pd.DataFrame({'PLAYER_DAY':forcast_days, 'DAU':DAU_COHORT_pred, 'REVENUE': REV_COHORT_pred})

#%% concat forcast to the existing data

COHORT_AU_NZ_LTV_ByPlayerDay = pd.concat([COHORT_by_player_day, df_forcast],ignore_index=True)

COHORT_AU_NZ_LTV_ByPlayerDay['DAU'][COHORT_AU_NZ_LTV_ByPlayerDay['DAU'] < 0] = 0
COHORT_AU_NZ_LTV_ByPlayerDay['REVENUE'][COHORT_AU_NZ_LTV_ByPlayerDay['REVENUE'] < 0] = 0

COHORT_AU_NZ_LTV_ByPlayerDay['RetentionRate'] = COHORT_AU_NZ_LTV_ByPlayerDay['DAU'] / COHORT_AU_NZ_LTV_ByPlayerDay['DAU'][0]
COHORT_AU_NZ_LTV_ByPlayerDay['ARPDAU'] = COHORT_AU_NZ_LTV_ByPlayerDay['REVENUE'] / COHORT_AU_NZ_LTV_ByPlayerDay['DAU']
COHORT_AU_NZ_LTV_ByPlayerDay['LTV'] = COHORT_AU_NZ_LTV_ByPlayerDay['RetentionRate'] * COHORT_AU_NZ_LTV_ByPlayerDay['ARPDAU']

COHORT_AU_NZ_LTV_ByPlayerDay['ARPDAU'] = COHORT_AU_NZ_LTV_ByPlayerDay['ARPDAU'].fillna(0)
COHORT_AU_NZ_LTV_ByPlayerDay['LTV'] = COHORT_AU_NZ_LTV_ByPlayerDay['LTV'].fillna(0)

#%%

AU_NZ_ByPlayerWeek = COHORT_AU_NZ_LTV_ByPlayerDay.copy()

AU_NZ_ByPlayerWeek = AU_NZ_ByPlayerWeek.groupby(AU_NZ_ByPlayerWeek.index // 7).sum()
AU_NZ_ByPlayerWeek = AU_NZ_ByPlayerWeek.reset_index()
AU_NZ_ByPlayerWeek.rename(columns = {'index':'PLAYER_WEEK','DAU':'WAU'}, inplace = True)
AU_NZ_ByPlayerWeek['RetentionRate'] = AU_NZ_ByPlayerWeek['WAU'] / AU_NZ_ByPlayerWeek['WAU'][0]
AU_NZ_ByPlayerWeek['ARPWAU'] = AU_NZ_ByPlayerWeek['REVENUE'] / AU_NZ_ByPlayerWeek['WAU']
AU_NZ_ByPlayerWeek = AU_NZ_ByPlayerWeek.drop(columns=['PLAYER_DAY'])
#AU_NZ_ByPlayerWeek = AU_NZ_ByPlayerWeek.drop(columns=['PLAYER_DAY','ARPDAU','LTV'])
#AU_NZ_ByPlayerWeek['LTV'] = AU_NZ_ByPlayerWeek['RetentionRate'] * AU_NZ_ByPlayerWeek['ARPWAU']

AU_NZ_ByPlayerWeek['ARPWAU'] = AU_NZ_ByPlayerWeek['ARPWAU'].fillna(0)
#AU_NZ_ByPlayerWeek['LTV'] = AU_NZ_ByPlayerWeek['LTV'].fillna(0)

#%% drops selected dataframe into snowflake

from sqlalchemy import Table, MetaData #, Column, Integer,  ForeignKeyConstraint
'''
registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

engine = create_engine(URL(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="PROD_GAMES",
    schema="DIMENSION_HOP",
    role='PROD_ADMIN'
))

connection = engine.connect()

connection.execute(DropTable(Table('LTV_AU_BYDATE', MetaData())))
connection.execute(DropTable(Table('LTV_AU_BYPLAYERDAY', MetaData())))
connection.execute(DropTable(Table('LTV_AU_BYPLAYERWEEK', MetaData())))

connection.close()
engine.dispose()
'''
#%% inserts selected dataframe into snowflake

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')

engine = create_engine(URL(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="PROD_GAMES",
    schema="DIMENSION_HOP",
    role='PROD_ADMIN'
))

connection = engine.connect()

connection.execute(DropTable(Table('LTV_AU_BYDATE', MetaData())))
connection.execute(DropTable(Table('LTV_AU_BYPLAYERDAY', MetaData())))
connection.execute(DropTable(Table('LTV_AU_BYPLAYERWEEK', MetaData())))

COHORT_by_date.to_sql('LTV_AU_BYDATE', con=engine, index=False)
COHORT_AU_NZ_LTV_ByPlayerDay.to_sql('LTV_AU_BYPLAYERDAY', con=engine, index=False)
AU_NZ_ByPlayerWeek.to_sql('LTV_AU_BYPLAYERWEEK', con=engine, index=False)
connection.close()
engine.dispose()

#%% sub campaign calculations

subcamp_names = []
subcamp = {}

def sub_camp_cohort(cohort):
    return df_user_marketing[df_user_marketing['SUB_CAMPAIGN_NAME'] == cohort]
    
for i in range(len(sub_camps)):
    df_name = 'sub-campaign:' + sub_camps[i].replace(" ", "_")
    subcamp_names.append(df_name)
    subcamp[df_name] = sub_camp_cohort(sub_camps[i])
    
for i in range(len(subcamp_names)):
    subcamp[subcamp_names[i]]['Campaign_Size'] = len(subcamp[subcamp_names[i]].USER_ID)

camp_users = {}

def match_users(camp):
    return df_users[df_users['USER_ID'].isin(campaign_users)] # makes a dataframe with the users from df_users that match the marketing data    

for i in range(len(subcamp_names)):
    subcamp[subcamp_names[i]] = subcamp[subcamp_names[i]].dropna(subset=['USER_ID']) # drops the rows without a USER_ID to match with the user/session/revenue data
    campaign_users = subcamp[subcamp_names[i]].USER_ID.unique() # establishes which users are in the marketing cohort
    camp_users[subcamp_names[i]] = match_users(subcamp[subcamp_names[i]])

camps_MAPPED = {}

def merge_dataframes(frame1,frame2):
    return pd.merge(frame1,frame2, on=['USER_ID','SESSION_DATE']).reset_index(drop=True) # merges the user and marketing data into a single dataframe

for i in range(len(subcamp_names)):
    camps_MAPPED[subcamp_names[i]] = merge_dataframes(camp_users[subcamp_names[i]],subcamp[subcamp_names[i]])

for i in range(len(subcamp_names)):
    camps_MAPPED[subcamp_names[i]].rename(columns = {'PRICE_IN_USD':'REVENUE'}, inplace = True)

sub_camps_by_date = {}

def new_dataframe(data):
    frame = pd.DataFrame(data, columns=['SESSION_DATE'])
    return frame

for i in range(len(subcamp_names)):
    if len(camps_MAPPED[subcamp_names[i]].SESSION_DATE) > 0:
        campaign_start_date = camps_MAPPED[subcamp_names[i]].SESSION_DATE.min()
        campaign_latest_date = camps_MAPPED[subcamp_names[i]].SESSION_DATE.max()
        numdays = campaign_latest_date - campaign_start_date
        numperiods = numdays / pd.Timedelta(1, unit='D')
        campaign_dates = pd.date_range(campaign_start_date, periods = numperiods+1).tolist()
        for d in range(len(campaign_dates)):
            campaign_dates[d] = datetime.date(campaign_dates[d])
        sub_camps_by_date[subcamp_names[i]] = new_dataframe(campaign_dates)
    else:
        sub_camps_by_date[subcamp_names[i]] = new_dataframe([None])

for i in range(len(subcamp_names)):
    camps_MAPPED[subcamp_names[i]] = camps_MAPPED[subcamp_names[i]].sort_values(by='SESSION_DATE',ascending=True).reset_index(drop=True) # sorts by session_date
    camps_MAPPED[subcamp_names[i]]['PLAYER_DAY'] = camps_MAPPED[subcamp_names[i]]['SESSION_DATE'] - camps_MAPPED[subcamp_names[i]]['INSTALL_DATE'] # calculates player day
    REV_by_date = camps_MAPPED[subcamp_names[i]].groupby(['SESSION_DATE'])['REVENUE'].sum().reset_index()
    DAU_by_date = camps_MAPPED[subcamp_names[i]].groupby(['SESSION_DATE'])['USER_ID'].nunique().reset_index()
    DAU_by_date.rename(columns = {'USER_ID':'DAU'}, inplace = True)
    sub_camps_by_date[subcamp_names[i]] = pd.merge(sub_camps_by_date[subcamp_names[i]],REV_by_date, how='outer', on='SESSION_DATE')
    sub_camps_by_date[subcamp_names[i]] = pd.merge(sub_camps_by_date[subcamp_names[i]],DAU_by_date, how='outer', on='SESSION_DATE')  
    sub_camps_by_date[subcamp_names[i]]['Campaign_Size'] = camps_MAPPED[subcamp_names[i]].Campaign_Size

for i in range(len(subcamp_names)):
    sub_camps_by_date[subcamp_names[i]]['RetentionRate'] = sub_camps_by_date[subcamp_names[i]]['DAU']/sub_camps_by_date[subcamp_names[i]]['Campaign_Size']
    sub_camps_by_date[subcamp_names[i]]['ARPDAU'] = sub_camps_by_date[subcamp_names[i]]['REVENUE']/sub_camps_by_date[subcamp_names[i]]['DAU']
    sub_camps_by_date[subcamp_names[i]]['LTV'] = sub_camps_by_date[subcamp_names[i]]['RetentionRate']*sub_camps_by_date[subcamp_names[i]]['ARPDAU']
    sub_camps_by_date[subcamp_names[i]]['LTV'] = sub_camps_by_date[subcamp_names[i]]['LTV'].fillna(0)
    sub_camps_by_date[subcamp_names[i]].loc[:,sub_camps_by_date[subcamp_names[i]].columns!='SESSION_DATE'] = sub_camps_by_date[subcamp_names[i]].loc[:,sub_camps_by_date[subcamp_names[i]].columns!='SESSION_DATE'].fillna(0)
    sub_camps_by_date[subcamp_names[i]] = sub_camps_by_date[subcamp_names[i]].sort_values(by=['SESSION_DATE']).reset_index(drop=True)

#%% marketing campaign calculations

camp_names = []
mark_camp = {}

def mark_camp_cohort(cohort):
    return df_user_marketing[df_user_marketing['CAMPAIGN_NAME'] == cohort]
    
for i in range(len(ad_camps)):
    df_name = 'campaign:' + ad_camps[i].replace(" ", "_")
    camp_names.append(df_name)
    mark_camp[df_name] = mark_camp_cohort(ad_camps[i])
    
for i in range(len(camp_names)):
    mark_camp[camp_names[i]]['Campaign_Size'] = len(mark_camp[camp_names[i]].USER_ID)

mark_camp_users = {}

def match_users(camp):
    return df_users[df_users['USER_ID'].isin(campaign_users)] # makes a dataframe with the users from df_users that match the marketing data    

for i in range(len(camp_names)):
    mark_camp[camp_names[i]] = mark_camp[camp_names[i]].dropna(subset=['USER_ID']) # drops the rows without a USER_ID to match with the user/session/revenue data
    campaign_users = mark_camp[camp_names[i]].USER_ID.unique() # establishes which users are in the marketing cohort
    mark_camp_users[camp_names[i]] = match_users(mark_camp[camp_names[i]])

mark_camps_MAPPED = {}

def merge_dataframes(frame1,frame2):
    return pd.merge(frame1,frame2, on=['USER_ID','SESSION_DATE']).reset_index(drop=True) # merges the user and marketing data into a single dataframe

for i in range(len(camp_names)):
    mark_camps_MAPPED[camp_names[i]] = merge_dataframes(mark_camp_users[camp_names[i]],mark_camp[camp_names[i]])

for i in range(len(camp_names)):
    mark_camps_MAPPED[camp_names[i]].rename(columns = {'PRICE_IN_USD':'REVENUE'}, inplace = True)

mark_camps_by_date = {}

def new_dataframe(data):
    frame = pd.DataFrame(data, columns=['SESSION_DATE'])
    return frame

for i in range(len(camp_names)):
    if len(mark_camps_MAPPED[camp_names[i]].SESSION_DATE) > 0:
        campaign_start_date = mark_camps_MAPPED[camp_names[i]].SESSION_DATE.min()
        campaign_latest_date = mark_camps_MAPPED[camp_names[i]].SESSION_DATE.max()
        numdays = campaign_latest_date - campaign_start_date
        numperiods = numdays / pd.Timedelta(1, unit='D')
        campaign_dates = pd.date_range(campaign_start_date, periods = numperiods+1).tolist()
        for d in range(len(campaign_dates)):
            campaign_dates[d] = datetime.date(campaign_dates[d])
        mark_camps_by_date[camp_names[i]] = new_dataframe(campaign_dates)
    else:
        mark_camps_by_date[camp_names[i]] = new_dataframe([None])

for i in range(len(camp_names)):
    mark_camps_MAPPED[camp_names[i]] = mark_camps_MAPPED[camp_names[i]].sort_values(by='SESSION_DATE',ascending=True).reset_index(drop=True) # sorts by session_date
    mark_camps_MAPPED[camp_names[i]]['PLAYER_DAY'] = mark_camps_MAPPED[camp_names[i]]['SESSION_DATE'] - mark_camps_MAPPED[camp_names[i]]['INSTALL_DATE'] # calculates player day
    REV_by_date = mark_camps_MAPPED[camp_names[i]].groupby(['SESSION_DATE'])['REVENUE'].sum().reset_index()
    DAU_by_date = mark_camps_MAPPED[camp_names[i]].groupby(['SESSION_DATE'])['USER_ID'].nunique().reset_index()
    DAU_by_date.rename(columns = {'USER_ID':'DAU'}, inplace = True)
    mark_camps_by_date[camp_names[i]] = pd.merge(mark_camps_by_date[camp_names[i]],REV_by_date, how='outer', on='SESSION_DATE')
    mark_camps_by_date[camp_names[i]] = pd.merge(mark_camps_by_date[camp_names[i]],DAU_by_date, how='outer', on='SESSION_DATE')  
    mark_camps_by_date[camp_names[i]]['Campaign_Size'] = mark_camps_MAPPED[camp_names[i]].USER_ID.nunique()

for i in range(len(camp_names)):
    mark_camps_by_date[camp_names[i]]['RetentionRate'] = mark_camps_by_date[camp_names[i]]['DAU']/mark_camps_by_date[camp_names[i]]['Campaign_Size']
    mark_camps_by_date[camp_names[i]]['ARPDAU'] = mark_camps_by_date[camp_names[i]]['REVENUE']/mark_camps_by_date[camp_names[i]]['DAU']
    mark_camps_by_date[camp_names[i]]['LTV'] = mark_camps_by_date[camp_names[i]]['RetentionRate']*mark_camps_by_date[camp_names[i]]['ARPDAU']
    mark_camps_by_date[camp_names[i]]['LTV'] = mark_camps_by_date[camp_names[i]]['LTV'].fillna(0)
    mark_camps_by_date[camp_names[i]].loc[:,mark_camps_by_date[camp_names[i]].columns!='SESSION_DATE'] = mark_camps_by_date[camp_names[i]].loc[:,mark_camps_by_date[camp_names[i]].columns!='SESSION_DATE'].fillna(0)
    mark_camps_by_date[camp_names[i]] = mark_camps_by_date[camp_names[i]].sort_values(by=['SESSION_DATE']).reset_index(drop=True)
    
#%%

sub_camp_dau_preds = {}
sub_camp_rev_preds = {}

for i in range(len(subcamp_names)):
    if len(sub_camps_by_date[subcamp_names[i]]['DAU']) >= 10:
        dau_data = sub_camps_by_date[subcamp_names[i]]['DAU']
        DAU_COHORT_ARIMA = pm.auto_arima(np.log(dau_data+1),start_p=1, start_q=1,
                                     max_p=5, max_q=5,
                                     start_P=0,
                                     d=1, D=1, trace=True,
                                     error_action='warn',  # don't want to know if an order does not work
                                     suppress_warnings=True,  # don't want convergence warnings
                                     stepwise=True)  # set to stepwise
        # Forecast
        n_periods = 181-len(dau_data)
        fc, confint = DAU_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
        index_of_fc = np.arange(len(dau_data), len(dau_data)+n_periods)
        
        # make series for plotting purpose
        fc_series = pd.Series(fc, index=index_of_fc)
        lower_series = pd.Series(confint[:, 0], index=index_of_fc)
        upper_series = pd.Series(confint[:, 1], index=index_of_fc)
            
        fc_series = np.exp(fc_series)-1
            
        sub_camp_dau_preds[subcamp_names[i]] = fc_series
    else:
        sub_camp_dau_preds[subcamp_names[i]] = np.zeros(181-len(sub_camps_by_date[subcamp_names[i]]['DAU']))
        
for i in range(len(subcamp_names)):
    if len(sub_camps_by_date[subcamp_names[i]]['REVENUE']) >= 10:
        rev_data = sub_camps_by_date[subcamp_names[i]]['REVENUE']
        DAU_COHORT_ARIMA = pm.auto_arima(np.log(rev_data+1),start_p=1, start_q=1,
                                     max_p=5, max_q=5,
                                     start_P=0,
                                     d=1, D=1, trace=True,
                                     error_action='warn',  # don't want to know if an order does not work
                                     suppress_warnings=True,  # don't want convergence warnings
                                     stepwise=True)  # set to stepwise
        # Forecast
        n_periods = 181-len(rev_data)
        fc, confint = DAU_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
        index_of_fc = np.arange(len(rev_data), len(rev_data)+n_periods)
        
        # make series for plotting purpose
        fc_series = pd.Series(fc, index=index_of_fc)
        lower_series = pd.Series(confint[:, 0], index=index_of_fc)
        upper_series = pd.Series(confint[:, 1], index=index_of_fc)
            
        fc_series = np.exp(fc_series)-1
            
        sub_camp_rev_preds[subcamp_names[i]] = fc_series
    else:
        sub_camp_rev_preds[subcamp_names[i]] = np.zeros(181-len(sub_camps_by_date[subcamp_names[i]]['REVENUE']))
        
sub_camp_preds = {}

for i in range(len(subcamp_names)):
    known_dau = pd.Series(sub_camps_by_date[subcamp_names[i]].DAU)
    pred_dau = pd.Series(sub_camp_dau_preds[subcamp_names[i]])
    dau_fc = known_dau.append(pred_dau)
    sub_camp_preds[subcamp_names[i]] = pd.DataFrame(dau_fc, columns=['DAU'])
    known_rev = pd.Series(sub_camps_by_date[subcamp_names[i]].REVENUE)
    pred_rev = pd.Series(sub_camp_dau_preds[subcamp_names[i]])
    rev_fc = known_rev.append(pred_rev)
    sub_camp_preds[subcamp_names[i]]['REVENUE'] = rev_fc

for i in range(len(subcamp_names)):
    start_date = sub_camps_by_date[subcamp_names[i]]['SESSION_DATE'][0]
    print(start_date)
    if start_date != None:
        start_date = sub_camps_by_date[subcamp_names[i]]['SESSION_DATE'][0]
    else:
        start_date = datetime.today()
        start_date = start_date.date()
        print(start_date)
    dates_index = pd.date_range(start = start_date, periods=181, freq='D').tolist()
    for d in range(len(dates_index)):
        dates_index[d] = datetime.date(dates_index[d])
    dates_index = pd.Series(dates_index)
    sub_camp_preds[subcamp_names[i]] = sub_camp_preds[subcamp_names[i]].set_index(dates_index)
    sub_camp_preds[subcamp_names[i]].index.name = 'SESSION_DATE'
    sub_camp_preds[subcamp_names[i]] = sub_camp_preds[subcamp_names[i]].reset_index()

for i in range(len(subcamp_names)):
    sub_camp_preds[subcamp_names[i]]['Campaign_Size'] = sub_camps_by_date[subcamp_names[i]]['Campaign_Size']
    sub_camp_preds[subcamp_names[i]]['Campaign_Size'] = sub_camp_preds[subcamp_names[i]]['Campaign_Size'].fillna(sub_camp_preds[subcamp_names[i]]['Campaign_Size'][0])
    
for i in range(len(subcamp_names)):
    sub_camp_preds[subcamp_names[i]]['RetentionRate'] = sub_camp_preds[subcamp_names[i]]['DAU']/sub_camp_preds[subcamp_names[i]]['Campaign_Size']
    sub_camp_preds[subcamp_names[i]]['RetentionRate'][sub_camp_preds[subcamp_names[i]]['RetentionRate'] < 0] = 0
    sub_camp_preds[subcamp_names[i]]['DAU'][sub_camp_preds[subcamp_names[i]]['DAU'] < .1] = 0
    sub_camp_preds[subcamp_names[i]]['REVENUE'][sub_camp_preds[subcamp_names[i]]['REVENUE'] < .1] = 0
    sub_camp_preds[subcamp_names[i]]['ARPDAU'] = sub_camp_preds[subcamp_names[i]]['REVENUE']/sub_camp_preds[subcamp_names[i]]['DAU']
    sub_camp_preds[subcamp_names[i]]['LTV'] = sub_camp_preds[subcamp_names[i]]['RetentionRate']*sub_camp_preds[subcamp_names[i]]['ARPDAU']
    sub_camp_preds[subcamp_names[i]]['LTV'] = sub_camp_preds[subcamp_names[i]]['LTV'].fillna(0)
    sub_camp_preds[subcamp_names[i]].loc[:,sub_camp_preds[subcamp_names[i]].columns!='SESSION_DATE'] = sub_camp_preds[subcamp_names[i]].loc[:,sub_camp_preds[subcamp_names[i]].columns!='SESSION_DATE'].fillna(0)

#%%

start_date = df_users.SESSION_DATE.min()
end_date = datetime.today() + timedelta(days=180)
end_date = end_date.date()

sub_camps_dates_index = pd.date_range(start = start_date, end = end_date, freq='D').tolist()

for d in range(len(sub_camps_dates_index)):
        sub_camps_dates_index[d] = datetime.date(sub_camps_dates_index[d])
sub_camps_dates_index = pd.Series(sub_camps_dates_index)

df_sub_camp_daus = pd.DataFrame(sub_camps_dates_index, columns=['DATE'])
sub_camp_daus = {}
for i in range(len(subcamp_names)):
    sub_camp_preds[subcamp_names[i]] = sub_camp_preds[subcamp_names[i]].set_index('SESSION_DATE')
    sub_camp_preds[subcamp_names[i]].index.name = 'DATE'
    sub_camp_daus[subcamp_names[i]] = pd.DataFrame(sub_camp_preds[subcamp_names[i]].DAU)
    sub_camp_daus[subcamp_names[i]] = sub_camp_daus[subcamp_names[i]].reset_index() 
for i in range(len(subcamp_names)):
    col_name = subcamp_names[i].replace('_',' ').split(':')
    sub_camp_daus[subcamp_names[i]].rename(columns = {'DAU':col_name[1]}, inplace = True)
for i in range(len(subcamp_names)):
    df_sub_camp_daus = pd.merge(df_sub_camp_daus,sub_camp_daus[subcamp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)
    
df_sub_camp_revs = pd.DataFrame(sub_camps_dates_index, columns=['DATE'])
sub_camp_revs = {}
for i in range(len(subcamp_names)):
    sub_camp_revs[subcamp_names[i]] = pd.DataFrame(sub_camp_preds[subcamp_names[i]].REVENUE)
    sub_camp_revs[subcamp_names[i]] = sub_camp_revs[subcamp_names[i]].reset_index() 
for i in range(len(subcamp_names)):
    col_name = subcamp_names[i].replace('_',' ').split(':')
    sub_camp_revs[subcamp_names[i]].rename(columns = {'REVENUE':col_name[1]}, inplace = True)
for i in range(len(subcamp_names)):
    df_sub_camp_revs = pd.merge(df_sub_camp_revs,sub_camp_revs[subcamp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)
    
df_sub_camp_LTVs = pd.DataFrame(sub_camps_dates_index, columns=['DATE'])
sub_camp_ltvs = {}
for i in range(len(subcamp_names)):
    sub_camp_ltvs[subcamp_names[i]] = pd.DataFrame(sub_camp_preds[subcamp_names[i]].LTV)
    sub_camp_ltvs[subcamp_names[i]] = sub_camp_ltvs[subcamp_names[i]].reset_index() 
for i in range(len(subcamp_names)):
    col_name = subcamp_names[i].replace('_',' ').split(':')
    sub_camp_ltvs[subcamp_names[i]].rename(columns = {'LTV':col_name[1]}, inplace = True)
for i in range(len(subcamp_names)):
    df_sub_camp_LTVs = pd.merge(df_sub_camp_LTVs,sub_camp_ltvs[subcamp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)

#%%

mark_camp_dau_preds = {}
mark_camp_rev_preds = {}

for i in range(len(camp_names)):
    if len(mark_camps_by_date[camp_names[i]]['DAU']) >= 10:
        dau_data = mark_camps_by_date[camp_names[i]]['DAU']
        print('<<<<<<<<<<<<<<'+str(i)+'>>>>>>>>>>>>>>>>>>')
        DAU_COHORT_ARIMA = pm.auto_arima(np.log(dau_data+1), start_p=1, start_q=1,
                                     max_p=5, max_q=5,
                                     start_P=0,
                                     d=1, D=1, trace=True,
                                     error_action='warn',  # don't want to know if an order does not work
                                     suppress_warnings=True,  # don't want convergence warnings
                                     stepwise=True)  # set to stepwise
        # Forecast
        n_periods = 181-len(dau_data)
        fc, confint = DAU_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
        index_of_fc = np.arange(len(dau_data), len(dau_data)+n_periods)
        
        # make series for plotting purpose
        fc_series = pd.Series(fc, index=index_of_fc)
        lower_series = pd.Series(confint[:, 0], index=index_of_fc)
        upper_series = pd.Series(confint[:, 1], index=index_of_fc)
            
        fc_series = np.exp(fc_series)-1
            
        mark_camp_dau_preds[camp_names[i]] = fc_series
    else:
        mark_camp_dau_preds[camp_names[i]] = np.zeros(181-len(mark_camps_by_date[camp_names[i]]['DAU']))
        
for i in range(len(camp_names)):
    if len(mark_camps_by_date[camp_names[i]]['REVENUE']) >= 10:
        rev_data = mark_camps_by_date[camp_names[i]]['REVENUE']
        DAU_COHORT_ARIMA = pm.auto_arima(np.log(rev_data+1), start_p=1, start_q=1,
                                     max_p=5, max_q=5,
                                     start_P=0,
                                     d=1, D=1, trace=True,
                                     error_action='warn',  # don't want to know if an order does not work
                                     suppress_warnings=True,  # don't want convergence warnings
                                     stepwise=True)  # set to stepwise
        # Forecast
        n_periods = 181-len(rev_data)
        fc, confint = DAU_COHORT_ARIMA.predict(n_periods=n_periods, return_conf_int=True)
        index_of_fc = np.arange(len(rev_data), len(rev_data)+n_periods)
        
        # make series for plotting purpose
        fc_series = pd.Series(fc, index=index_of_fc)
        lower_series = pd.Series(confint[:, 0], index=index_of_fc)
        upper_series = pd.Series(confint[:, 1], index=index_of_fc)
            
        fc_series = np.exp(fc_series)-1
            
        mark_camp_rev_preds[camp_names[i]] = fc_series
    else:
        mark_camp_rev_preds[camp_names[i]] = np.zeros(181-len(mark_camps_by_date[camp_names[i]]['REVENUE']))
        
mark_camp_preds = {}

for i in range(len(camp_names)):
    known_dau = pd.Series(mark_camps_by_date[camp_names[i]].DAU)
    pred_dau = pd.Series(mark_camp_dau_preds[camp_names[i]])
    dau_fc = known_dau.append(pred_dau)
    mark_camp_preds[camp_names[i]] = pd.DataFrame(dau_fc, columns=['DAU'])
    known_rev = pd.Series(mark_camps_by_date[camp_names[i]].REVENUE)
    pred_rev = pd.Series(mark_camp_dau_preds[camp_names[i]])
    rev_fc = known_rev.append(pred_rev)
    mark_camp_preds[camp_names[i]]['REVENUE'] = rev_fc

for i in range(len(camp_names)):
    start_date = mark_camps_by_date[camp_names[i]]['SESSION_DATE'][0]
    print(start_date)
    if start_date != None:
        start_date = mark_camps_by_date[camp_names[i]]['SESSION_DATE'][0]
    else:
        start_date = datetime.today()
        start_date = start_date.date()
        print(start_date)
    dates_index = pd.date_range(start = start_date, periods=181, freq='D').tolist()
    for d in range(len(dates_index)):
        dates_index[d] = datetime.date(dates_index[d])
    dates_index = pd.Series(dates_index)
    mark_camp_preds[camp_names[i]] = mark_camp_preds[camp_names[i]].set_index(dates_index)
    mark_camp_preds[camp_names[i]].index.name = 'SESSION_DATE'
    mark_camp_preds[camp_names[i]] = mark_camp_preds[camp_names[i]].reset_index()

for i in range(len(camp_names)):
    mark_camp_preds[camp_names[i]]['Campaign_Size'] = mark_camps_by_date[camp_names[i]]['Campaign_Size']
    mark_camp_preds[camp_names[i]]['Campaign_Size'] = mark_camp_preds[camp_names[i]]['Campaign_Size'].fillna(mark_camp_preds[camp_names[i]]['Campaign_Size'][0])
    
for i in range(len(camp_names)):
    mark_camp_preds[camp_names[i]]['RetentionRate'] = mark_camp_preds[camp_names[i]]['DAU']/mark_camp_preds[camp_names[i]]['Campaign_Size']
    mark_camp_preds[camp_names[i]]['RetentionRate'][mark_camp_preds[camp_names[i]]['RetentionRate'] < 0] = 0
    mark_camp_preds[camp_names[i]]['DAU'][mark_camp_preds[camp_names[i]]['DAU'] < .1] = 0
    mark_camp_preds[camp_names[i]]['REVENUE'][mark_camp_preds[camp_names[i]]['REVENUE'] < .1] = 0
    mark_camp_preds[camp_names[i]]['ARPDAU'] = mark_camp_preds[camp_names[i]]['REVENUE']/mark_camp_preds[camp_names[i]]['DAU']
    mark_camp_preds[camp_names[i]]['LTV'] = mark_camp_preds[camp_names[i]]['RetentionRate']*mark_camp_preds[camp_names[i]]['ARPDAU']
    mark_camp_preds[camp_names[i]]['LTV'] = mark_camp_preds[camp_names[i]]['LTV'].fillna(0)
    mark_camp_preds[camp_names[i]].loc[:,mark_camp_preds[camp_names[i]].columns!='SESSION_DATE'] = mark_camp_preds[camp_names[i]].loc[:,mark_camp_preds[camp_names[i]].columns!='SESSION_DATE'].fillna(0)

#%%

start_date = df_users.SESSION_DATE.min()
end_date = datetime.today() + timedelta(days=180)
end_date = end_date.date()

mark_camps_dates_index = pd.date_range(start = start_date, end = end_date, freq='D').tolist()

for d in range(len(mark_camps_dates_index)):
        mark_camps_dates_index[d] = datetime.date(mark_camps_dates_index[d])
mark_camps_dates_index = pd.Series(mark_camps_dates_index)

df_mark_camp_daus = pd.DataFrame(mark_camps_dates_index, columns=['DATE'])
mark_camp_daus = {}
for i in range(len(camp_names)):
    mark_camp_preds[camp_names[i]] = mark_camp_preds[camp_names[i]].set_index('SESSION_DATE')
    mark_camp_preds[camp_names[i]].index.name = 'DATE'
    mark_camp_daus[camp_names[i]] = pd.DataFrame(mark_camp_preds[camp_names[i]].DAU)
    mark_camp_daus[camp_names[i]] = mark_camp_daus[camp_names[i]].reset_index() 
for i in range(len(camp_names)):
    col_name = camp_names[i].strip('campaign:')
    mark_camp_daus[camp_names[i]].rename(columns = {'DAU':col_name}, inplace = True)
for i in range(len(camp_names)):
    df_mark_camp_daus = pd.merge(df_mark_camp_daus,mark_camp_daus[camp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)
    
df_mark_camp_revs = pd.DataFrame(mark_camps_dates_index, columns=['DATE'])
mark_camp_revs = {}
for i in range(len(camp_names)):
    mark_camp_revs[camp_names[i]] = pd.DataFrame(mark_camp_preds[camp_names[i]].REVENUE)
    mark_camp_revs[camp_names[i]] = mark_camp_revs[camp_names[i]].reset_index() 
for i in range(len(camp_names)):
    col_name = camp_names[i].strip('campaign:')
    mark_camp_revs[camp_names[i]].rename(columns = {'REVENUE':col_name}, inplace = True)
for i in range(len(camp_names)):
    df_mark_camp_revs = pd.merge(df_mark_camp_revs,mark_camp_revs[camp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)
    
df_mark_camp_LTVs = pd.DataFrame(mark_camps_dates_index, columns=['DATE'])
mark_camp_ltvs = {}
for i in range(len(camp_names)):
    mark_camp_ltvs[camp_names[i]] = pd.DataFrame(mark_camp_preds[camp_names[i]].LTV)
    mark_camp_ltvs[camp_names[i]] = mark_camp_ltvs[camp_names[i]].reset_index() 
for i in range(len(camp_names)):
    col_name = camp_names[i].strip('campaign:')
    mark_camp_ltvs[camp_names[i]].rename(columns = {'LTV':col_name}, inplace = True)
for i in range(len(camp_names)):
    df_mark_camp_LTVs = pd.merge(df_mark_camp_LTVs,mark_camp_ltvs[camp_names[i]],how = 'left', on=['DATE']).reset_index(drop=True)

#%%
'''
engine = create_engine(URL(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="PROD_GAMES",
    schema="DIMENSION_HOP",
    role='PROD_ADMIN'
))

connection = engine.connect()

connection.execute(DropTable(Table('SUB_CAMPAIGN_DAU', MetaData())))
connection.execute(DropTable(Table('SUB_CAMPAIGN_REVENUE', MetaData())))
connection.execute(DropTable(Table('SUB_CAMPAIGN_LTV', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_DAU', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_REVENUE', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_LTV', MetaData())))

connection.close()
engine.dispose()
'''
 
#%%

engine = create_engine(URL(
    user = email,
    password = secret_password,
    account = 'ted_as.us-east-1',
    authenticator = 'https://tw.okta.com/app/snowflake/exkm4az8mcVI9DJdV0x7/sso/saml',
    database="PROD_GAMES",
    schema="DIMENSION_HOP",
    role='PROD_ADMIN'
))

connection = engine.connect()

connection.execute(DropTable(Table('SUB_CAMPAIGN_DAU', MetaData())))
connection.execute(DropTable(Table('SUB_CAMPAIGN_REVENUE', MetaData())))
connection.execute(DropTable(Table('SUB_CAMPAIGN_LTV', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_DAU', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_REVENUE', MetaData())))
connection.execute(DropTable(Table('CAMPAIGN_LTV', MetaData())))

df_sub_camp_daus.to_sql('SUB_CAMPAIGN_DAU', con=engine, index=False)
df_sub_camp_revs.to_sql('SUB_CAMPAIGN_REVENUE', con=engine, index=False)
df_sub_camp_LTVs.to_sql('SUB_CAMPAIGN_LTV', con=engine, index=False)
df_mark_camp_daus.to_sql('CAMPAIGN_DAU', con=engine, index=False)
df_mark_camp_revs.to_sql('CAMPAIGN_REVENUE', con=engine, index=False)
df_mark_camp_LTVs.to_sql('CAMPAIGN_LTV', con=engine, index=False)

connection.close()
engine.dispose()

#%% prints done when finished

print("<<<<<<<<<<< Done >>>>>>>>>>>>")
