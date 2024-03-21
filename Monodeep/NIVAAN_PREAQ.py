import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector



##########LEADS##################3

mydb_prod = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor_prod = mydb_prod.cursor()
conn_prod = mycursor_prod.execute

sql_query_prod = """
  SELECT 
   DATE_FORMAT(url.created_time, '%Y-%m-%d') as Date,
   url.created_time as created_time,
   url.modified_time  as modified_time,
   url.utm_source as UTM_SOURCE,
   url.lead_new_status as lead_new_status,
   url.lead_sub_source AS lead_sub_source,
   url.channel_name  AS channel_name,
  url.mobile as mobile,
  url.utm_campaign as utm_campaign
  FROM 
    nivaancare_production.user_registration_lead url
WHERE 
    DATE_FORMAT(url.created_time, '%Y-%m-%d') >= '2024-03-01' and DATE_FORMAT(url.created_time, '%Y-%m-%d') <=  '2024-03-20';
  
   

"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)


df_LEAD['Rank_Status'] = df_LEAD.groupby(['mobile'])['modified_time'].rank("dense", ascending=False)
df_FINAL  = df_LEAD[['Date'	,'created_time',	'modified_time',	'UTM_SOURCE',	'lead_new_status',	'mobile',	'Rank_Status','lead_sub_source','channel_name','utm_campaign']]
print(df_FINAL)

df_FINAL.to_csv('Nivaan_LEAD.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1eni28VWN7hluEUImtROeHL9YoIUyw5Jxh196PgxZ4ic' 

sheetName = 'LEAD'        # Please set sheet name you want to put the CSV data.
csvFile = 'Nivaan_LEAD.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'LEAD'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
