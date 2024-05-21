import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


mydb_prod = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor_prod = mydb_prod.cursor()
conn_prod = mycursor_prod.execute

sql_query = """
  SELECT 
  DATE_FORMAT(fa.date_start  , '%Y-%m-%d') as Date,
    SUM(fa.link_click) as Clicks ,
  SUM(fa.impressions) as Impressions ,
  SUM(fa.spend) as Amount 
  FROM nivaancare_production.facebook_ads fa
 WHERE DATE_FORMAT(fa.date_start, '%Y-%m-%d') >= '2024-02-26' AND DATE_FORMAT(fa.date_start, '%Y-%m-%d') <= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
  GROUP BY fa.date_start;
  
   
"""
df_DATA = pd.read_sql_query(sql_query,mydb_prod)

df_Final = df_DATA[['Date','Amount','Impressions','Clicks']]
print(df_Final)

df_Final.to_csv('Nivaan_FB_GADS.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1eni28VWN7hluEUImtROeHL9YoIUyw5Jxh196PgxZ4ic' 

sheetName = 'FB_ADS'        # Please set sheet name you want to put the CSV data.
csvFile = 'Nivaan_FB_GADS.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'FB_ADS'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

