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
  *
  FROM nivaancare_production.GOOGLE_ADS_KEYWORDS gak;
  
   
"""
df_DATA = pd.read_sql_query(sql_query,mydb_prod)
print(df_DATA)

df_DATA.to_csv('G_ADS_KEY.csv', index=False)
import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '133pG7ChW42GKIrjD233S1QC_tB-EVVEDWzBzhIBP0VM' 

sheetName = 'G_ADS_KEY'        # Please set sheet name you want to put the CSV data.
csvFile = 'G_ADS_KEY.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'G_ADS_KEY'!A2:Z")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

