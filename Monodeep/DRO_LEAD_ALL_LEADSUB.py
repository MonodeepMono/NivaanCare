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
  url.lead_sub_source AS lead_sub_source,
  url.dro as DRO_Name,
  url.mobile as mobile,
  url.hospital_clinic_dispensary_name as dispensary_name
FROM 
  nivaancare_production.user_registration_lead url  ;


"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)
df_FINAL  = df_LEAD[['Date'	,'mobile',	'DRO_Name',	'lead_sub_source','dispensary_name']]
print(df_FINAL)


df_FINAL.to_csv('LEAD_DRO_SUB.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'LEAD_DRO_SUB'        # Please set sheet name you want to put the CSV data.
csvFile = 'LEAD_DRO_SUB.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'LEAD_DRO_SUB'!A2:E")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



