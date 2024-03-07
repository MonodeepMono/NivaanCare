import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


mydb = mysql.connector.connect(
  host="elevatenow-prod.ccu6uuygqnkc.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="aw102070eZiRey",
  database="elevatenow_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
SELECT 
    id, 
    CONVERT_TZ(a.created_time, 'UTC', 'Asia/Kolkata') AS created_time_ist, 
    CONVERT_TZ(a.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time_ist,
    a.full_name, 
    a.extracted_phone, 
    a.lead_type, 
    a.lead_status, 
    a.pitch_completed, 
    a.new_lead_status, 
    age, 
    a.lead_qualifier, 
    a.formula_1, 
    a.owner_name, 
    a.lead_quality1, 
    lead_source, 
    utm_source, 
    adsetid, 
    utmcontent,
    utm_campaign 
FROM 
    user_lead_status a 
WHERE 
    a.modified_time > '2024-02-01'
GROUP BY 
    a.extracted_phone, 
    a.lead_status, 
    a.substatus1, 
    a.new_lead_status;
"""
df_DATA = pd.read_sql_query(sql_query,mydb)
print(df_DATA)

df_DATA.to_csv('Elevate_now_new.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '18_SbM3TAscyhXhQpw2uPD_uKYtrUmjO_iy9-xcWQvYU' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'Elevate_now_new.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
