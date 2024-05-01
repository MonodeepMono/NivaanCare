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
    CONVERT_TZ(c.consult_datetime, 'UTC', 'Asia/Kolkata') AS consult_datetime,
   ap.full_name As Name,
    up.patient_id AS PatientId,
    up.full_name AS PatientName,
    url.lead_source AS LeadSource,
    up.phone AS Mobile
FROM 
    nivaancare_production.consultation c
LEFT JOIN 
    user_profile up ON c.user_id = up.id
LEFT JOIN 
 admin_profile ap ON c.admin_id = ap.id
 LEFT JOIN 
    user_registration_lead url ON up.phone = url.mobile
where
c.consult_datetime >= '2024-04-01' AND c.patient_attendance_status ='show'
"""
df = pd.read_sql_query(sql_query_prod,mydb_prod)
df['consult_date']= df["consult_datetime"].dt.date
filtered_df = df[~df['Name'].str.contains('test')]


df_csv = filtered_df[["Name", "consult_datetime", "consult_date",'PatientId','PatientName','Mobile','LeadSource']]

print(df_csv)


df_csv.to_csv('OPD_ALL.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1JSu5R8_b1PbQudIHjC0LN5a4jaYaZd9phqlbnTuqQBw' 

sheetName = 'OPD'        # Please set sheet name you want to put the CSV data.
csvFile = 'OPD_ALL.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'OPD'!A2:AG")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

