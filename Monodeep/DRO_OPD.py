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
    CONVERT_TZ(url.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time,
    up.patient_id AS PatientId,
    up.full_name AS PatientName,
    up.phone AS Mobile,
    url.owner AS owner,
    url.lead_source AS LeadSource,
    url.lead_sub_source AS lead_sub_source,
    url.lead_new_status AS lead_new_status,
    c.amount AS "Amount",
    CASE 
        WHEN CONVERT(JSON_UNQUOTE(JSON_EXTRACT(owner, '$.name')) USING utf8) = 'Himanshu H' THEN 'Himanshu_H'
        ELSE CONVERT(JSON_UNQUOTE(JSON_EXTRACT(owner, '$.name')) USING utf8) 
    END AS owner_name
FROM 
    nivaancare_production.consultation c
LEFT JOIN 
    user_profile up ON c.user_id = up.id
LEFT JOIN 
    user_registration_lead url ON up.phone = url.mobile
where
c.consult_datetime >= '2024-03-01' AND c.patient_attendance_status ='show' AND url.lead_source = 'DRO'
"""
df = pd.read_sql_query(sql_query_prod,mydb_prod)
df['consult_date']= df["consult_datetime"].dt.date
df['Rank_Status'] = df.groupby(['Mobile'])['modified_time'].rank("dense", ascending=False)



df_csv = df[["PatientId", "PatientName", "Mobile", 'consult_datetime', "consult_date", "owner_name", "LeadSource", 
             "lead_new_status",'lead_sub_source','Amount','Rank_Status']]

print(df_csv)


df_csv.to_csv('OPD.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'OPD'        # Please set sheet name you want to put the CSV data.
csvFile = 'OPD.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'OPD'!A2:K")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

