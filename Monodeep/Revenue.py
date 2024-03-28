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
WITH profileanswer AS (
    SELECT 
        user_id,
        MAX(CASE WHEN `key` = 'source' THEN answer END) AS "source"
    FROM 
        profile_answer
    GROUP BY 
        user_id
),
consultation AS (
    SELECT 
        c.user_id,
        MAX(c.title) AS title,
        MAX(CASE WHEN c.title LIKE '%First Consultation%' THEN DATE_ADD(DATE_ADD(c.consult_datetime, INTERVAL 5 HOUR), INTERVAL 30 MINUTE) END) AS consult_datetime,
        MAX(c.payment_mode) AS payment_mode,
        MAX(c.amount) AS TotalGMV,
        MAX(c.payment_with_source) AS payment_with_source,
        MAX(c.admin_id) AS admin_id
    FROM 
        consultation c
    GROUP BY 
        c.user_id, DATE(c.created_at)
)
SELECT 
    up.patient_id AS "Patient Id",
    up.full_name AS "Patient Name",
    up.phone AS "Phone Number",
    ap.full_name AS "PMS",
    consultation.consult_datetime AS "1st PMS Consult Date",
    l.name AS "Centre",
    pa.source AS "Patient source",
    consultation.TotalGMV AS "TotalGMV",
    consultation.payment_mode AS "Payment_Mode",
    consultation.payment_with_source AS "Amount_With"
FROM 
    user_profile up
LEFT JOIN 
    consultation ON up.id = consultation.user_id
LEFT JOIN 
    patient_prescription pp ON up.id = pp.user_id
LEFT JOIN 
    admin_profile ap ON consultation.admin_id = ap.id
LEFT JOIN 
    user_profile_locations upl ON up.id = upl.userprofile_id
LEFT JOIN 
    location l ON upl.location_id = l.id
LEFT JOIN 
    profileanswer pa ON up.id = pa.user_id
WHERE 
    up.full_name NOT LIKE '%Test%'
ORDER BY 
    consultation.consult_datetime DESC;

"""
df = pd.read_sql_query(sql_query_prod,mydb_prod)
df['1st PMS Consult Date'] = pd.to_datetime(df['1st PMS Consult Date']).dt.date


df['Month'] = pd.to_datetime(df['1st PMS Consult Date']).dt.month
data_FEB= df[df['Month'] == 3]
print(data_FEB)
sorted_df = data_FEB.sort_values(by='1st PMS Consult Date')
print("---------SortedDF------------")
print(sorted_df)
sorted_df['Nivaan Share'] = df['TotalGMV'] * 0.7
sorted_df['Percentage'] = (sorted_df['Nivaan Share'] / sorted_df['TotalGMV'])

df_FINAL = sorted_df[["Patient Id",  "Phone Number", "Patient source", "Centre", "Patient Name", "1st PMS Consult Date","PMS",  "TotalGMV","Nivaan Share","Percentage", "Payment_Mode", "Amount_With", "Month"]]

df_FINAL.to_csv('Revenue.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '10umB0e_ZCG-9z6NPiRhBWPFfzc9VjYrjcn4U9a_GE0c' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'Revenue.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:L")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



