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
    up.patient_id AS PatientId,
    up.full_name AS PatientName,
    up.phone AS Mobile,
    url.owner AS owner,
    url.lead_source AS LeadSource,
    url.lead_new_status AS lead_new_status,
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
c.consult_datetime >= DATE_SUB(NOW(), INTERVAL 32 DAY) AND c.patient_attendance_status ='show'; 

"""
df = pd.read_sql_query(sql_query_prod,mydb_prod)


df_csv = df[["PatientId", "PatientName", "Mobile", "consult_datetime", "owner_name", "LeadSource", 
             "lead_new_status"]]

print(df_csv)


# df_csv.to_csv('EHR.csv',index=False)

# import gspread
# import csv
# from oauth2client.service_account import ServiceAccountCredentials


# scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

# credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
# gc = gspread.authorize(credentials)
# client = gspread.authorize(credentials)
# spreadsheetId = '1ib-_s67SYFFl7jqifXrV8a45h3PvUscsRjzSiQf88zM' 

# sheetName = 'Raw'        # Please set sheet name you want to put the CSV data.
# csvFile = 'EHR.csv'  # Please set the filename and path of csv file.
# sh = client.open_by_key(spreadsheetId)
# sh.values_clear("'Raw'!A2:AG")
# sh.values_update(sheetName,
#                  params={'valueInputOption': 'USER_ENTERED'},
#                  body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

