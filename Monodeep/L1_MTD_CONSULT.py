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
  up.patient_id AS "Patient Id",
  up.full_name AS "Patient Name",
  DATE_FORMAT(CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata'), '%Y-%m-%d') AS Date,
  CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata') AS created_time, 
  CONVERT_TZ(url.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time,
  url.lead_new_status AS lead_new_status,
  url.lead_sub_source AS lead_sub_source,
  url.lead_source AS LeadSource,
  url.lead_id AS LeadId,
  url.lead_new_sub_status AS lead_new_sub_status,
  url.mobile AS mobile
FROM 
  nivaancare_production.user_registration_lead url
LEFT JOIN 
  user_profile up ON url.mobile = up.phone
WHERE 
  url.lead_new_status = 'L1 - MDT Consult Scheduled'
  AND 
  DATE_FORMAT(CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata'), '%Y-%m-%d') >= '2024-04-01' 
  AND DATE_FORMAT(CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata'), '%Y-%m-%d') <= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
  AND url.lead_id NOT IN (
    SELECT 
      DISTINCT(lead_id) 
    FROM 
      nivaancare_production.user_registration_lead 
    WHERE 
      lead_new_sub_status IN ('Test Cases', 'Duplicate Lead')
  );



"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)
print(df_LEAD)
df_LEAD['Date']= df_LEAD["created_time"].dt.date
# df_LEAD['Month']= df_LEAD["created_time"].dt.month

# df_LEAD_V1 = df_LEAD[df_LEAD['lead_new_sub_status'] != 'Duplicate Lead']


df_LEAD['Rank_Status'] = df_LEAD.groupby(['LeadId'])['modified_time'].rank("dense", ascending=False)
df_LEAD_V1 = df_LEAD[df_LEAD['Rank_Status'] == 1]
df_FINAL  = df_LEAD_V1[['Date'	,	'Patient Id','Patient Name', 'LeadSource',	'mobile',	'LeadId','lead_new_status']]
print(df_FINAL)


df_FINAL.to_csv('L1 - MDT Consult Scheduled.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '13XltPEwsFpqTtSId0OwuzPWXS6N0G_Ez7-xomqBtxZc' 

sheetName = 'L1 - MDT Consult Scheduled'        # Please set sheet name you want to put the CSV data.
csvFile = 'L1 - MDT Consult Scheduled.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'L1 - MDT Consult Scheduled'!A2:G")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



