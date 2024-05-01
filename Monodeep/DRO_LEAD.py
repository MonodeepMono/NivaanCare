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
  #  DATE_FORMAT(url.created_time, '%Y-%m-%d') as Date,
  CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata') AS created_time, 
    CONVERT_TZ(url.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time,
   url.lead_new_status as lead_new_status,
   url.lead_sub_source AS lead_sub_source,
      url.lead_source AS LeadSource,
    url.hospital_clinic_dispensary_name as hospital_clinic_dispensary_name,
    url.dro_lead_type as LeadType,
  url.mobile as mobile
  FROM 
    nivaancare_production.user_registration_lead url
  LEFT JOIN 
            user_profile up ON url.mobile = up.phone
WHERE 
    DATE_FORMAT(url.created_time, '%Y-%m-%d') >= '2024-03-01' AND DATE_FORMAT(url.created_time, '%Y-%m-%d') <= DATE_SUB(CURDATE(), INTERVAL 1 DAY);
  
   

"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)
df_LEAD['Date']= df_LEAD["created_time"].dt.date
df_LEAD['Month']= df_LEAD["created_time"].dt.month



df_LEAD['Rank_Status'] = df_LEAD.groupby(['mobile'])['modified_time'].rank("dense", ascending=False)
df_FINAL  = df_LEAD[['Date'	,'created_time',	'modified_time',	'LeadSource',	'lead_new_status',	'mobile',	'Rank_Status','lead_sub_source','Month','hospital_clinic_dispensary_name','LeadType']]
print(df_FINAL)


df_FINAL.to_csv('Nivaan_LEAD_DRO.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'DRO_LEAD'        # Please set sheet name you want to put the CSV data.
csvFile = 'Nivaan_LEAD_DRO.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'DRO_LEAD'!A2:M")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



