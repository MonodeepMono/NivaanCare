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
SELECT   up.patient_id AS "Patient Id",
    up.full_name AS "Patient Name",
    up.phone AS "Phone",
    ap.full_name AS "PMS_Name"
    FROM nivaancare_production.user_profile up
LEFT JOIN 
    consultation c ON up.id = c.user_id
LEFT JOIN 
    admin_profile ap ON c.consultant_id  = ap.id;
"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)


df_LEAD['Rank_Status'] = df_LEAD.groupby(['Phone'])['PMS_Name'].rank("dense", ascending=False)
# df_LEAD['Rank_OWNER'] = df_LEAD.groupby(['Mobile','owner_name'])['modified_time'].rank("dense", ascending=True)
# df_LEAD['Rank_OWNER'] = df_LEAD.groupby('LeadID')['owner_name'].transform('nunique')
# df_LEAD['Rank_OWNER_NEW'] = df_LEAD.groupby('LeadID')['owner_name'].transform(lambda x: x.ne(x.shift()).cumsum())

# df_LEAD_V1 = df_LEAD[df_LEAD['Rank_Status']!= 1]

df_FINAL  = df_LEAD[['Phone', 'Patient Id'	,'Patient Name',	'PMS_Name',	'Rank_Status']]
print(df_FINAL)

df_FINAL.to_csv('LEAD_PMS.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1hcggPDtHAbEdHdYeVvatF3opcrrcAn7g7R1lbLRZ9m4' 

sheetName = 'PMS'        # Please set sheet name you want to put the CSV data.
csvFile = 'LEAD_PMS.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'PMS'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
