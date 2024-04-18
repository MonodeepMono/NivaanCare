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
    url.owner AS owner,
    CONVERT_TZ(url.created_time, 'UTC', 'Asia/Kolkata') AS created_time, 
    CONVERT_TZ(url.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time,
    CASE 
        WHEN CONVERT(JSON_UNQUOTE(JSON_EXTRACT(owner, '$.name')) USING utf8) = 'Himanshu H' THEN 'Himanshu_H'
        ELSE CONVERT(JSON_UNQUOTE(JSON_EXTRACT(owner, '$.name')) USING utf8) 
    END AS owner_name,
    url.mobile AS Mobile,
    url.lead_new_status AS lead_new_status
FROM nivaancare_production.user_registration_lead url 
WHERE url.lead_new_status NOT IN ('Junk','Lead Dead - PD') 
    AND DATE_FORMAT(url.created_time, '%Y-%m-%d') >= '2024-04-01' 
    AND DATE_FORMAT(url.created_time, '%Y-%m-%d') <= '2024-04-15';


 
"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)


df_LEAD['Rank_Status'] = df_LEAD.groupby(['Mobile'])['modified_time'].rank("dense", ascending=False)
df_FINAL  = df_LEAD[['Mobile'	,'created_time',	'modified_time',	'owner_name',	'lead_new_status','Rank_Status']]
print(df_FINAL)

df_FINAL.to_csv('LEAD.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1fzooUUKEF1DAwXqEsFjO17c72cYSwo9m5Rytztil_sA' 

sheetName = 'LEAD'        # Please set sheet name you want to put the CSV data.
csvFile = 'LEAD.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'LEAD'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
