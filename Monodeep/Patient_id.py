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

mydb = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
        SELECT up.full_name as PatientName, up.phone as Phone, up.patient_id as patient_id, l.name as "Clinic Name"
FROM nivaancare_production.user_profile up
LEFT JOIN user_profile_locations upl ON upl.userprofile_id = up.id
LEFT JOIN location l ON upl.location_id = l.id

"""
# df = pd.read_sql_query(sql_query,mycursor)
df = pd.read_sql_query(sql_query,mydb)



print(df)


df.to_csv('PatientId.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '164jbgNEcozqfZo3jpA68AhmtBX3u6c8VNdop1SVF28k' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'PatientId.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:AH")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

