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
  SELECT up.patient_id ,up.phone ,up.created_at ,up.full_name  From nivaancare_production.user_profile up ;

"""
# df = pd.read_sql_query(sql_query,mycursor)
df = pd.read_sql_query(sql_query,mydb)



print(df)


df.to_csv('USER_PROFILE.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1xBYx3jgszEneYHvK8ZLxPRtkwQ7l7aVExWUL-TmK93w' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'USER_PROFILE.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:AH")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

