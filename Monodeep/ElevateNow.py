import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


mydb = mysql.connector.connect(
  host="elevatenow-prod.ccu6uuygqnkc.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="aw102070eZiRey",
  database="elevatenow_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
SELECT 
    id, 
    CONVERT_TZ(a.created_time, 'UTC', 'Asia/Kolkata') AS created_time_ist, 
    CONVERT_TZ(a.modified_time, 'UTC', 'Asia/Kolkata') AS modified_time_ist,
    a.full_name, 
    a.extracted_phone, 
    a.lead_type, 
    a.lead_status, 
    a.pitch_completed, 
    a.new_lead_status, 
    age, 
    a.lead_qualifier, 
    a.formula_1, 
    a.owner_name, 
    a.lead_quality1, 
    lead_source, 
    utm_source, 
    adsetid, 
    utmcontent,
    utm_campaign 
FROM 
    user_lead_status a 
GROUP BY 
    a.extracted_phone, 
    a.lead_status, 
    a.substatus1, 
    a.new_lead_status;

"""
df_DATA = pd.read_sql_query(sql_query,mydb)

df_DATA.to_csv('Elevate_now.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials

# Define the OAuth2 scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# Load the credentials from the JSON key file
credentials = ServiceAccountCredentials.from_json_keyfile_name('my-project-2024-414004-60efb95f9e7f.json', scope)

# Authorize the client using the credentials
gc = gspread.authorize(credentials)

# Open the spreadsheet by its ID
spreadsheetId = '18_SbM3TAscyhXhQpw2uPD_uKYtrUmjO_iy9-xcWQvYU'
sh = gc.open_by_key(spreadsheetId)

# Define the sheet name and CSV file path
sheetName = 'RAW'
csvFile = 'Elevate_now.csv'

# Clear existing values in the specified range
sh.values_clear("'RAW'!A:BM")

# Read the CSV file with UTF-8 encoding and update the spreadsheet with its values
with open(csvFile, 'r', encoding='utf-8') as file:
    csv_values = list(csv.reader(file))
    sh.values_update(
        sheetName,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': csv_values}
    )

# Print the data written
print("Data written to the spreadsheet:")
# for row in csv_values:
#     print(row)

# Print the number of rows and columns written
num_rows = len(csv_values)
num_columns = len(csv_values[0]) if csv_values else 0
print(f"Number of rows written: {num_rows}")
print(f"Number of columns written: {num_columns}")
                    