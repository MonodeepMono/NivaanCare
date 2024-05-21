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
 up.full_name AS patientName,
    up.phone AS Mobile,
  JSON_UNQUOTE(JSON_EXTRACT(pal.extra_info, '$.logs[0].id')) AS id,
  JSON_UNQUOTE(JSON_EXTRACT(pal.extra_info, '$.logs[0].value')) AS Value,
  CONVERT_TZ(JSON_UNQUOTE(JSON_EXTRACT(pal.extra_info, '$.logs[0].created_at')), '+00:00', '+05:30') AS created_at_ist,
    DATE_FORMAT(CONVERT_TZ(JSON_UNQUOTE(JSON_EXTRACT(pal.extra_info, '$.logs[0].created_at')), '+00:00', '+05:30'), '%Y-%m-%d') AS created_at_date
FROM 
    nivaancare_production.user_profile up
LEFT JOIN 
    patient_activity_log pal ON up.id = pal.user_id
WHERE Value IS NOT NULL;
"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)
print(df_LEAD)

# # Parse JSON string to dictionary
# df_LEAD['Date_FINAL'] = json.loads(df_LEAD["extra_info"][0])
# print(df_LEAD)
# # created_at = extra_info_dict["created_at"]
# # df_LEAD_V1[DATE_FINAL] = df_LEAD["extra_info"][0]["created_at"]
# # print(df_LEAD_V1)
# # df_LEAD['Rank_Status'] = df_LEAD.groupby(['Mobile'])['Date'].rank("dense", ascending=True)
# # print(df_LEAD)
# # df_LEAD['Date']= df_LEAD["Date"].dt.date
# # df_FINAL = df_LEAD[df_LEAD['Rank_Status']!=1]
# # print(df_FINAL)

df_FINAL_V1  = df_LEAD[['patientName','Mobile','created_at_ist','created_at_date','Value']]
print(df_FINAL_V1)


# df_FINAL_V1.to_csv('pain_score.csv',index = False)

# import gspread
# import csv
# from oauth2client.service_account import ServiceAccountCredentials


# scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

# credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
# gc = gspread.authorize(credentials)
# client = gspread.authorize(credentials)
# spreadsheetId = '1IEwtpCA7PETueJWWbAS8cQsTlxhHSW0lxI08TN9jALA' 

# sheetName = 'pain_score'        # Please set sheet name you want to put the CSV data.
# csvFile = 'pain_score.csv'  # Please set the filename and path of csv file.
# sh = client.open_by_key(spreadsheetId)
# sh.values_clear("'pain_score'!A2:E")
# sh.values_update(sheetName,
#                  params={'valueInputOption': 'USER_ENTERED'},
#                  body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



