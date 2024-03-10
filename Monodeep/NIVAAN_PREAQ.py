import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


# mydb = mysql.connector.connect(
#   host="stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com",
#   user="monodeep.saha",
#   password="u5eOX37kNPh13Jdhgfv",
#   database="nivaancare_production"
# )
# mycursor = mydb.cursor()
# conn = mycursor.execute

# sql_query = """
#   SELECT 
#   ga.campaign_date as Date,
#   SUM(ga.clicks) as Clicks ,
#   SUM(ga.impressions) as Impressions ,
#   SUM(cost) as Amount 
#   FROM nivaancare_production.GOOGLE_ADS ga
#   WHERE ga.campaign_date >= '2024-03-01'
#   GROUP BY ga.campaign_date;
  
   
# """
# df_DATA = pd.read_sql_query(sql_query,mydb)
# print(df_DATA)



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
    DATE_FORMAT(url.created_time, '%Y-%m-%d') as Date,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Google%' THEN url.mobile END) as GoogleFormLead,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Google%' AND url.lead_new_status like '%Junk%' THEN url.mobile END) as Google_JunkLead,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Google%' AND url.lead_new_status like '%L6 - New Lead%' THEN url.mobile END) as L6_New_Lead,
    COUNT(DISTINCT CASE WHEN url.lead_sub_source like '%Google%' AND url.channel_name = 'Call' THEN url.mobile END) as GoogleCallLead,
    COUNT(DISTINCT CASE WHEN url.lead_sub_source like '%Google%' AND url.channel_name = 'Call' AND url.lead_new_status like '%Junk%' THEN url.mobile END) as GoogleCallJunkLead,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Facebook%' THEN url.mobile END) as FacebookLead,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Facebook%' AND url.lead_new_status LIKE '%Junk%' THEN url.mobile END) as FacebookJunkLead,
    COUNT(DISTINCT CASE WHEN url.utm_source like '%Facebook%' AND url.lead_new_status LIKE '%L6 - New Lead%' THEN url.mobile END) as FacebookJunkLead
    
FROM 
    nivaancare_production.user_registration_lead url
WHERE 
    DATE_FORMAT(url.created_time, '%Y-%m-%d') BETWEEN '2024-02-01' AND '2024-03-09'
GROUP BY 
    DATE_FORMAT(url.created_time, '%Y-%m-%d');

"""
df_LEAD = pd.read_sql_query(sql_query_prod,mydb_prod)
print(df_LEAD)



df_LEAD.to_csv('Nivaan_LEAD.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1eni28VWN7hluEUImtROeHL9YoIUyw5Jxh196PgxZ4ic' 

sheetName = 'LEAD'        # Please set sheet name you want to put the CSV data.
csvFile = 'Nivaan_LEAD.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'LEAD'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
