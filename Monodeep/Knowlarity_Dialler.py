import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


mydb_prod = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor_prod = mydb_prod.cursor()
conn_prod = mycursor_prod.execute

sql_query = """
  

SELECT `InOut` ,sr_number ,Caller ,CallerName ,call_date as Date, duration ,credits_deducted ,`action` as Action , extension , destination ,callid ,order_id  FROM nivaancare_production.knowlarity_call_logs kcl;

   
"""
df = pd.read_sql_query(sql_query,mydb_prod)

# df_Final = df_DATA[['Date','Amount','Impressions','Clicks']]
print(df)




df['Call_Status'] = np.where(df["destination"].str.contains("Customer Missed"),"Customer Missed",
                            np.where(df["destination"].str.contains("Agent Missed"),"Agent Missed",
                                    np.where(df["destination"].str.contains("\("),"Connected",
                                            np.where(df["destination"].str.contains("Call Missed"),"Call Missed",
                                                    np.where(df["destination"].str.contains("Did Not Process"),"Did Not Process",
                                                            np.where(df["destination"].str.contains("Welcome Sound"),"Welcome Sound",
                                                                    np.where(df["destination"].str.contains("NA"),"NA","Others")
                                                                    )
                                                            )
                                                    )
                                            )
                                    )
                            )


mapping = {
    "+917042592600" : "Himanshu",
    "+917303556605" : "Harsh",
    "+919990061711" : "Harsh",
    "+917428941888" : "Manshi",
    "+918770723378" : "pradip",
    "+919140774454" : "Fardeen",
    "+919205160333" : "Arpita",
    "+919205712200" : "Tauheed",
    "+917004942077" : "Ankit",
    "+919205784333" : "Ankit",
    "+919205851222" : "Santosh",
    "+919210774344" : "Aditi",
    "+919654856542" : "Roshni",
    "+919871318666" : "Abhishek",
    "+919871403666" : "Sharmeen",
    "+919891618581" : "Kshama",
    "+917905520401" : "Dilshad",
    "+917007780646" : "Akash",
    "+917428474445" : "Akash",
    "+919599292028" : "Harsh_prasad"
}

flag = {
    "Santosh"  : "EE",
    "Tauheed"  : "EE",
    "Abhishek" : "EE",
    "Arpita"   : "EE",
    "Manshi"   : "EE",
    "Sharmeen" : "Care M",
    "Ankit"    : "Care M",
    "Fardeen"  : "Care M",
    "Aditi"    : "EE",
    "Kshama"   : "EE",
    "pradip"   : "Care M",
    "Harsh"    : "EE",
    "Himanshu" : "EE",
    "Roshni"   : "EE",
    "Shaan"    : "EE",
    "Dilshad"  : "EE",
    "Akash"    : "EE",
    "Harsh_prasad"    : "EE"
}

def numbercheck(a):
    try:
        if '(' in a:
            return a.split(" ")[0]
        elif 'Missed' in a:
            return a.split("#")[1]
        else :
            return None   
    except :
        return None
    
df['Agent_Number'] = df["destination"].apply(numbercheck)
df['Agent_Name'] = df["Agent_Number"].map(mapping)
df['Flag'] = df["Agent_Name"].map(flag)
df['Month'] = pd.to_datetime(df['Date']).dt.month
df['Date'] = pd.to_datetime(df['Date']).dt.date
df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
df['In-Out'] = df['InOut']
print(df)

df_new = df[["In-Out", "Date","Month", "duration", "Action", "destination", "callid","Agent_Name","Call_Status","Flag",'Week']]

print(df_new)
df_new.to_csv('Knowlarity_Dialler.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1C_6FFQxOshWmjNiIi7p3C4PHki9EaJELL-BcksXPA04' 

sheetName = 'Dailer_Raw'        # Please set sheet name you want to put the CSV data.
csvFile = 'Knowlarity_Dialler.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'Dailer_Raw'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})













