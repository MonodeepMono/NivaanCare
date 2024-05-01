import warnings
warnings.filterwarnings("ignore")
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
import pandas as pd
import datetime , time
import pandas as pd
from datetime import datetime

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('my-project-2024-414004-60efb95f9e7f.json',
                                                               scope)
client = gspread.authorize(credentials)
sheet = client.open_by_key("1bGxHdtkLojvrCdjsFgezkeTXddVZS3bSXDVqATGpnEk") # Open by key the spreadhseet
#sheet.share
tab = sheet.worksheet('Visit')
calls = pd.DataFrame(tab.get_all_records())

calls['Date'] = pd.to_datetime(calls['Date'], format='%d-%m-%Y')

# df = calls[['Full Name','Scheduled By','Speciality','Date','Nthvisit','Month','Doctor Status On Nivaan What Is The Business Possibility With The Doctor','Visit per day' ]]
print(calls)

calls.to_csv('DRO_REPORT.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'DRO_REPORT.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:AR")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name('my-project-2024-414004-60efb95f9e7f.json',
                                                               scope)
client = gspread.authorize(credentials)
sheet = client.open_by_key("1Rfe91TmokYkvYkayKfOXOkOUsFL8-iPb48hCCBtrj-4") # Open by key the spreadhseet
#sheet.share
tab = sheet.worksheet('Ops View 2nd Half for Count')
VISITS_2 = pd.DataFrame(tab.get_all_records())


# df = calls[['Full Name','Scheduled By','Speciality','Date','Nthvisit','Month','Doctor Status On Nivaan What Is The Business Possibility With The Doctor','Visit per day' ]]
print(VISITS_2)
VISITS_2.to_csv('DRO_REPORT_VISIT.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'RAW_A'        # Please set sheet name you want to put the CSV data.
csvFile = 'DRO_REPORT_VISIT.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW_A'!A2:F")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
