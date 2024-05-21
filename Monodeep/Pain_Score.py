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
sheet = client.open_by_key("1IEwtpCA7PETueJWWbAS8cQsTlxhHSW0lxI08TN9jALA") # Open by key the spreadhseet
#sheet.share
tab = sheet.worksheet('DATA_A')
calls = pd.DataFrame(tab.get_all_records())
calls['Rank_Status'] = calls.groupby(['Mobile'])['created_at_ist'].rank("dense", ascending=True)
print(calls)




calls.to_csv('pain_score_new.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1IEwtpCA7PETueJWWbAS8cQsTlxhHSW0lxI08TN9jALA' 

sheetName = 'pS'        # Please set sheet name you want to put the CSV data.
csvFile = 'pain_score_new.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'pS'!A2:E")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})


