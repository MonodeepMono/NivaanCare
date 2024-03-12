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
sheet = client.open_by_key("1heBl3kVZEaBbYeUHauuasCfto8uKZaRg4_MFpqEbt9A") # Open by key the spreadhseet
#sheet.share
tab = sheet.worksheet('Visit')
calls = pd.DataFrame(tab.get_all_records())
df = calls[['Full Name','Scheduled By','Speciality','hot','warm','cold','Date','Nthvisit']]
# Convert 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
df['Nthvisit'] = pd.to_numeric(df['Nthvisit'], errors='coerce')
# df.dropna(subset=['Nthvisit'], inplace=True)

print(df)

def calculate_latest_status(row):
    if row['hot'] == 'Yes' and row['warm'] == 'No' and row['cold'] == 'No':
        return 'hot'
    elif row['hot'] == 'No' and row['warm'] == 'Yes' and row['cold'] == 'No':
        return 'warm'
    elif row['hot'] == 'No' and row['warm'] == 'No' and row['cold'] == 'Yes':
        return 'cold'
    else:
        return 'Unknown'

# Applying the function to create the LatestStatus column
df['LatestStatus'] = df.apply(calculate_latest_status, axis=1)
# Group by Doctor and calculate Ageing
current_date = datetime.now()
df['Ageing'] = (current_date - df.groupby('Full Name')['Date'].transform('min')).dt.days
print(df)
df['Visits'] = df.groupby('Full Name')['Nthvisit'].transform('max')

df_NEW = df.rename(columns={'Full Name': 'Doctor_Name', 'Scheduled By': 'DRO_Name'})
df_FINAL = df_NEW[['Doctor_Name','DRO_Name','Speciality','LatestStatus','Ageing','Visits']]
unique =df_FINAL.drop_duplicates(subset=['Doctor_Name'])
print(unique)


unique.to_csv('DRO.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1qikDElqYDD0FUdu8BWyzx36kpjoaKDBwkzSPGL19BG8' 

sheetName = 'Doctor Wise - Overall'        # Please set sheet name you want to put the CSV data.
csvFile = 'DRO.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'Doctor Wise - Overall'!A2:F")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
