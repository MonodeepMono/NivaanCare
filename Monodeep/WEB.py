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
sheet = client.open_by_key("1hzeRksqC5B_YnZpDvvyPf2Zs3iYL8CVcrdmJSclYhhk") # Open by key the spreadhseet
#sheet.share
tab = sheet.worksheet('Visit')
calls = pd.DataFrame(tab.get_all_records())
df = calls[['Full Name','Scheduled By','Speciality','hot','warm','cold','Date','Nthvisit','Month' ]]
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
df['Visits_Month'] = df.groupby(['Full Name', 'Month'])['Nthvisit'].transform('count')

df_NEW = df.rename(columns={'Full Name': 'Doctor_Name', 'Scheduled By': 'DRO_Name'})
df_FINAL = df_NEW[['Doctor_Name','DRO_Name','Speciality','LatestStatus','Ageing','Visits']]




# Pivot table to get count of visits for each month and each doctor
visit_counts = df.pivot_table(index='Full Name', columns='Month', values='Nthvisit', aggfunc='count')

# Fill NaN values with 0
visit_counts.fillna(0, inplace=True)

# Add 'Visits' suffix to column names
visit_counts.columns = [col + '_Visits' for col in visit_counts.columns]

# Merge visit counts with the unique doctor dataframe
unique_with_visits = df_FINAL.merge(visit_counts, how='left', left_on='Doctor_Name', right_index=True)

# Fill NaN values with 0
unique_with_visits.fillna(0, inplace=True)
unique =unique_with_visits.drop_duplicates(subset=['Doctor_Name'])
print(unique)

DF_FINAL_DATA = unique[["Doctor_Name", "DRO_Name", "Speciality", "LatestStatus", "Ageing", "Visits", "Aug_Visits", "Sep_Visits", "Oct_Visits", "Nov_Visits", "Dec_Visits", "Jan_Visits", "Feb_Visits", "Mar_Visits"]]

DF_FINAL_DATA_RENAME = DF_FINAL_DATA.rename(columns={'Visits': 'Call', 'Aug_Visits': 'Aug Calls','Sep_Visits': 'Sep Call', 'Oct_Visits': 'Oct Call','Nov_Visits': 'Nov Call', 'Dec_Visits': 'Dec Calls','Jan_Visits': 'Jan Call', 'Feb_Visits': 'Feb Call','Mar_Visits': 'Mar Call'})

print(DF_FINAL_DATA_RENAME)
DF_FINAL_DATA_RENAME.to_csv('DRO_TEST.csv',index = False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '14c0KHi09ZNzE07uiSItLMTw2DLqWjvjIIqxnIO4rxAk' 

sheetName = 'Doc_View_Final'        # Please set sheet name you want to put the CSV data.
csvFile = 'DRO_TEST.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'Doc_View_Final'!A2:O")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
