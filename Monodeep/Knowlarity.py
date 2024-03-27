import requests
import json
import pandas as pd
from datetime import datetime
import numpy as np

def get_current_Date():
    current_datetime = datetime.now()
    return current_datetime.strftime('%Y-%m-%d %H:%M:%S')

url = "https://kpi.knowlarity.com/Basic/v1/account/calllog"

# Define start and end time as parameters
start_time = '2024-01-01 00:00:06+05:30'  # Start from 1st January 2024
end_time = get_current_Date()
headers = {
    'Accept': 'application/json',
    'x-api-key': 'hsgEAzAy5y4Z8FBjkJgya2BPaoHdYPN88IRzIXuL',
    'Authorization': '43ae518c-3a91-4ab2-af0c-be03b7edd721'
}

# Function to fetch data with pagination
def fetch_data_with_pagination(url, headers, start_time, end_time):
    all_data = []
    page = 1
    while True:
        params = {
            'start_time': start_time,
            'end_time': end_time,
            'page': page
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            response_body = response.json()
            if 'objects' in response_body and len(response_body['objects']) > 0:
                all_data.extend(response_body['objects'])
                page += 1
            else:
                break
        else:
            print("Request was not successful. Status code:", response.status_code)
            break
    return all_data

# Fetch data with pagination
data = fetch_data_with_pagination(url, headers, start_time, end_time)

# Convert data to DataFrame
df = pd.DataFrame(data)

# Perform data transformations
df['In-Out'] = np.where(df['Call_Type'] == 1, 'Outgoing', 'Incoming')
df['sr_number'] = df['knowlarity_number']
df['Caller Name'] = df['caller_name']
df['Date'] = df['start_time']
df['duration'] = df['call_duration']
df['credits_deducted'] = df['credits_deducted']
df['callid'] = df['uuid']
df['Action'] = df['business_call_type']
df['Caller'] = df['customer_number']
df['destination/Agent'] = df['agent_number']
df_FINAL = df[["In-Out", "sr_number", "Caller", "Caller Name", "Date", "duration", "credits_deducted", "Action", "extension", "destination/Agent", "callid", "order_id"]]

# Save DataFrame to CSV
df_FINAL.to_csv('KNOWLARITY_API.csv', index=False)

# Continue with Google Sheets integration...

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '16LvcLVFqhVc9Yn09OpxhbYM0b4KTc6lZt0JP6U_flpI' 

sheetName = 'KNOWLARITY_API'        # Please set sheet name you want to put the CSV data.
csvFile = 'KNOWLARITY_API.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'KNOWLARITY_API'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})



# Assuming the response contains a list of dictionaries where each dictionary represents a row in the DataFrame

