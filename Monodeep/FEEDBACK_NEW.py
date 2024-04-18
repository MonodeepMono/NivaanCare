import os
import pandas as pd

def extract_files_with_keyphrase(folder_path):
    file_paths = []

    # Keyphrase to search for in the text documents
    keyphrase = """Select *Positive* if you’re happy with the chat."""

    # Iterate through each text document in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:  # Specify encoding as 'utf-8'
                text = file.read()
             
                # Check if the keyphrase is present in the document
                if keyphrase in text:
                    file_paths.append(file_path)  # Append the file path to the list

    return file_paths

# Specify the folder path containing the text documents
folder_path = r'C:\Users\Admin\Downloads\chats 24_7'
file_paths = extract_files_with_keyphrase(folder_path)

# Convert the list of file paths into a DataFrame
df = pd.DataFrame(file_paths, columns=['File Path'])

# Print the DataFrame
print("DataFrame containing the file paths:")
print(df)

df.to_csv('FEEDBACK.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1-0wuVQJZx-s1r_a5QscSScSGEs7UgNHWvIKr47KosJI' 

sheetName = 'Positive_Improve'        # Please set sheet name you want to put the CSV data.
csvFile = 'FEEDBACK.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'Positive_Improve'!A2:AG")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})
