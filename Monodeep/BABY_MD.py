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
  host="prod-babymd-mysql-rds-01.cyw98bspufl6.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="80w70Zl5F7guX7",
  database="babymd_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
SELECT * 
FROM babymd_production.user_crm_lead ucl 
LEFT JOIN babymd_production.webhook_wati_message_status wwms 
ON ucl.whatsapp_mobile_number = wwms.wa_id
WHERE ucl.lead_created_time IS NOT NULL;
"""
df_DATA = pd.read_sql_query(sql_query,mydb)
df_DATA
df_DATA['DATE']= df_DATA["lead_created_time"].dt.date
df_DATA['Month']= df_DATA["lead_created_time"].dt.month

df_DATA
df_Final = df_DATA [["baby_dob", "lead_category", "lead_id", "created_time", "parent_relation", "last_chat", "feedback_notes", "current_medicine", "in_clinic_category", "full_name", "lead_status", "lead_type", "form_location", "utm_ad_name", "lead_created_time", "previous_history_of_admission", "mobile", "lead_source", "baby_age_month", "email", "keywords", "baby_gender", "last_activity_time", "utm_campaign", "whatsapp_mobile_number", "baby_feeding_method", "baby_any_current_medication", "utm_adset_name", "preconsult_information", "lead_sub_status", "modified_time", "lead_sub_source", "previous_allergies", "feedback_type", "last_name", "consultation_247_chat", "id", "id", "updated_at", "event_type", "local_message_id", "wati_id", "whatsapp_message_id", "template_id", "template_name", "created_at", "conversation_id", "ticket_id", "message_text", "operator_email", "wa_id", "message_type", "status_string", "source_type", "message_timestamp", "assignee_id", "data", "operator_name", "sender_name","DATE","Month"]]
df_Final.to_csv('BABY_MD.csv',index = False)
df_Final
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
spreadsheetId = '17JJR02aOYnBPwIS6vAxCFF8PXta7mzXAWjsFJLbQhQE'
sh = gc.open_by_key(spreadsheetId)

# Define the sheet name and CSV file path
sheetName = 'RAW'
csvFile = 'BABY_MD.csv'

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
                    