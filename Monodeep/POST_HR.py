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
 WITH pat_pres AS (
            SELECT 
                user_id,
                DATE_ADD(DATE_ADD(MAX(updated_at) ,INTERVAL 5 hour),INTERVAL 30 minute) AS updated_at,
                max(data) as data
            FROM 
                patient_prescription pp 
            GROUP BY 
                user_id
        )
        , profileanswer AS (
            SELECT 
                user_id,
                MAX(CASE WHEN `key` = 'care_pathway' THEN answer END) AS "care_pathway",
                MAX(CASE WHEN `key` = 'occupation' THEN answer END) AS "occupation",
                MAX(CASE WHEN `key` = 'pain' THEN answer END) AS "pain",
                MAX(CASE WHEN `key` = 'pain_since' THEN answer END) AS "pain_since",
                MAX(CASE WHEN `key` = 'pain_site' THEN answer END) AS "pain_site",
                MAX(CASE WHEN `key` = 'pain_type' THEN answer END) AS "pain_type",
                MAX(CASE WHEN `key` = 'past_treatment' THEN answer END) AS "past_treatment",
                MAX(CASE WHEN `key` = 'source' THEN answer END) AS "source",
                MAX(CASE WHEN `key` = 'age' THEN answer END) AS "age"
            FROM 
                profile_answer
            GROUP BY 
                user_id
        )
        , consultation AS (
            select c.user_id user_id,
                   MAX(c.title) title,
                   MAX(c.consult_type) consult_type,
                   MAX(DATE_ADD(DATE_ADD(c.consult_datetime ,INTERVAL 5 hour),INTERVAL 30 minute)) consult_datetime,
                   MAX(c.patient_attendance_status) patient_attendance_status,
                   MAX(c.payment_type) payment_type,
                   MAX(c.payment_mode) payment_mode,
                   MAX(c.amount) amount,
                   MAX(c.payment_from) payment_from,
                   MAX(c.payment_with) payment_with,
                   MAX(c.admin_id) admin_id
            from consultation c
            group by 1,date(c.created_at)
        )
        SELECT 
            up.patient_id AS "Patient Id",
            up.full_name AS "Patient Name",
            up.address AS "Address",
            pa.age as "Age",
            up.gender AS "Gender",
            up.phone AS "Phone",
            up.pincode AS "Pincode",
            pa.care_pathway AS "Care Pathway",
            rs.title AS "Master Status",
            pp.updated_at AS "PMS Consult Date",
            pp.data as "Diagnosis",
            pp.data as "ON_examination",
            pa.occupation AS "Occupation",
            pa.pain AS "Pain Score",
            pa.pain_since AS "Pain Since",
            pa.pain_site AS "Pain Site",
            pa.pain_type AS "Pain Type",
            pa.past_treatment AS "Past Treatment",
            pa.source AS "Source",
            up.onboarding_status AS "Onboarding Status",
            c.title AS "Consultation Name",
            c.consult_type AS "Consultation Type",
            ap.full_name AS "Consulting Specialist",
            c.consult_datetime AS "Consult Datetime",
            c.patient_attendance_status AS "Patient Attendance Status",
            c.payment_type AS "Payment Type",
            c.payment_mode AS "Payment Mode",
            c.amount AS "Amount",
            c.payment_from AS "Payment From",
            c.payment_with AS "Payment with",
            l.name as "Clinic Name"
        FROM 
            user_profile up
        LEFT JOIN 
            consultation c ON up.id = c.user_id
        LEFT JOIN 
            resource_status rs ON up.status_id = rs.uuid
        LEFT JOIN 
            profileanswer pa ON up.id = pa.user_id
        LEFT JOIN 
            pat_pres pp ON up.id = pp.user_id
        LEFT JOIN 
            admin_profile ap ON c.admin_id = ap.id
        LEFT JOIN 
            user_profile_locations upl ON upl.userprofile_id = up.id
        LEFT JOIN 
            location l ON upl.location_id = l.id
        ORDER BY c.consult_datetime DESC;
"""
df = pd.read_sql_query(sql_query_prod,mydb_prod)


def check_data(a):
    if type(a) is type(None):
        return None
    else:
        for i in json.loads(a):
            if i['name']=='diagnosis':
                return i['answer']

df['Diagnosis'] = df['Diagnosis'].apply(check_data)


def check_data_new(b):
    if type(b) is type(None):
        return None
    else:
        for i in json.loads(b):
            if i['name']=='on_examination':
                return i['answer']

df['ON_examination'] = df['ON_examination'].apply(check_data_new)
print(df)

df_csv = df[["Patient Id", "Patient Name", "Phone", "Clinic Name", "Consult Datetime", "Consultation Name", 
             "Consultation Type", "Consulting Specialist", "Patient Attendance Status", "Payment Type", 
             "Payment Mode", "Amount", "Payment From", "Payment with", "Address", "Age", "Gender", "Pincode", 
             "Care Pathway", "Master Status", "PMS Consult Date", "Diagnosis","ON_examination",  "Occupation", "Pain Score", 
             "Pain Since", "Pain Site", "Pain Type", "Past Treatment", "Source", "Onboarding Status"]]


df_csv.to_csv('Post_EHR_Tracker_NEW.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1TfijYVO74DgqInivHaBwf3exOLJ9Kg856P-I5H4aI2o' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'Post_EHR_Tracker_NEW.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:X")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

