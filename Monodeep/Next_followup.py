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

mydb = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
 WITH pat_pres AS (
    SELECT 
        user_id,
        DATE_ADD(DATE_ADD(MAX(updated_at), INTERVAL 5 HOUR), INTERVAL 30 MINUTE) AS updated_at,
        MAX(data) AS data
    FROM 
        patient_prescription pp 
    GROUP BY 
        user_id
), profileanswer AS (
    SELECT 
        user_id,
        MAX(CASE WHEN `key` = 'care_pathway' THEN answer END) AS "care_pathway",
        MAX(CASE WHEN `key` LIKE '%next_followup_date%' THEN answer END) AS "nextfollowup"
    FROM 
        profile_answer
    GROUP BY 
        user_id
), consultation AS (
    SELECT 
        c.user_id AS user_id,
        MAX(c.title) AS title,
        MAX(c.consult_type) AS consult_type,
        MAX(DATE_ADD(DATE_ADD(c.consult_datetime, INTERVAL 5 HOUR), INTERVAL 30 MINUTE)) AS consult_datetime,
        MAX(c.patient_attendance_status) AS patient_attendance_status,
        MAX(c.admin_id) AS admin_id,
        MAX(c.consultant_id) AS consultant_id,
        MAX(c.service_id) AS service_id,
        MAX(c.is_closed) AS is_closed
    FROM 
        consultation c
    GROUP BY 
        1, DATE(c.created_at)
)
SELECT 
    up.patient_id AS "Patient Id",
    up.full_name AS "Patient Name",
    up.phone AS "Phone",
    pa.care_pathway AS "Care Pathway",
    c.consult_datetime AS "Consult Datetime",
    c.patient_attendance_status AS "Patient Attendance Status",
    l.name AS "Clinic Name",
    CONVERT_TZ(pa.nextfollowup, '+00:00', '+05:30') AS NextFollowUpCM,
    CONVERT_TZ(
        STR_TO_DATE(
            JSON_UNQUOTE(JSON_EXTRACT(pp.data, '$[8].answer')),
            '%Y-%m-%dT%H:%i:%s'
        ),
        '+00:00',
        '+05:30'
    ) AS "Follow_up_PMS_consultation",
    ap.full_name AS Name  -- Adding the admin's full name
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
    admin_profile ap ON c.admin_id = ap.id  -- Joining admin_profile
LEFT JOIN 
    user_profile_locations upl ON upl.userprofile_id = up.id
LEFT JOIN 
    location l ON upl.location_id = l.id
LEFT JOIN 
    service s ON c.service_id = s.id
WHERE 
    DATE(c.consult_datetime) <= CURDATE() AND c.is_closed != 1 
ORDER BY 
    c.consult_datetime DESC;

"""
# df = pd.read_sql_query(sql_query,mycursor)
df = pd.read_sql_query(sql_query,mydb)



df['NextFollowUpCM Date']= df["NextFollowUpCM"].dt.date

df['Consult Date']= df["Consult Datetime"].dt.date
df['Consult Time']= df["Consult Datetime"].dt.time
# df['Consultation Name'] = np.where(df['Consultation Name_New'].notnull(), df['Consultation Name_New'], df['Consultation Name'])
# df['Care Pathway'] = np.where(df['Care Pathway'].notnull(),"YES", "NO")
# df['NextFollowUpCM'] = np.where(df['NextFollowUpCM'].notnull(),"YES", "NO")


df_csv = df[["Patient Id", "Patient Name", "Phone", "Clinic Name", "Consult Date", "Consult Time", "Patient Attendance Status",  "Care Pathway","NextFollowUpCM","NextFollowUpCM Date"]]

print(df_csv)


df_csv.to_csv('Next_followup.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1LnpwoNaPHRgDpl602ZN0hjPH0EbHcIi1BkCj9ZzXehY' 

sheetName = 'RAW'        # Please set sheet name you want to put the CSV data.
csvFile = 'Next_followup.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'RAW'!A2:AH")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

