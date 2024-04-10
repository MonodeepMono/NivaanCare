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
                MAX(CASE WHEN `key` = 'age' THEN answer END) AS "age",
                MAX(CASE WHEN `key` LIKE '%next_followup_date%' THEN answer END) AS "nextfollowup"
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
                   MAX(c.payment_with_source) payment_with_source,
                   MAX(c.admin_id) admin_id,
                   MAX(c.consultant_id) consultant_id
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
            c.payment_with_source AS "Payment with",
            l.name as "Clinic Name",
            CONVERT_TZ(pa.nextfollowup, '+00:00', '+05:30') as NextFollowUpCM,
            CONVERT_TZ(
                STR_TO_DATE(
                    JSON_UNQUOTE(JSON_EXTRACT(pp.data, '$[8].answer')),
                    '%Y-%m-%dT%H:%i:%s'
                ),
                '+00:00',
                '+05:30'
            )AS "Follow_up_PMS_consultation"
            
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
            admin_profile ap ON c.consultant_id = ap.id
        LEFT JOIN 
            user_profile_locations upl ON upl.userprofile_id = up.id
        LEFT JOIN 
            location l ON upl.location_id = l.id
    WHERE 
        up.phone IN (
        "9718189607", 
"9899689357", 
"9899691995", 
"9990489999", 
"9717601001", 
"9756554408", 
"9810507344", 
"9899547633", 
"8800092040", 
"9871086644", 
"9555240543", 
"9871096123", 
"9873028882", 
"9953510013", 
"9650444377", 
"7217650062", 
"8423395793", 
"9650907464", 
"8505955614", 
"8058407388", 
"8826510113", 
"8527492753", 
"9871145518", 
"8527215131", 
"9650388990", 
"9810328002", 
"9958791166", 
"8468073850", 
"9868332468", 
"9818397423", 
"9777777175", 
"9818034684", 
"9210305061", 
"8882508075", 
"9871001052", 
"9971808392", 
"9899495959", 
"8287869191", 
"7618784226", 
"9811136859", 
"9899184096", 
"6009943276", 
"9050639685", 
"9810019782", 
"9312235609", 
"9811705163", 
"7838598258", 
"8368901953", 
"7976151306", 
"9990023536", 
"9313732781", 
"7827476254", 
"9975328586", 
"9910233332", 
"9968523977", 
"9810462102", 
"7428334454", 
"9892090821", 
"9748975250", 
"9968262171", 
"9312958812", 
"9953954348", 
"9266896996", 
"9990544159", 
"7838761294", 
"9818376376", 
"9891194400", 
"7869330411", 
"9891015351", 
"8691998592", 
"9999224441", 
"9873272117", 
"9811856988", 
"8510818182", 
"8800983367", 
"9971808390", 
"9999971817"

        )
    ORDER BY 
        c.consult_datetime DESC;

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



df_csv = df[["Patient Id", "Patient Name", "Phone", "Clinic Name", "Consult Datetime", "Consultation Name", 
             "Consultation Type", "Consulting Specialist", "Patient Attendance Status", "Payment Type", 
             "Payment Mode", "Amount", "Payment From", "Payment with", "Address", "Age", "Gender", "Pincode", 
             "Care Pathway", "Master Status", "PMS Consult Date", "Diagnosis","ON_examination",  "Occupation", "Pain Score", 
             "Pain Since", "Pain Site", "Pain Type", "Past Treatment", "Source", "Onboarding Status","NextFollowUpCM", "Follow_up_PMS_consultation"]]

print(df_csv)


df_csv.to_csv('EHR.csv',index=False)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(r'my-project-2024-414004-60efb95f9e7f.json',scope)
gc = gspread.authorize(credentials)
client = gspread.authorize(credentials)
spreadsheetId = '1ib-_s67SYFFl7jqifXrV8a45h3PvUscsRjzSiQf88zM' 

sheetName = 'Raw'        # Please set sheet name you want to put the CSV data.
csvFile = 'EHR.csv'  # Please set the filename and path of csv file.
sh = client.open_by_key(spreadsheetId)
sh.values_clear("'Raw'!A2:AG")
sh.values_update(sheetName,
                 params={'valueInputOption': 'USER_ENTERED'},
                 body={'values': list(csv.reader(open(csvFile,encoding='utf-8')))})

