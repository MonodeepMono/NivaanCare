
# Create Table stagging

import mysql.connector

#establishing the connection
conn = mysql.connector.connect(
   user='monodeep.saha', password='u5eOX37kNPh13Jdhgfv', host='stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com', database='nivaancare_production'
)

#Creating a cursor object using the cursor() method
cursor = conn.cursor()


#Creating table as per requirement
sql ='''CREATE TABLE IF NOT EXISTS toolyt (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    address VARCHAR(255),
    phone VARCHAR(20),
    country_code VARCHAR(10),
    category_id INT,
    user_assigned VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    company_id INT,
    created_by INT,
    customer_id INT,
    channel_id INT,
    visit_option VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    location_name VARCHAR(255),
    accuracy INT,
    visit_mode_id INT,
    remarks TEXT,
    schedule_content TEXT,
    date DATE,
    time TIME,
    schedule_date DATE,
    schedule_time TIME,
    schedule_end_date DATE,
    schedule_end_time TIME,
    comments TEXT,
    next_action_date DATE,
    next_action_time TIME,
    status VARCHAR(50),
    cancel_reason VARCHAR(255),
    cancelled_user INT,
    cancelled_datetime TIMESTAMP,
    reschedule_status VARCHAR(50),
    image VARCHAR(255),
    image_url VARCHAR(255),
    check_in_time DATETIME,
    check_out_time DATETIME,
    recording_url VARCHAR(255),
    customer_contact_id INT,
    customer_opportunity_id INT,
    customer_opportunity_stage_id INT,
    customer_status_type VARCHAR(50),
    customer_progress_id INT,
    reason_id INT,
    sub_reason_id INT,
    order_added INT,
    beat_or_not BOOLEAN,
    beat_id INT,
    tab_update_details TEXT,
    sms_reminder_status BOOLEAN,
    payment_id INT,
    external_sync_status VARCHAR(50),
    call_history_id INT,
    new_path VARCHAR(255),
    user_name VARCHAR(255),
    full_name VARCHAR(255),
    customer_name VARCHAR(255),
    customer_company VARCHAR(255),
    website VARCHAR(255),
    email VARCHAR(255),
    company_name VARCHAR(255)
);'''

cursor.execute(sql)
#Closing the connection
conn.close()


