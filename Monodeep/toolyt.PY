import requests
import json
import pandas as pd
from datetime import datetime
def get_current_Date():
    current_datetime = datetime.now()

    return current_datetime.strftime('%Y-%m-%d %H:%M:%S')

url = "https://api.toolyt.com/view-visits"

payload = json.dumps({
  "from_date": "2022-01-01 18:30:00",
  "to_date": get_current_Date()
})
headers = {
  'AuthorizationKey': 'jj1xz5u6b32qi7qqx8oh',
  'Content-Type': 'application/json',
  'Cookie': 'laravel_session=eyJpdiI6IjRxaVhwSldEM0JcL083NVZpXC9XQmN4UT09IiwidmFsdWUiOiJGY2ZpamNLYTY1SzFSd3BQRGhaOERabm1tazMzQ3ZHTVBvR3YwNVpaR3A5aXRqdVhtNno4STMra29uS3ZsT3Q5bmlaV1FBOWxId1hjSVwvY1cyNndvMkhLMldDQnpyMWhlNzBWS0RzODhrK0JJdGJwVW5vVm9YcUkrS2xESE9qSkQiLCJtYWMiOiIwYWZiZWIxMjliYmRiODU3NjlmNTk3N2E1ZTk1MjIwYmJjNmUzMDY3NzFhYTI3MThmMjVhODE5MzIzODNmMjYyIn0%3D'
}

response = requests.request("POST", url, headers=headers, data=payload)

# Parse JSON response
data = response.json()

# Assuming the response contains a list of dictionaries where each dictionary represents a row in the DataFrame
df = pd.DataFrame(data['Visits']['completed'])

# Display the DataFrame
print(df)
print(df.columns)


# Create connection to database Prod
# mysql = lazysql(sql_type = 'mysql',
#                  host_name = 'stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com',
#                  database_name = 'nivaancare_production',
#                  user = 'monodeep.saha',
#                  password = 'u5eOX37kNPh13Jdhgfv')
# # Upsert "df" into your table
# mysql.dump_replace(df, 'toolyt', list_key = ["id"])


from sqlalchemy import create_engine
import pandas as pd

# Define your database connection details
host_name = 'stg-nivaancare-mysql-01.cydlopxelbug.ap-south-1.rds.amazonaws.com'
database_name = 'nivaancare_production'
user = 'monodeep.saha'
password = 'u5eOX37kNPh13Jdhgfv'

table_name = 'toolyt'

primary_key_column = 'id'

# Create a connection to the MySQL database
engine = create_engine(f'mysql://{user}:{password}@{host_name}/{database_name}')

# "if_exists='replace'" will replace the existing data in the table
df.to_sql(table_name, con=engine, if_exists='replace', index=False, method='multi', chunksize=1000)

try:
    print(f"Data Pushed to DB")

except Exception as e:
    print(f"An error occurred: {e}")