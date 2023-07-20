
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import pandas as pd
import json



def extract_header():
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%m-%d-%Y')
    completed_date = yesterday
    headers = {'APIKey':'xxxxxxxxxxxxxxx'}
    URL = f"https://api.cys.group/public/api/project?projectId=1&versionId=-1&filter=sys_completedDate%20%3E%3D%20'{completed_date}'"
    request = requests.get(URL, headers=headers, verify=False)
    res = request.json() 
    header = res['Data']['Headers']   
    return header


def extract_data():
    # yesterday = (datetime.now() - timedelta(days=1)).strftime('%m-%d-%Y')    
    # completed_date = yesterday
    completed_date = datetime(2021, 1, 1).strftime('%m/%d/%Y')
    take = 1000
    skip = 0
    headers = {'APIKey': 'xxxxxxxxxxxxxxx'}
    response = pd.DataFrame()  # create an empty DataFrame to hold the data
    while True:
        URL = f"https://api.cys.group/public/api/project?projectId=1&versionId=-1&filter=sys_completedDate%20%3E%3D%20'{completed_date}'&take={take}&skip={skip}"
        request = requests.get(URL, headers=headers, verify=False)
        Response = request.json()
        data = Response['Data']['Data']
        print("TAKE", len(data))

        if len(data) == 0:
            break
        df = pd.DataFrame(data)  # convert the data to a DataFrame and append it to the response
        response = pd.concat([response, df], ignore_index=True)
        print("SKIP", len(response))
        
        if len(data) < take:
            break
        skip += take
        if len(response) == 1000:
            break

    return response



def transform(header, data):
    # Extracting the Data and transforming it into a data frame 
    df = pd.DataFrame(data)

    # filter the dataframe from NULL values
    df = df[df[['13']].notnull().all(axis=1)]

    # Rename the columns
    df = df.rename(columns={'301': 'Dienst_inmeten'})
    df = df.rename(columns={'299': 'Dienst_montage'})
    df = df.rename(columns={'300': 'Dienst_advies'})
    
    # cloumn 11 flattening
    def parse_json(x):
        try:
            return json.loads(x)
        except:
            return None
    df['11'] = df['11'].apply(lambda x: parse_json(x) if isinstance(x, str) else None)
    df_11_main = pd.json_normalize(df['11'].tolist())
    df_11 = df_11_main.add_prefix('sys_importInfo.')

    # column 13
    df['13'] = df['13'].apply(lambda x: json.loads(x) if x is not None else None)
    df_13_main = pd.json_normalize(df['13'].tolist())
    df_13_lastModified = df_13_main.drop(columns=['sessions'])
    df_sessions = pd.json_normalize(df['13'].tolist() , record_path=['sessions'])
    df_sessions.columns = ["sessions."+col for col in df_sessions.columns]
    df_13 = pd.concat([df_sessions, df_13_lastModified], axis=1)
    df_13 = df_13.add_prefix('sys_sessionInfo.')
    df_13 = df_13.astype(str) # convert column 13 to string type 

    # column 19
    def check_valid_json(json_string):
        try:
            json.loads(json_string)
            return True
        except json.decoder.JSONDecodeError:
            return False

    df['19'] = df['19'].apply(lambda x: json.loads(x) if (x is not None and check_valid_json(x)) else None)
    df_19 = pd.json_normalize(df['19'].tolist()) 
    df_19.to_csv('df_19.csv', index=False)
    if 'ListReminders' in df_19.columns:
        df_19['ListReminders_ReminderSentDateTime'] = df_19['ListReminders'].apply(lambda x: x[0]['ReminderSentDateTime'] if isinstance(x, list) and len(x) > 0 else None)
        df_19['ListReminders_ReminderSequence'] = df_19['ListReminders'].apply(lambda x: x[0]['ReminderSequence'] if isinstance(x, list) and len(x) > 0 else None)
    else:
        df_19['ListReminders_ReminderSentDateTime'] = df_19['ListReminders']
        df_19['ListReminders_ReminderSequence'] = df_19['ListReminders']
        
    df_19 = df_19.drop(columns='ListReminders')
    df_19 = df_19.add_prefix('sys_EmailStatus_')

    # Concatenate the modefied columns 11, 13,and 19 to the original DataFrame
    df = pd.concat([df, df_11, df_13, df_19], axis=1)

    # drop the original columns
    df = df.drop(columns=['11', '13', '19'])
    
    # Replace the UniqueId number in the Data table with the coorsponding name in the Header table as a column name 
    for dic in header:
        if str(dic["UniqueId"]) in df.columns:
            df.rename(columns={str(dic["UniqueId"]): dic["Name"]}, inplace=True)

    # Replace all the "." with "_" in the columns name, "." are not accepted by BigQuery
    df.columns = df.columns.str.replace("\.", "_", regex=True)
    df.columns = df.columns.str.replace(" ", "_", regex=True)

    # # Convert sys_completedDate column to datetime type
    df['sys_completedDate'] = pd.to_datetime(df['sys_completedDate'], format='%Y-%m-%dT%H:%M:%S')

    # remove rows where sys_completedDate = NULL 
    df = df.dropna(axis=0, subset=['sys_completedDate'])
    
    # remove rows where sys_completedDate = today
    today = datetime.now() 
    df['sys_completedDate'] = pd.to_datetime(df['sys_completedDate'])
    df = df[df['sys_completedDate'].dt.date != today.date()]
    return df



def load(df):
      # BigQuery credentials 
      PROJECT_ID = "kwantum-omnichannel"
      DATASET_ID = "kw_business_data"
      TABLE = "kw_nps_data_test"
      TABLE_ID=f"{PROJECT_ID}.{DATASET_ID}.{TABLE}"
      KEY_PATH="/Users/Aibrahim/Desktop/kw-nps/kwantum-omnichannel-8c9e1d9df285.json"
               

      credentials = service_account.Credentials.from_service_account_file(
          KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

      # Construct a BigQuery client object.
      client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

        # Set the partitioning configuration based on the 'sys_completedDate' column.
      partitioning = bigquery.table.TimePartitioning(
            field="sys_completedDate",
            type_=bigquery.table.TimePartitioningType.DAY,
       )

      # Load the data into the table.
      load_job_config = bigquery.LoadJobConfig(
      write_disposition='WRITE_TRUNCATE',
      autodetect=True,
      time_partitioning=partitioning,
      )
      
      load_job = client.load_table_from_dataframe(
          df, 
          TABLE_ID,
          job_config=load_job_config,

      )  

      load_job.result()  

      # Get the updated table data.
      data = client.get_table(TABLE_ID)  
      return data

def arguments():
    header = extract_header()
    data = extract_data()
    df = transform(header,data)
    # load(df)
    print("done")
arguments()

