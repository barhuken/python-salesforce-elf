
# python-salesforce-elf

python-salesforce-elf is a simple [python3](https://www.python.org/downloads/) script used to extract all of the **Daily** EventLogs stored in a Salesforce org's [EventLogFile Object](https://salesforce-elf.herokuapp.com/) between a specified start and end date.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install [pandas](https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html) and [simple-salesforce](https://simple-salesforce.readthedocs.io/en/latest/). (You may need to install a version of [conda](https://docs.conda.io/projects/conda/en/4.6.0/user-guide/install/index.html) before installing the packages)

```bash
pip install pandas
pip install simple-salesforce
```

## Usage
```javascript
//elf_config.json

{
    "creds": {
        "username": "yourSFusername",
        "password": "yourSFpassword",
        "security_token": "EXAMPLEabcdEFGH123456789jkLMNopQrs",
        "domain": "login"
    },
    "setup": {
        "start_date": "2021-06-01",
        "end_date": "2021-06-30",
        "file_path": "./myLogs"
    }
}

```

```python
# query_ELF.py

import json
import pandas as pd
import os
import datetime
from simple_salesforce import Salesforce, SalesforceLogin, SFType

print("Begin...")

# read in data from elf_config.json
configInfo = json.load(open('elf_config.json'))

# creds
username = configInfo['creds']['username']
password = configInfo['creds']['password']
security_token = configInfo['creds']['security_token'] # https://help.salesforce.com/articleView?id=sf.user_security_token.htm&type=5
domain = configInfo['creds']['domain'] # login (for Developer and Production orgs) or test (for UAT orgs)


# connect to Salesforce org
print("Connecting...")
session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)
sf = Salesforce(instance=instance, session_id=session_id)

# setup SOQL query string
querySOQL = """SELECT ID, EventType, CreatedDate, LogFileLength, LogDate, ApiVersion, LogFileContentType, Sequence, Interval, LogFile FROM EventLogFile """

# query records method
response = sf.query(querySOQL)
lstRecords = response.get('records')
nextRecordsUrl = response.get('nextRecordsUrl')

while not response.get('done'):
    response = sf.query_more(nextRecordsUrl, identifier_is_url=True)
    lstRecords.extend(response.get('records'))
    nextRecordsUrl = response.get('nextRecordsUrl')

df_records = pd.DataFrame(lstRecords)

# extra step for debugging
df_records.to_csv('EventLogFile details.csv', index=False)

# setup
folder_path = configInfo['setup']['file_path']
start_date = datetime.datetime.strptime(configInfo['setup']['start_date'], '%Y-%m-%d')
end_date = datetime.datetime.strptime(configInfo['setup']['end_date'], '%Y-%m-%d')
instance_name = sf.sf_instance

for row in df_records.iterrows():
    #print(row)
    m_EventType = row[1]['EventType']
    m_LogDate = row[1]['LogDate'].split("T")[0]
    m_LogFile= row[1]['LogFile']
    m_FileName = m_LogDate + "_" + m_EventType + ".csv"
    #print(m_FileName)
    
    LogDate_date = datetime.datetime.strptime(m_LogDate, '%Y-%m-%d')
    
    # check if LogDate is between setup start_date and end_date
    if(LogDate_date >= start_date and LogDate_date <= end_date):
        if not os.path.exists(os.path.join(folder_path, m_LogDate)):
            os.mkdir(os.path.join(folder_path, m_LogDate))       
            
        request = sf.session.get('https://{0}{1}/'.format(instance_name, m_LogFile), headers=sf.headers)
        with open(os.path.join(folder_path, m_LogDate, m_FileName), 'wb') as f:
            print("writing: "+ m_FileName)
            f.write(request.content)
            f.close()

print("Complete")
```

## Contributing
Pull requests are welcome. 

## License
[MIT](https://choosealicense.com/licenses/mit/)