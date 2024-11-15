
from datetime import datetime

import requests
import base64
import json
import pandas as pd

base_url = 'https://jira.footlocker.com/rest/api/2/search'
filepath='/Users/u1002018/Library/CloudStorage/OneDrive-FootLocker/WorkSpace/src/PCR/'
datestr = datetime.now().strftime('%Y%m%d_%H%M')

def get_report(base_url):
    print("Getting report results...")
    header_gs = {'Accept': 'application/json'}
    headers = { 'Authorization' : 'Bearer %s' %  'OTQ5MjQwODU4MTU4OtHnNkWQEBeFpg4UlD91PxlqSR+H'}
    # 'Content-Length':3000 }
    print(base_url)
    
    for i in range(1,2):
        r = requests.get(base_url,headers=headers)
        print(r)
        if r.ok:
            print("Report results received...")
            print("HTTP %i - %s" % (r.status_code, r.reason))
            return r.text
        else:
            print("HTTP %i - %s" % (r.status_code, r.reason))
            continue
    return None

def getDataSet(query):
    pd_obj = None
    dataset= get_report(base_url+query)
    if dataset != None:
        obj = json.loads(dataset)
        pd_obj = obj['issues']
        # pd_obj = pd.json_normalize(obj['issues'])
    return pd_obj

def getUrlDataSet(url):
    pd_obj = None

    dataset= get_report(url)
    if dataset != None:
        pd_obj = json.loads(dataset)
        # pd_obj = pd.json_normalize(obj['issues'])
    return pd_obj

searchQry='?jql= project in ("SOLMERCH", "SOLEFIN") and issueType=Epic &maxResults=10000&fields=key,status,issuetype,project,summary,created,priority,customfield_10006'
searchQry_DS = getDataSet(searchQry)
epic_Df=pd.json_normalize(searchQry_DS)

epic_Df= epic_Df.drop(columns=['expand','fields.status.statusCategory.id','fields.status.statusCategory.key','fields.status.statusCategory.colorName','fields.status.statusCategory.name','fields.status.iconUrl','fields.issuetype.subtask','fields.issuetype.iconUrl' ])


searchQry='?jql= project in ("SOLMERCH", "SOLEFIN") and issueType=Story &maxResults=10000&fields=key,status,issuetype,project,summary,created,priority,customfield_10006'
searchQry_DS = getDataSet(searchQry)
story_Df=pd.json_normalize(searchQry_DS)

story_Df= story_Df.drop(columns=['expand','fields.status.statusCategory.id','fields.status.statusCategory.key','fields.status.statusCategory.colorName','fields.status.statusCategory.name','fields.status.iconUrl','fields.issuetype.subtask','fields.issuetype.iconUrl' ])

story_Df[story_Df['fields.project.key']== 'SOLEFIN'].to_csv(f'{filepath}SOLEFIN_Story_{datestr}.csv')
story_Df[story_Df['fields.project.key']== 'SOLMERCH'].to_csv(f'{filepath}SOLEMERCH_Story_{datestr}.csv')

epic_Df[epic_Df['fields.project.key']== 'SOLEFIN'].to_csv(f'{filepath}SOLEFIN_Epic_{datestr}.csv')
epic_Df[epic_Df['fields.project.key']== 'SOLMERCH'].to_csv(f'{filepath}SOLEMERCH_Epic_{datestr}.csv')


