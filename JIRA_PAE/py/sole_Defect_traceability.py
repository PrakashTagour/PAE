
import requests
import base64
import json
import logging
import os
import sys
import pytz
import warnings
import gc
import pandas as pd
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

base_url = 'https://jira.footlocker.com/rest/api/2/search'



def get_report(base_url,api_token):
    logger.info("Getting report results...")
    header_gs = {'Accept': 'application/json'}
    headers = { 'Authorization' : 'Bearer %s' %  api_token}
    # 'Content-Length':3000 }
    logger.info(base_url)
    
    for i in range(0,3):
        r = requests.get(base_url,headers=headers)
        logger.info(r)
        if r.ok:
            logger.info("Report results received...")
            logger.info("HTTP %i - %s" % (r.status_code, r.reason))
            return r.text
        else:
            logger.error("HTTP %i - %s" % (r.status_code, r.reason))
            continue
    return None


def getDataSet(query, project, issueType,api_token):
    pd_obj = None
    maxRslt = 500
    totalRsltFetch = -1
    df= pd.DataFrame()
    while True:
        dataset= get_report(base_url+query+f"&maxResults={maxRslt}&startAt={totalRsltFetch + 1 }", api_token)
        if dataset != None:
            obj = json.loads(dataset)
            logger.info(f"total: {obj['total']} ({project} - {issueType})")
            totalRsltFetch = totalRsltFetch + maxRslt

            if obj['issues']:
                df = pd.concat([df, pd.json_normalize(obj['issues']) ], ignore_index=True)
            else:
               logger.error(f'No records found ({project} - {issueType})....')
            if (totalRsltFetch >= obj['total'] ):
                break
        # pd_obj = pd.json_normalize(obj['issues'])
        else:
            logger.error(f'Dataset was empty ({project} - {issueType})....')
            break
        # break       

    return df
    


def getUrlDataSet(url,api_token):
    pd_obj = None

    dataset= get_report(ur, api_token)
    if dataset != None:
        pd_obj = json.loads(dataset)
        # pd_obj = pd.json_normalize(obj['issues'])
    return pd_obj


def setup_logger( level=logging.WARNING):
    """Configures a logger and returns it."""

    # if not os.path.exists('./log'):
    #     os.makedirs('./log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # handler = logging.FileHandler('./log/soleProj.log')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
   
    logger = logging.getLogger()
    logger.setLevel(level)
    
    if (logger.hasHandlers()):
        logger.handlers.clear()

    logger.addHandler(handler)
   

    return logger


def projectList():
    searchURL='https://jira.footlocker.com/rest/api/2/project'
    searchQry_DS = getUrlDataSet(searchURL)
    df=pd.json_normalize(searchQry_DS)
    df=df.drop(df[df['key'].isin(['DEVOPS','EPL'])].index)
    return df['key']



def sprintvalue(x,field):
    if(len(x)!=0):
      return x[field]
    else:
     return 'NA'


def keyvalue(x,field):
    if(len(x)!=0):
      return x[0][field]
    else:
     return 'NA'



def clean_folder(filepath,finder):
    # Get all .csv files in the 'data' directory
    csv_files = Path(filepath).glob(f"*{finder}")                                   
    for file in csv_files:
        filename=file.name
        newname= filename.replace(finder,'.csv')
        logger.warning(f'renaming {filename} TO {newname}....')
        os.rename(os.path.join(filepath, filename), os.path.join(filepath, newname))
        os.utime(os.path.join(filepath, newname), None)



filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/SOLE/'

#filepath='./output/QA/'
from dotenv import load_dotenv
load_dotenv()

api_token=os.getenv('JIRA_TOKEN')

if not api_token:
    raise ValueError("No JIRA API token found. Check your .env file / environment variable.")


datestr = ""
partitionCnt=1


issueLists = [
          {'issueType':"Portfolio Initiative",'partitionCnt':1},
          {'issueType':"Product Initiative", 'partitionCnt':1 },
          {'issueType':"Epic",'partitionCnt':5},
          {'issueType':"Task",'partitionCnt':partitionCnt},
          {'issueType':"Sub-task",'partitionCnt':partitionCnt},
          {'issueType':"Bug",'partitionCnt':partitionCnt},
          {'issueType':"Incident",'partitionCnt':partitionCnt},
          {'issueType':"Production Defects",'partitionCnt':partitionCnt},
          {'issueType':"Defect",'partitionCnt':partitionCnt},
          {'issueType':"Issue",'partitionCnt':partitionCnt},
          {'issueType':"Test",'partitionCnt':partitionCnt},
          {'issueType':"Story",'partitionCnt':partitionCnt}
            ]
             
pd.set_option('display.max_columns', None)



def initDataframe():
    return pd.DataFrame (columns=['key','FixVersion', 
    'Priority',
    'Acceptance Criteria',
    'Severity Level',
    'Labels',
    'EpicLink',
    'Issuelinks',
    'Status',
    'Components',
    'issueType',
    'projectKey',
    'projectName',
    'created_ts',
    'updated_ts',
    'ParentLink',
    'summary'
    ])


global logger
logger = setup_logger(logging.WARNING)
logger.warning("=======================================")
logger.warning("Script start accumulating data from JIRA")


def data_clean(dataframe):
    if dataframe is not None and not dataframe.empty:
       dataframe= dataframe.filter(['key','fields.fixVersions', 
                                  'fields.priority.name',
                                  'fields.customfield_31601',
                                  'fields.customfield_12402.value',
                                  'fields.labels',
                                  'fields.customfield_10006',
                                  'fields.issuelinks',
                                  'fields.status.name',
                                  'fields.components',
                                  'fields.issuetype.name',
                                  'fields.project.key',
                                  'fields.project.name',
                                  'fields.created',
                                  'fields.updated',
                                  'fields.customfield_12823',
                                  'fields.summary']).rename(columns={'fields.fixVersions': 'FixVersion', 
                                  'fields.priority.name':'Priority',
                                  'fields.customfield_31601': 'Acceptance Criteria',
                                  'fields.customfield_12402.value':'Severity Level',
                                  'fields.labels':'Labels',
                                  'fields.customfield_10006':'EpicLink',
                                  'fields.issuelinks': 'Issuelinks',
                                  'fields.status.name': 'Status',
                                  'fields.components':'Components',
                                  'fields.issuetype.name':'issueType',
                                  'fields.project.key':'projectKey',
                                  'fields.project.name':'projectName',
                                  'fields.created':'created_ts',
                                  'fields.updated':'updated_ts',
                                  'fields.customfield_12823': 'ParentLink',
                                  'fields.summary':'summary',
                                                                    })
    return dataframe



def fetch( project, issueType):
    global fetch_df 
    logger.info(f"Fetch {issueType}...")


    attribute =' &expand=projects.issuetypes.fields'

    searchQry=f'?jql= project in ({project}) and issueType="{issueType}" and updated > startOfMonth(-11)'

    # if cond_flg == True: 
    #     searchQry=f"{searchQry} and status not in (Cancelled, Done)  {attribute}"
    # else:
    searchQry=f"{searchQry} {attribute}"

    fetch_df = getDataSet(searchQry,project, issueType)

    return data_clean(fetch_df)    
    


def split_list(lst, n):
    """Splits a list into n approximately equal parts."""
    k, m = divmod(len(lst), n)
    
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def convert_to_key_value(string):
    result = {}
   
    if string :
        pairs = string[0].split(',')  # Split string by comma
        # logger.info(pairs)
        idx =1
        for pair in pairs:
            key, value = pair.split('=')  # Split each pair by '='
            result[key.strip()] = value.strip()  # Add to dictionary, stripping whitespace
            idx = idx +1 
            if idx > 9:
                break
    return result


def createIssueLinkNode(issue,df,type,direction):
      dfi = pd.json_normalize(issue['fields'])
      # dft = pd.json_normalize(type)
      #   display(type)
      node = {'key': df['key'],'IssueLinkKey': issue['key'], 'projectKey' : issue['key'].split('-')[0],
            'status': dfi['status.name'][0], 'priority':dfi['priority.name'][0], 
            'issuetype':dfi['issuetype.name'][0], 
            'TypeName': type['name'], 'inward': type['inward'],'outward': type['outward'],'direction': direction}
      # display(node)
      return node


def getIssueLink(df):
    l_df = pd.DataFrame (columns=['key','IssueLinkKey', 
        'status',
        'priority',
        'issuetype',
        'inward',
        'outward',
        'projectKey'
        ])


    for index, row in df.iterrows():
        lst = row['Issuelinks']
        # itype = row['']

        if len(lst) > 0:
            idxlen = len(lst)
            for rec in lst:
                if rec.get("inwardIssue"):
                    inwardIssues = rec['inwardIssue']
                    l_df = l_df.append(createIssueLinkNode(inwardIssues,row,rec['type'],'inward'), ignore_index=True)
                if rec.get("outwardIssue"):
                    outwardIssues = rec['outwardIssue']
                    l_df = l_df.append(createIssueLinkNode(outwardIssues,row,rec['type'],'outward'), ignore_index=True)
    # display(l_df)
    return l_df
    # print (df['Issuelinks'].to_list()[0][idx]['inwardIssue']['fields'])


# Get all .csv files in the 'data' directory
csv_files = Path(filepath).glob("*_working_QA.csv")
for file in csv_files:
    filename=file.name
    os.remove(os.path.join(filepath, filename))

strings ='"SOLMerch"'

# strings = projectList()

for issueList in issueLists:
    # split_string_list = split_list(strings, issueList['partitionCnt'])
    # filtered_list = [series for series in split_string_list if not series.empty]
    # for partition in filtered_list:
        # logger.warning(f"processing %s - %s",partition,issueList['issueType'])

    df=fetch('SOLMerch',issueList['issueType'])
    if df is not None and not df.empty:
        g_Df = initDataframe()
        g_Df = pd.concat([g_Df, df ], ignore_index=True)
        g_Df=g_Df.fillna("NA")

        g_Df['FixVersion']= g_Df['FixVersion'].apply(lambda x: keyvalue(x,'name') if x is not None else "" )

        # display(g_Df)
        # g_Df['Issuelinks']= g_Df.apply(lambda row: getIssueLink(row['Issuelinks'], row['key']) )


        logger.info("Files uploaded")
        logger.info("Upload file to sharepoint...")

        try:
            if issueList['issueType'] in ['Portfolio Initiative']:
                output_path = os.path.join(filepath, 'Portfolio_Initiative_ParentEpic_QA_working.csv')
            else:              
                output_path = os.path.join(filepath, f"IssueLink_QA_working.csv")

            i_df = getIssueLink(g_Df)
            # display(i_df)
            if (len(i_df.index) > 0):
                i_df.to_csv(output_path ,mode='a', header=not os.path.exists(output_path), index=False)
        except Exception as e:  
            logger.error(f"Failed to upload files to {filepath}: {e}")
            pass
        
        clean_folder(filepath,"_QA_working.csv")
        # Delete the old DataFrame 
        del(g_Df)

        # Perform garbage collection                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
        gc.collect()
        
logger.info("Story Files uploaded")

logger.info("Move working files to current files")
