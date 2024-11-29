import requests
import base64
import json
import pandas as pd
import logging
import os
from datetime import datetime
import sys

base_url = 'https://jira.footlocker.com/rest/api/2/search'

global logger
logger = setup_logger()
logger.info("=======================================")
logger.info("Script start accumulating data from JIRA")



filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/SOLE/'
# filepath='./output/'


datestr = ""

def get_report(base_url):
    logger.info("Getting report results...")
    header_gs = {'Accept': 'application/json'}
    headers = { 'Authorization' : 'Bearer %s' %  'OTQ5MjQwODU4MTU4OtHnNkWQEBeFpg4UlD91PxlqSR+H'}
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

def getDataSet(query):
    pd_obj = None
    maxRslt = 2000
    totalRsltFetch = -1
    df= pd.DataFrame()
    while True:
        dataset= get_report(base_url+query+f"&maxResults={maxRslt}&startAt={totalRsltFetch + 1 }")
        if dataset != None:
            obj = json.loads(dataset)
            logger.info(f"total: {obj['total']}")
            totalRsltFetch = totalRsltFetch + maxRslt

            if obj['issues']:
                df = pd.concat([df, pd.json_normalize(obj['issues']) ], ignore_index=True)
            else:
               logger.error(f'No records found....')
            if (totalRsltFetch >= obj['total'] ):
                break
        # pd_obj = pd.json_normalize(obj['issues'])
            

    return df
    

def getUrlDataSet(url):
    pd_obj = None

    dataset= get_report(url)
    if dataset != None:
        pd_obj = json.loads(dataset)
        # pd_obj = pd.json_normalize(obj['issues'])
    return pd_obj

def setup_logger( level=logging.INFO):
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



def convert_to_key_value(string):
    result = {}
    # print(string)
    if string :
        pairs = string[0].split(',')  # Split string by comma
        for pair in pairs:
            key, value = pair.split('=')  # Split each pair by '='
            result[key.strip()] = value.strip()  # Add to dictionary, stripping whitespace
    return result

g_Df = pd.DataFrame (columns=['key','FixVersion', 
 'Priority',
 'Acceptance Criteria',
 'Severity Level',
 'Labels',
 'EpicLink',
 'Issuelinks',
 'Status',
 'Components',
 'IssueCreator',
 'creator.emailAddress',
 'creator.displayName',
 'SubTasks',
 'Reporter.name',
 'reporter.emailAddress',
 'reporter.displayName',
 'issueType',
 'projectKey',
 'projectName',
 'created_ts',
 'updated_ts',
 'ParentLink',
 'summary',
 'StoryPoint',
 'Sprint', 
 'AssigneeName',
 'AssigneeEmailAddress',
 'AssigneeDisplayName',
 'ResolutionDescription',
 'ResolutionName',
 'parent.Summary',
 'parent.key',
 'parent.status',
 'parent.priority',
 'Defect Type',
 'Test Cycle',
 'Bug Severity',
 'Sub-track',
 'RICE Object Type',
 'Complexity',
 'Scope',
 'Baseline Scope',
 'Sprint_state',
 'Sprint_name',
 'Sprint_startDate',
 'Sprint_endDate',
 'Sprint_completeDate',        
 'Sprint_activatedDate',
 'Sprint_sequence',
 'Sprint_goal',
 'Intial SOW',
 'After Global Design',
 'The Rudy Special'
 ])




def data_clean(dataframe):
    if dataframe is not None and not dataframe.empty:
       dataframe= dataframe.filter(['key','fields.fixVersions',
                    'fields.priority.name',
                    'fields.customfield_31601',
                    'fields.customfield_12402.value',
                    'fields.labels',
                    'fields.customfield_10006',
                    # 'fields.issuelinks',
                    'fields.status.name',
                    'fields.components',
                    'fields.creator.name',
                    'fields.creator.emailAddress',
                    'fields.creator.displayName',
                    'fields.subtasks',
                    'fields.reporter.name',
                    'fields.reporter.emailAddress',
                    'fields.reporter.displayName',
                    'fields.issuetype.name',
                    'fields.project.key',
                    'fields.project.name',
                    'fields.created',
                    'fields.updated',
                    'fields.customfield_12823',
                    'fields.summary',
                    'fields.customfield_10002',
                    'fields.customfield_10004',
                    'fields.assignee.name',
                    'fields.assignee.emailAddress',
                    'fields.assignee.displayName',
                    'fields.resolution.description',
                    'fields.resolution.name',
                    'fields.parent.fields.summary',
                    'fields.parent.key',
                    'fields.parent.fields.status.name',
                    'fields.parent.fields.priority.name',
                    'fields.customfield_32101.value',
                    'fields.customfield_32302.value',
                    'fields.customfield_11605.value',
                    'fields.customfield_32901',
                    'fields.customfield_16902' ,
                    'fields.customfield_32900.value' ,
                    'fields.customfield_27503' ,
                    'fields.customfield_16902.value',
                    'fields.customfield_33000']).rename(columns={'fields.fixVersions': 'FixVersion', 
                                  'fields.priority.name':'Priority',
                                  'fields.customfield_31601': 'Acceptance Criteria',
                                  'fields.customfield_12402.value':'Severity Level',
                                  'fields.labels':'Labels',
                                  'fields.customfield_10006':'EpicLink',
                                #   'fields.issuelinks': 'Issuelinks',
                                  'fields.status.name': 'Status',
                                  'fields.components':'Components',
                                  'fields.creator.name':'IssueCreator',
                                  'fields.creator.emailAddress':'creator.emailAddress',
                                  'fields.creator.displayName': 'creator.displayName',
                                  'fields.subtasks':'SubTasks',
                                  'fields.reporter.name':'Reporter.name',
                                  'fields.reporter.emailAddress':'reporter.emailAddress',
                                  'fields.reporter.displayName': 'reporter.displayName',
                                  'fields.issuetype.name':'issueType',
                                  'fields.project.key':'projectKey',
                                  'fields.project.name':'projectName',
                                  'fields.created':'created_ts',
                                  'fields.updated':'updated_ts',
                                  'fields.customfield_12823': 'ParentLink',
                                  'fields.summary':'summary',
                                  'fields.customfield_10002':'StoryPoint',
                                  'fields.customfield_10004':'Sprint',                                  
                                  'fields.assignee.name':'AssigneeName',
                                  'fields.assignee.emailAddress':'AssigneeEmailAddress',
                                  'fields.assignee.displayName':'AssigneeDisplayName',
                                  'fields.resolution.description':'ResolutionDescription',
                                  'fields.resolution.name':'ResolutionName',
                                  'fields.parent.fields.summary': 'parent.Summary',
                                  'fields.parent.key': 'parent.key',
                                  'fields.parent.fields.status.name': 'parent.status',
                                  'fields.parent.fields.priority.name':'parent.priority',
                                  'fields.customfield_32101.value':'Defect Type',
                                  'fields.customfield_32302.value':'Test Cycle',
                                  'fields.customfield_11605.value':'Bug Severity',
                                  'fields.customfield_32901':'Sub-track',
                                  'fields.customfield_32900.value' : 'RICE Object Type',
                                  'fields.customfield_27503' : 'Complexity',
                                  'fields.customfield_16902.value':'Scope',
                                  'fields.customfield_33000':'Baseline Scope'
                                  })
    return dataframe


def fetch(issueType, project):
    global fetch_df 
    logger.info(f"Fetch {issueType}...")
    attribute =' &expand=projects.issuetypes.fields'
    searchQry=f'?jql= project in ("{project}") and issueType="{issueType}" '

    # if cond_flg == True: 
    #     searchQry=f"{searchQry} and status not in (Cancelled, Done)  {attribute}"
    # else:
    searchQry=f"{searchQry} {attribute}"

    fetch_df = getDataSet(searchQry)
    
    # if searchQry_DS:
    #     fetch_df=pd.json_normalize(searchQry_DS)
    # else:
    #     logger.error(f'No records found under {project} - {issueType} category!')
    
    return data_clean(fetch_df)    
    
def getBaselineLst(element):
   baselinelst =[]
   if element != None:
      print(element)
      if  element != 'NA':
         for idx in element:
            baselinelst.append(idx['value'])
   return baselinelst

pd.set_option('display.max_columns', None)

issueLists = [
              {'issueType':"Portfolio Initiative",'cond_flg':False},
              {'issueType':"Product Initiative", 'cond_flg':False},
              {'issueType':"Epic", 'cond_flg':False},
              {'issueType':"Story", 'cond_flg':False},
              {'issueType':"Task", 'cond_flg':False},
              {'issueType':"Sub-task", 'cond_flg':False},
              {'issueType':"Bug", 'cond_flg':False},
              {'issueType':"Incident", 'cond_flg':False},
              {'issueType':"Production Defects", 'cond_flg':False},
              {'issueType':"Defect", 'cond_flg':False},
              {'issueType':"Issue", 'cond_flg':False},
              {'issueType':"Test", 'cond_flg':False}
             ]
             
projects =["SOLEFIN", "SOLMerch"]
# projects =["SOLEFIN"]
for project in projects:
    for issueList in issueLists:
        df=fetch(issueList['issueType'], project)
        if df is not None and not df.empty:
            g_Df = pd.concat([g_Df, df ], ignore_index=True)

g_Df['Sprint'] = g_Df['Sprint'].apply(lambda x: convert_to_key_value(x))

g_Df['Sprint_state'] = g_Df['Sprint'].apply(lambda x: sprintvalue(x,'state') if x is not None else "NA" )
g_Df['Sprint_name']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'name') if x is not None else "NA" )
g_Df['Sprint_startDate']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'startDate') if x is not None else "NA" )
g_Df['Sprint_endDate']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'endDate') if x is not None else "NA" )
g_Df['Sprint_completeDate']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'completeDate') if x is not None else "NA" )
g_Df['Sprint_activatedDate']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'activatedDate') if x is not None else "NA" )
# sole_tracker['Sprint_sequence']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'sequence') if x is not None else "NA" )
g_Df['Sprint_goal']= g_Df['Sprint'].apply(lambda x: sprintvalue(x,'goal') if x is not None else "NA" )
g_Df= g_Df.drop(columns=['Sprint'])
# epic_Df['FixVersions'][4207][0]['name']
g_Df['FixVersion']= g_Df['FixVersion'].apply(lambda x: keyvalue(x,'name') if x is not None else "" )



g_Df=g_Df.fillna("NA")
g_Df['Baseline Scope']=g_Df['Baseline Scope'].apply(lambda x: getBaselineLst(x) )
g_Df['Intial SOW']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 1')] )
g_Df['After Global Design']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 2')] )
g_Df['The Rudy Special']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 3')] )

g_Df[g_Df['Intial SOW']=='YES'][['Intial SOW','After Global Design','The Rudy Special','Baseline Scope']]

logger.info("Upload file to sharepoint...")

try:
    g_Df[g_Df['issueType'] == 'Portfolio Initiative'].to_csv(os.path.join(filepath, 'Portfolio_Initiative.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Product Initiative'].to_csv(os.path.join(filepath, 'Product_Initiative.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Epic'].to_csv(os.path.join(filepath, 'Epic.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Story'].to_csv(os.path.join(filepath, 'Story.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Task'].to_csv(os.path.join(filepath, 'Task.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Sub-task'].to_csv(os.path.join(filepath, 'Subtasks.csv'), index=False)
    g_Df[g_Df['issueType'] == 'Test'].to_csv(os.path.join(filepath, 'Test.csv'), index=False)
    g_Df.loc[g_Df['issueType'].isin(['Bug','Epic','Incident','Production Defects','Defect','Issue'])].to_csv(os.path.join(filepath, 'Bug.csv'), index=False)
except Exception as e:  
    logger.error(f"Failed to upload files to {filepath}: {e}")
    pass


logger.info("Files uploaded")
logger.info("Script ended accumulating data from JIRA")