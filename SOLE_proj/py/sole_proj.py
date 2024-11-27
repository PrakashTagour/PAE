import requests
import base64
import json
import pandas as pd
import logging
import os
from datetime import datetime


base_url = 'https://jira.footlocker.com/rest/api/2/search'


def get_report(base_url):
    print("Getting report results...")
    header_gs = {'Accept': 'application/json'}
    headers = { 'Authorization' : 'Bearer %s' %  'OTQ5MjQwODU4MTU4OtHnNkWQEBeFpg4UlD91PxlqSR+H'}
    # 'Content-Length':3000 }
    print(base_url)
    
    for i in range(1,3):
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

def setup_logger( level=logging.INFO):
    """Configures a logger and returns it."""

    if not os.path.exists('./log'):
        os.makedirs('./log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler('./log/soleProj.log')
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger

filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/SOLE/'
# filepath='./output/'


datestr = ""

logger = setup_logger()

logger.info("=======================================")
logger.info("Script start accumulating data from JIRA")

searchQry=f'?jql= project in ("SOLMERCH", "SOLEFIN") and issueType="Portfolio Initiative" &maxResults=6000&&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)
if searchQry_DS:
    portfolioInit_Df=pd.json_normalize(searchQry_DS)
else:
    logger.error("Portfolio Initiative ")

searchQry=f'?jql= project in ("SOLMERCH", "SOLEFIN") and issueType="Product Initiative" &maxResults=6000&&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)
if searchQry_DS:
    productInit_Df=pd.json_normalize(searchQry_DS)
else:
    logger.error("Product Initiative was added to the data set")


searchQry=f'?jql= project in ("SOLMERCH", "SOLEFIN") and issueType=Epic &maxResults=6000&&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)

if searchQry_DS:  
   epic_Df=pd.json_normalize(searchQry_DS)
else:
    logger.error("Epic was added to the data set")

# epic_Df= epic_Df.drop(columns=['expand','fields.status.statusCategory.id','fields.status.statusCategory.key','fields.status.statusCategory.colorName','fields.status.statusCategory.name','fields.status.iconUrl','fields.issuetype.subtask','fields.issuetype.iconUrl' ])


searchQry=f'?jql= project in ("SOLMERCH") and issueType=Story &maxResults=6000&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)
if searchQry_DS:  
   story_Df1=pd.json_normalize(searchQry_DS)
else:
    logger.error("SOLEMERCH story was added to the data set")

# searchQry=f'?jql= project in ("SOLEFIN") and issueType=Story &maxResults=6000&expand=projects.issuetypes.fields'
# searchQry_DS = getDataSet(searchQry)

# if searchQry_DS: 
#     story_Df2=pd.json_normalize(searchQry_DS)
# else:
#     logger.error("SOLEFIN story was added to the data set")

searchQry=f'?jql= project in ("SOLMERCH") and issueType="Sub-task" &maxResults=6000&&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)
if searchQry_DS:
    SOLMERCH_subTask_Df=pd.json_normalize(searchQry_DS)
else:
    logger.error("SOLMERCH_subTask_Df was added to the data set")

# searchQry=f'?jql= project in ("SOLEFIN") and issueType="Sub-task" and  createdDate <= startOfYear("6M")  &maxResults=5000&&expand=projects.issuetypes.fields'
# searchQry_DS = getDataSet(searchQry)
# if searchQry_DS:
#     SOLEFIN_lessthanSix_subTask_Df=pd.json_normalize(searchQry_DS)
# else:
#     logger.error("SOLEFIN_lessthanSix_subTask_Df was added to the data set")

# searchQry=f'?jql= project in ("SOLEFIN") and createdDate > startOfYear("6M") and issueType="Sub-task" &maxResults=5000&&expand=projects.issuetypes.fields'
# searchQry_DS = getDataSet(searchQry)
# if searchQry_DS:
#     SOLEFIN_SixPlus_subTask_Df=pd.json_normalize(searchQry_DS)
# else:
#     logger.error("SOLEFIN_SixPlus_subTask_Df was added to the data set")

searchQry=f'?jql= project in ("SOLMERCH", "SOLEFIN") and issueType="Bug" &maxResults=5000&&expand=projects.issuetypes.fields'
searchQry_DS = getDataSet(searchQry)
if searchQry_DS:
    bug_Df=pd.json_normalize(searchQry_DS)
else:
    logger.error("Bug was added to the data set")

# story_Df[story_Df['key']=='SOLMERCH-3048']['fields.customfield_10004']
try:
    epic_Df=epic_Df._append(story_Df1,ignore_index=True)
    epic_Df=epic_Df._append(story_Df2,ignore_index=True)
    epic_Df=epic_Df._append(productInit_Df,ignore_index=True)
    epic_Df=epic_Df._append(portfolioInit_Df,ignore_index=True)
    epic_Df=epic_Df._append(SOLMERCH_subTask_Df,ignore_index=True)
    epic_Df=epic_Df._append(SOLEFIN_lessthanSix_subTask_Df,ignore_index=True)
    epic_Df=epic_Df._append(SOLEFIN_SixPlus_subTask_Df,ignore_index=True)
    epic_Df=epic_Df._append(bug_Df,ignore_index=True)
except NameError:
    pass


# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', 500)


epic_Df= epic_Df.drop(columns=['expand','id','self','fields.customfield_23301',
                                'fields.customfield_23300',
                                'fields.customfield_19720',
                                'fields.customfield_19725',
                                'fields.customfield_23301',
                                'fields.customfield_23300',
                                'fields.customfield_19720',
                                'fields.customfield_19725',
                                'fields.customfield_19724',
                                'fields.customfield_31600',
                                'fields.customfield_14309',
                                'fields.customfield_10901',
                                'fields.customfield_19716',
                                'fields.customfield_19713',	
                                'fields.customfield_11704',	
                                'fields.customfield_19714',
                                'fields.aggregatetimeoriginalestimate',
                                'fields.timeestimate',
                                'fields.customfield_21900',	
                                'fields.assignee',	
                                'fields.customfield_21502',	
                                'fields.customfield_22314',
                                'fields.customfield_19309',
                                'fields.customfield_28105',	
                                'fields.customfield_28106',	
                                'fields.customfield_20800',	
                                'fields.customfield_28103',	
                                'fields.customfield_24302',
                                'fields.creator.avatarUrls.48x48',	
                                'fields.creator.avatarUrls.24x24',
                                'fields.creator.avatarUrls.16x16',	
                                'fields.creator.avatarUrls.32x32',
                                'fields.reporter.self',
                                'fields.reporter.avatarUrls.48x48',	
                                'fields.reporter.avatarUrls.24x24',	
                                'fields.reporter.avatarUrls.16x16',	
                                'fields.reporter.avatarUrls.32x32',
                                'fields.customfield_32600',
                                'fields.customfield_18601.self',
                                'fields.customfield_12500.self',
                                'fields.customfield_11807',
                                'fields.customfield_25401',
                                'fields.customfield_27305',
                                'fields.customfield_23107',
                                'fields.issuetype.self',
                                'fields.customfield_27308',
                                'fields.customfield_23101',
                                'fields.timespent',
                                'fields.project.self',
                                'fields.customfield_12606',
                                'fields.resolutiondate',
                                'fields.customfield_20508',
                                'fields.customfield_24203',
                                'fields.customfield_20500',
                                'fields.customfield_16002',
                                'fields.customfield_17211',
                                'fields.customfield_17210',
                                'fields.customfield_12200',
                                'fields.customfield_32501',
                                'fields.customfield_13801',
                                'fields.customfield_13800',
                                'fields.customfield_13802',
                                'fields.customfield_11902',
                                'fields.customfield_11903',	
                                'fields.customfield_11906',	
                                'fields.customfield_22911',	
                                'fields.customfield_22910',
                                'fields.customfield_25306',
                                'fields.customfield_25307',	
                                'fields.timeoriginalestimate',	
                                'fields.customfield_23807',	
                                'fields.description',	
                                'fields.customfield_19748',	
                                'fields.customfield_19749',
                                'fields.customfield_10005',	
                                'fields.customfield_12305',
                                'fields.customfield_22904',	
                                'fields.customfield_12829',	
                                'fields.customfield_22900',	
                                'fields.customfield_22501',
                                'fields.customfield_10007.self',	
                                'fields.customfield_10007.value',	
                                'fields.customfield_10007.id',	
                                'fields.customfield_10007.disabled',
                                'fields.customfield_22500',	
                                'fields.customfield_16101',	
                                'fields.customfield_14201',	
                                'fields.customfield_10000',
                                'fields.customfield_10511',	
                                'fields.customfield_10512',
                                'fields.customfield_10513',	
                                'fields.customfield_19729',	
                                'fields.duedate',	
                                'fields.customfield_21801',	
                                'fields.customfield_27500',
                                'fields.customfield_31500',	
                                'fields.customfield_31501',	
                                'fields.customfield_28104',	
                                'fields.customfield_12101',	
                                'fields.customfield_31001',	
                                'fields.customfield_31003.self',	
                                'fields.customfield_31003.value',	
                                'fields.customfield_31003.id',	
                                'fields.customfield_31003.disabled',
                                'fields.customfield_31002',	
                                'fields.customfield_23102',	
                                'fields.customfield_31004',	
                                'fields.customfield_31006',	
                                'fields.customfield_32101',	
                                'fields.customfield_12207',	
                                'fields.customfield_28803',	
                                'fields.customfield_17216',	
                                'fields.customfield_10300',	
                                'fields.customfield_31300',	
                                'fields.customfield_31302',	
                                'fields.customfield_31301',	
                                'fields.customfield_32400',	
                                'fields.customfield_31200',	
                                'fields.customfield_11605',	
                                'fields.customfield_27503',	
                                'fields.customfield_31800',	
                                'fields.lastViewed',
                                'fields.customfield_22320',	
                                'fields.customfield_18901.self',	
                                'fields.customfield_18901.value',	
                                'fields.customfield_18901.id',	
                                'fields.customfield_18901.disabled',	
                                'fields.priority.self',	
                                'fields.priority.iconUrl',
                                'fields.priority.id',
                                'fields.customfield_14700.self',
                                'fields.customfield_14700.value',
                                'fields.customfield_14700.id',
                                'fields.customfield_14700.disabled',
                                'fields.customfield_16207',
                                'fields.customfield_12402.self',
                                'fields.customfield_12402.id',
                                'fields.customfield_12402.disabled',
                                'fields.status.self',
                                'fields.status.iconUrl',
                                'fields.status.id',
                                'fields.status.statusCategory.self',
                                'fields.status.statusCategory.id',
                                'fields.status.statusCategory.colorName',
                                'fields.customfield_17005.self',	
                                'fields.customfield_17005.value',	
                                'fields.customfield_17005.id',	
                                'fields.customfield_17005.disabled',
                                'fields.customfield_17004.self',	
                                'fields.customfield_17004.value',	
                                'fields.customfield_17004.id',	
                                'fields.customfield_17004.disabled',	
                                'fields.customfield_17405.self',	
                                'fields.customfield_17405.value',	
                                'fields.customfield_17405.id',	
                                'fields.customfield_17405.disabled',
                                'fields.customfield_17404',	
                                'fields.customfield_19702',	
                                'fields.customfield_10600',	
                                'fields.customfield_12106',	
                                'fields.customfield_10603',	
                                'fields.aggregatetimeestimate',	
                                'fields.customfield_28102',	
                                'fields.customfield_28100',
                                'fields.creator.self',
                                'fields.creator.key',
                                'fields.customfield_15613.self',	
                                'fields.customfield_15613.value',	
                                'fields.customfield_15613.id',	
                                'fields.customfield_15613.disabled',	
                                'fields.customfield_14800',
                                'fields.customfield_18601.value',	
                                'fields.customfield_18601.id',	
                                'fields.customfield_18601.disabled',	
                                'fields.customfield_12500.value',	
                                'fields.customfield_12500.id',	
                                'fields.customfield_12500.disabled',
                                'fields.customfield_11803',	
                                'fields.customfield_11802',	
                                'fields.customfield_11804',	
                                'fields.customfield_11806',	
                                'fields.progress.progress',	
                                'fields.progress.total',
                                'fields.votes.self',
                                'fields.votes.votes',	
                                'fields.votes.hasVoted',	
                                'fields.issuetype.id',
                                'fields.issuetype.description',	
                                'fields.issuetype.iconUrl',		
                                'fields.issuetype.subtask',
                                'fields.issuetype.avatarId',	
                                'fields.project.id',
                                'fields.project.projectTypeKey',
                                'fields.project.avatarUrls.48x48',	
                                'fields.project.avatarUrls.24x24',	
                                'fields.project.avatarUrls.16x16',	
                                'fields.project.avatarUrls.32x32',
                                'fields.aggregatetimespent',	
                                'fields.customfield_31400',	
                                'fields.customfield_11910',	
                                'fields.customfield_12603.self',	
                                'fields.customfield_12603.value',	
                                'fields.customfield_12603.id',
                                'fields.customfield_12603.disabled',	
                                'fields.workratio',	
                                'fields.customfield_22800',	
                                'fields.watches.self',	
                                'fields.watches.watchCount',	
                                'fields.watches.isWatching',
                                'fields.customfield_14345',	
                                'fields.customfield_12204.self',	
                                'fields.customfield_12204.value',	
                                'fields.customfield_12204.id',	
                                'fields.customfield_12204.disabled',	
                                'fields.customfield_11106.self',	
                                'fields.customfield_11106.value',	
                                'fields.customfield_11106.id',	
                                'fields.customfield_11106.disabled',
                                'fields.customfield_25305',	
                                'fields.customfield_17209.self',	
                                'fields.customfield_17209.value',	
                                'fields.customfield_17209.id',	
                                'fields.customfield_17209.disabled',
                                'fields.customfield_12821',	
                                'fields.customfield_11614',
                                'fields.assignee.avatarUrls.48x48',	
                                'fields.assignee.avatarUrls.24x24',	
                                'fields.assignee.avatarUrls.16x16',	
                                'fields.assignee.avatarUrls.32x32',
                                'fields.environment',
                                'fields.customfield_10009',
                                'fields.customfield_10008',
                                'fields.customfield_22906',
                                'fields.customfield_12810',
                                'fields.customfield_13500',
                                'fields.customfield_19721',	
                                'fields.resolution',	
                                'fields.customfield_11201',
                                'fields.customfield_19723',
                                'fields.customfield_31601',
                                'fields.customfield_22319',
                                'fields.status.statusCategory.key',	
                                'fields.status.statusCategory.name',
                                'fields.creator.active',
                                'fields.reporter.key',
                                'fields.reporter.active',
                                'fields.reporter.timeZone',	
                                'fields.aggregateprogress.progress',	
                                'fields.aggregateprogress.total',
                                'fields.assignee.self',
                                'fields.assignee.key',
                                'fields.assignee.active',
                                'fields.assignee.timeZone',
                                'fields.customfield_18600',
                                'fields.status.description',
                                'fields.resolution.self',	
                                'fields.resolution.id',
                                'fields.customfield_12302',
                                'fields.customfield_11610',
                                'fields.customfield_11200',	
                                'fields.customfield_11203',
                                'fields.creator.timeZone'
                                 ])

epic_Df= epic_Df.drop(columns=['fields.customfield_31500.self',
'fields.customfield_31500.value',
'fields.customfield_31500.id',
'fields.customfield_31500.disabled',
'fields.customfield_31501.self',
'fields.customfield_31501.value',
'fields.customfield_31501.id',
'fields.customfield_31501.disabled',
'fields.customfield_10300.self',
'fields.customfield_10300.value',
'fields.customfield_10300.id',
'fields.customfield_10300.disabled',
'fields.customfield_31300.self',
'fields.customfield_31300.value',
'fields.customfield_31300.id',
'fields.customfield_31300.disabled',
'fields.customfield_31200.self',
'fields.customfield_31200.value',
'fields.customfield_31200.id',
'fields.customfield_31200.disabled',
'fields.customfield_31301.self',
'fields.customfield_31301.name',
'fields.customfield_31301.key',
'fields.customfield_31301.emailAddress',
'fields.customfield_31301.avatarUrls.48x48',
'fields.customfield_31301.avatarUrls.24x24',
'fields.customfield_31301.avatarUrls.16x16',
'fields.customfield_31301.avatarUrls.32x32',
'fields.customfield_31301.displayName',
'fields.customfield_31301.active',
'fields.customfield_31301.timeZone',
'fields.customfield_27503.self',
'fields.customfield_27503.value',
'fields.customfield_27503.id',
'fields.customfield_27503.disabled',
'fields.customfield_31001.self',
'fields.customfield_31001.name',
'fields.customfield_31001.key',
'fields.customfield_31001.emailAddress',
'fields.customfield_31001.avatarUrls.48x48',
'fields.customfield_31001.avatarUrls.24x24',
'fields.customfield_31001.avatarUrls.16x16',
'fields.customfield_31001.avatarUrls.32x32',
'fields.customfield_31001.displayName',
'fields.customfield_31001.active',
'fields.customfield_31001.timeZone',
'fields.customfield_31003',
'fields.customfield_31004.self',
'fields.customfield_31004.name',
'fields.customfield_31004.key',
'fields.customfield_31004.emailAddress',
'fields.customfield_31004.avatarUrls.48x48',
'fields.customfield_31004.avatarUrls.24x24',
'fields.customfield_31004.avatarUrls.16x16',
'fields.customfield_31004.avatarUrls.32x32',
'fields.customfield_31004.displayName',
'fields.customfield_31004.active',
'fields.customfield_31004.timeZone',
'fields.aggregateprogress.percent',
'fields.progress.percent',
'fields.versions',

'fields.customfield_32307',
'fields.parent.id',
'fields.parent.self',

'fields.parent.fields.status.self',
'fields.parent.fields.status.description',
'fields.parent.fields.status.iconUrl',
'fields.parent.fields.status.id',
'fields.parent.fields.status.statusCategory.self',
'fields.parent.fields.status.statusCategory.id',
'fields.parent.fields.status.statusCategory.key',
'fields.parent.fields.status.statusCategory.colorName',
'fields.parent.fields.status.statusCategory.name',
'fields.parent.fields.priority.self',
'fields.parent.fields.priority.iconUrl',
'fields.parent.fields.priority.id',
'fields.parent.fields.issuetype.self',
'fields.parent.fields.issuetype.id',
'fields.parent.fields.issuetype.description',
'fields.parent.fields.issuetype.iconUrl',
'fields.parent.fields.issuetype.name',
'fields.parent.fields.issuetype.subtask',
'fields.parent.fields.issuetype.avatarId',
'fields.customfield_12826',	
'fields.customfield_32302',	
'fields.customfield_32101.self',
'fields.customfield_32101.id',	
'fields.customfield_32101.disabled',	
'fields.customfield_11605.self',	
'fields.customfield_11605.id',	
'fields.customfield_11605.disabled',	
'fields.customfield_32302.self',	
'fields.customfield_32302.value',	
'fields.customfield_32302.id',	
'fields.customfield_32302.disabled',

])

epic_Df = epic_Df.rename(columns={'fields.fixVersions': 'FixVersion', 
                                  'fields.priority.name':'Priority',
                                  'fields.customfield_31601': 'Acceptance Criteria',
                                  'fields.customfield_12402.value':'Severity Level',
                                  'fields.labels':'Labels',
                                  'fields.customfield_10006':'EpicLink',
                                  'fields.issuelinks': 'Issuelinks',
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
                                  'fields.customfield_11605.value':'Bug Severity'
                                  
                                  })



def sprintvalue(x,field):
    if(len(x)!=0):
      return x[field]
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

epic_Df['Sprint'] = epic_Df['Sprint'].apply(lambda x: convert_to_key_value(x))

epic_Df['Sprint_state'] = epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'state') if x is not None else "NA" )
epic_Df['Sprint_name']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'name') if x is not None else "NA" )
epic_Df['Sprint_startDate']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'startDate') if x is not None else "NA" )
epic_Df['Sprint_endDate']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'endDate') if x is not None else "NA" )
epic_Df['Sprint_completeDate']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'completeDate') if x is not None else "NA" )
epic_Df['Sprint_activatedDate']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'activatedDate') if x is not None else "NA" )
# sole_tracker['Sprint_sequence']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'sequence') if x is not None else "NA" )
epic_Df['Sprint_goal']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'goal') if x is not None else "NA" )
epic_Df= epic_Df.drop(columns=['Sprint'])


def keyvalue(x,field):
    if(len(x)!=0):
      return x[0][field]
    else:
     return 'NA'

# epic_Df['FixVersions'][4207][0]['name']
epic_Df['FixVersion']= epic_Df['FixVersion'].apply(lambda x: keyvalue(x,'name') if x is not None else "" )

def getLinkedIssues(key):
    check= epic_Df[epic_Df['key'] == key].Issuelinks.astype(bool)
    if check.to_list()[0] == True:
        issueLink_str=epic_Df[epic_Df['key'] == key]['Issuelinks']
        issueLink_str=issueLink_str.to_list()[0][0]

        searchQry=f'?jql= issue in linkedIssues("{key}")'
        searchQry_DS = getDataSet(searchQry)
        linkedIssue=pd.json_normalize(searchQry_DS)

        specficfields = linkedIssue[['key','fields.status.name','fields.priority.name','fields.summary']]
        specficfields=specficfields.rename(columns={'fields.status.name': 'status','fields.priority.name':'priority', 'fields.summary':'summary'})
        json_specficfields =specficfields.to_json(orient='records', lines=True)
        # print(json_specficfields)
        return json_specficfields
    else:
        return ""


# epic_Df['Issuelinks']= epic_Df['key'].apply(lambda x: getLinkedIssues(x) )
epic_Df= epic_Df.drop(columns=['Issuelinks'])


logger.debug("Upload file to sharepoint...")
epic_Df[epic_Df['issueType']== 'Story'].to_csv(f'{filepath}Story{datestr}.csv')
epic_Df[epic_Df['issueType']== 'Epic'].to_csv(f'{filepath}Epic{datestr}.csv')
epic_Df[epic_Df['issueType']== 'Product Initiative'].to_csv(f'{filepath}Product_Initiative{datestr}.csv')
epic_Df[epic_Df['issueType']== 'Portfolio Initiative'].to_csv(f'{filepath}Portfolio_Initiative{datestr}.csv')

epic_Df[epic_Df['issueType']== 'Bug'].to_csv(f'{filepath}Bug{datestr}.csv')
epic_Df[epic_Df['issueType']== 'Sub-task'].to_csv(f'{filepath}Subtasks{datestr}.csv')
logger.info("Files uploaded")
logger.info("Script ended accumulating data from JIRA")
logger.info("=======================================")
