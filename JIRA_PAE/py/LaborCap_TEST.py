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



discovery_state = ["created",
    "New",
    "Ready for Development",
    "PO Validation",
    "PO Grooming",
    "Ready for Refinement",
    "To Do",
    "FA Grooming",
    "Open",
    "In Planning",
    "Dev Grooming/Estimation",
    "Technical Grooming",
    "Deferred",
    "Backlog",
    "Scrum of Scrums Items",
    "Refinement",
    "Ready for Assignment",
    "Selected for Development",
    "M Editorial Request Drafts",
    "Pending Evaluation",
    "Add to Backlog",
    "Prioritized",
    "Intake",
    "Request Acknowledged",
    "Design",
    "Intake Request",
    "Initiated",
    "Assigned",
    "Triage",
    "Functional Grooming",
    "Style Review",
    "Acceptance / Review",
    "Hold for Dependencies",
    "Awaiting Requirements",
    "Action Needed",
    "On-Hold",
    "Ready For Deployment",
    "Grooming",
    "Analysis in Progress",
    "Waiting for Information",
    "M Editorial To Do",
    "M Editorial In Progress",
    # "S&D Request Drafts",
    # "S&D To Do",
    "SM Backlog",
    "Discovery Gate Prep",
    "CAR Submitted",
    "Ready for Requirements",
    "Req-Arch in Progress",
    "Ready for Estimation",
    "Ready for Planning",
    "Ready for Review",
    "Planning in Progress",
    "Architect Review",
    "New Project Requests",
    "Approved Project Backlog",
    "Discovery",
    "Architect Review Dependencies",
    "JDAP Ready",
    "Cleaned Up (Temporary)",
    "With Vendor",
    # "S&D Stakeholder Review",
    "Ready for test",
    "First Priority",
    "Results Ready",
    "Analysis",
    "ToDo",
    "Ready",
    "Integration Test",
    # "Approved & Prioritized",
    "Discussion/Review",
    "Pending",
    "Ready to Work",
    "Functional",
    "NEEDS MORE INFO",
    "IN UX/UI",
    "Needs Approval",
    "Assessing",
    "Submitted for Assessment",
    "Pending Decision",
    "Decided",
    "Ready for Dev",
    "Action Required",
    "Submitted",
    "Planning",
    "Measurement Requirements",
    "UX Design",
    "Stakeholder Review",
    "Stakeholder Kickoff",
    "Research",
    "Analyze Results",
    "Reviewed",
    "Requirements",
    "On Hold - To Do",
    "Ice Box",
    "Ready for Deploy",
    "TO-DO",
    "On Hold/Ready",
    "Reseach",
    "Wireframes To Do",
    "New Requests",
    "Approved for Estimation",
    "Estimated / Ready for Review",
    "Budgeted",
    "Tech Feasibility",
    "Product Review",
    "Ready For Approval",
    "New Request",
    "Defect Created",
    "Documentation and Tie-off",
    "Business Case Review",
    "Proposed",
    "Under Evaluation",
    "Under Review",
    "Request Drafts",
    "Parked for dependency",
    "Parked",
    "On Hold by FL",
    "Begin",
    "Concept",
    "Change in Requirements",
    "Ready For SNUG Review",
    "Waiting Approval",
    "Pending Acceptance Criteria",
    "Wishlist",
    "Approved Projects",
    "Epics",
    "Delayed",
    "Not Started",
    "Drafting",
    "Pending PO Review",
    "PO Acceptance",
    "Rework",
    "Ready for Implementation",
    "SE Backlog",
    "In UX/UI Review",
    "Planning / Requirements",
    # "Ready for Dev Grooming & Estimation",
    "Scoped",
    "Ready to Diagnose",
    "Waiting Feedback",
    "To be reviewed",
    "Board Review",
    "Business Review",
    "Wireframe IP",
    "Mockup To Do",
    "Wire Frame Needs Revision",
    "Estimation",
    "In Progress - UI",
    "Priority List",
    "Priority List - Ready for UI",
    "UX",
    "UX - Prioritized",
    "PO - Need More Information",
    "UX - In Progress",
    "UX - On Hold (Waiting for Peer Review)",
    "UX - Progress",
    "PO - On Hold",
    "Copy - In Progress",
    "UI - Prioritized",
    "PO - Unprioritized",
    "Copy - Prioritized",
    "UI-In Progress",
    "UX - On Hold (Waiting for Results)",
    "UX - On Hold (Waiting for Deploy)",
    "UI - Ready for Style Review",
    "In Progress - UX",
    "Needs Approval - PO",
    "UI - Needs Approval",
    "On Hold - PO",
    "Handed Off - Awaiting Dev",
    "Prioritized - UXD",
    "Design Idea",
    "Product Owner / Stakeholder Review",
    "Research Idea",
    "Verification",
    "WorkflowState"]


cancel_state = ["Closed - Not Doing",
    "Duplicate",
    "Closed - No Deploy",
    "Denied",
    "Canceled",
    "Rejected",
    "Cancel",
    "Cancelled",
    "Not Approved / Out of Scope",
    "Invalid",
    "Failed",
    "Closed - Inactivity",
    "Closed - Duplicate"]

close_state = ["Closed",
    "Complete",
    "Done",
    "Resolved",
    "Pass",
    # "Fixed",
    "Closed - Pending Review",
    "Archived",
    "Archive",
    "Completed",
    # "Completed - Change(s) Made",
    # "Closed - Archive"
    ]

development_state = ["In Delivery",
    "In Development",
    "Code Review",
    "On Hold",
    "In Progress",
    "Dev Blocked",
    "Deploy to Staging",
    "Dev Review",
    "Blocked",
    "Dev - In Progress",
    "Dev Complete",
    "Ready for Stage",
    "Reopened",
    "Build",
    "In Review",
    "Testing",
    "Review",
    "Work in Progress",
    "Dev In Progress",
    "Dev Complete-Pull Request",
    "Development",
    # "S&D In Progress",
    # "S&D Blocked",
    "Solution Review",
    "Peer Review",
    "Workaround",
    "Needs Review",
    "UA Review",
    "Merge Request",
    "Ready for Code Review",
    "Approval Needed",
    "PO Approval",
    "Reopen",
    "Ready for PR",
    "Sprint Test",
    "Demo",
    # "Remedy In Progress",
    "In Design",
    # "Staged",
    # "Development in Progress",
    "Merged",
    "Reivew",
    "In Code Review",
    # "Needs Style Review",
    # "Develop",
    # "Working",
    # "Style/Code Review",
    "Dev Done",
    # "Pull Request Review",
    "Executing",
    "Needs Revision",
    "Active",
    # "Wireframe Review",
    # "Mockup IP",
    # "Mockup Review",
    # "PO - Needs Approval"
    ]

deployment_state = ["Monitoring",
    "Production Review",
    "Ready for Prod",
    "Deployed",
    "Deploy",
    "Validate/Verify",
    # "S&D Complete",
    "Deployed to Production",
    "Deploying Increments",
    "Approved",
    "Validation",
    "Ready to Deploy",
    "Reporter Approval",
    # "Implemented",
    "Production",
    "CR Submitted",
    "Deploy to Production",
    "In Production",
    "Post-Production",
    "Validation Complete",
    # "Deployed to Prod",
    # "Staged for Deploy",
    # "Released Live",
    "Not a Bug",
    "Ready for Production", 
    "Deploy to Prod"]

qa_state =["QA Testing",
    "Future Test Cases",
    "Deploy to Test",
    "Deploy to UAT",
    "QA (UAT)",
    "QA Blocked",
    "QA (Test)",
    "Dev Complete / Deploy to Test",
    "QA (Staging)",
    "QA (Prod)",
    "UA (Test)",
    "SIT",
    "Ready for QA",
    "QA In Progress",
    "Ready for UAT",
    "QA Staging",
    "In QA",
    "UAT - In Progress",
    "In Testing",
    "QA",
    "UAT",
    "Test",
    "Testing / Integration (Test)",
    "Deploy to QA",
    "QA - In Progress",
    "UAT tesing",
    "test1",
    "Testing in UAT",
    "Ready for Testing",
    "UAT Validation",
    "UAT - Testing",
    "Dev QA",
    # "QA Automation",
    # "QA Icebox",
    # "Quality Assurance",
    "Deployed to UAT",
    "User Acceptance Testing",
    "UAT - In Review",
    "To Be Tested",
    "Sandbox",
    "UAT - Complete",
    # "Stores UAT",
    "QA / Testing"]

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
    


def format_timedelta(td):
    """Formats a timedelta object to 'day.hours:min:sec' format."""

    seconds = td.total_seconds()
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    return f"{int(days)}.{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


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


def split_list(lst, n):
    """Splits a list into n approximately equal parts."""
    k, m = divmod(len(lst), n)
    
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


def getUrlDataSet(url,api_token):
    pd_obj = None

    dataset= get_report(url, api_token)
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


def projectList(api_token):
    searchURL='https://jira.footlocker.com/rest/api/2/project'
    searchQry_DS = getUrlDataSet(searchURL,api_token)
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



def getBaselineLst(element):
   baselinelst =[]
   if element != None:
      if  element != 'NA':
         for idx in element:
            baselinelst.append(idx['value'])
   return baselinelst


def clean_folder(filepath,finder):
    # Get all .csv files in the 'data' directory
    csv_files = Path(filepath).glob(f"*{finder}")                                   
    for file in csv_files:
        filename=file.name
        newname= filename.replace(finder,'.csv')
        logger.warning(f'renaming {filename} TO {newname}....')
        os.rename(os.path.join(filepath, filename), os.path.join(filepath, newname))
        os.utime(os.path.join(filepath, newname), None)


def append_to_csv(df, csv_file):
    """Appends a DataFrame to a CSV file, handling missing columns."""

    # Check if the file exists and read the header if it does
    try:
        existing_df = pd.read_csv(csv_file, nrows=0)
        header = list(existing_df.columns)
    except FileNotFoundError:
        header = None

    # Reindex the DataFrame to match the existing header (if it exists)
    if header:
        df = df.reindex(columns=header, fill_value="")

    # Append the DataFrame to the CSV file
    df.to_csv(csv_file, mode='a', index=False, header=header is None)



from dotenv import load_dotenv
load_dotenv()

api_token=os.getenv('JIRA_TOKEN')

if not api_token:
    raise ValueError("No JIRA API token found. Check your .env file / environment variable.")


global logger
logger = setup_logger(logging.INFO)
logger.info("=======================================")
logger.info("Script start accumulating data from JIRA")



# run_proj = sys.argv[1]
run_proj ='ALL'


partitionCnt=5
filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/LaborCap_TEST/'

# filepath='../output/TimeSheet'
importfile='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/AllProjects/transition_history_Story.csv'

issueLists = [
            # {'issueType':"Portfolio Initiative",'partitionCnt':3},
            # {'issueType':"Product Initiative", 'partitionCnt':3},
            # {'issueType':"Epic",'partitionCnt':partitionCnt},
            {'issueType':"Story",'partitionCnt':partitionCnt}
            ]
             
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# Jira ID 
# Name
# Planview Portfolio ID
# Work Description
# IssueType
# Work State
def portoFolioDataframe(dataframe):
    return dataframe.filter(['key', 'fields.summary', 'fields.issuetype.name',
                             'fields.status.name','fields.customfield_32307'
                         #  'fields.description',
        ]).rename(columns={
        'key':'Key',
        'fields.summary':'Summary',
        'fields.customfield_32307':'Planview Portfolio ID',
        # 'fields.description':'Work Description',
        'fields.issuetype.name':'IssueType',
        'fields.status.name':'Status'})


# Name
# ParentLink
# Work Description
# Jira ID
# IssueType
# Start date
# End date
# Work State
def productInitDataframe(dataframe):
    return dataframe.filter(['key', 'fields.summary', 'fields.customfield_12823',
                             'fields.issuetype.name','fields.status.name',
                            #  'fields.description',
                            'fields.project.name'
                            ]).rename(columns={
                            'key':'Key',
                            'fields.summary':'Summary',
                            'fields.customfield_12823': 'ParentLink',
                            # 'fields.description':'Work Description',
                            'fields.issuetype.name':'IssueType',
                            'fields.status.name':'Status', 
                            'fields.project.name':'ProjectName'})


# Name ✅
# ParentLink ✅
# Work Description ❌
# Jira ID ✅
# IssueType ✅
# Start date ✅
# End date ✅
# Banner ✅
# Region ✅
# T-shirt Size ✅
# Epic Work Type ✅
# Work State ✅
# In-Progress DateDate ⏳
# Completed Date ⏳
def epicDataframe(dataframe):
    return dataframe.filter(['key', 'fields.summary', 'fields.customfield_12823',
                             'fields.issuetype.name','fields.status.name','fields.customfield_12305',
                             'fields.customfield_31400.value', 'fields.project.name', 
                             'fields.customfield_13800', 'fields.customfield_13801','fields.project.key','fields.customfield_10207.value',
                             'fields.customfield_17210','fields.customfield_17211'
                            #  'fields.description',
        ]).rename(columns={
        'fields.summary':'Summary',
        'key':'Key',
        'fields.customfield_12823': 'ParentLink',
        # 'fields.description':'Work Description',
        'fields.customfield_12305':'Banner',
        'fields.issuetype.name':'IssueType',
        'fields.status.name':'Status',
        'fields.project.name':'ProjectName',
        'fields.project.key':'ProjectKey',
        'fields.customfield_31400.value': 'Epic Type',
        'fields.customfield_13800': 'Target start',
        'fields.customfield_13801': 'Target end',
        'fields.customfield_10207.value':'T-Shirt size',
        'fields.customfield_17210':'Affected Banners',
        'fields.customfield_17211':'Affected Geographies'})


# Name ✅
# ParentLink ✅
# Work Description ❌
# Jira ID ✅
# IssueType ✅
# Story Estimate ✅
# Story Work Type ✅
# Work State ✅
# In-Progress Date ✅
# Completed Date ✅
# Jira Project Name ✅
def StoryDataframe(dataframe):
    return dataframe.filter(['key', 'fields.summary', 'fields.customfield_10006',
                             'fields.customfield_10002',
                             'fields.issuetype.name','fields.status.name',
                             'fields.project.name','fields.customfield_31800.value','fields.project.key'
         #  'fields.description',
        ]).rename(columns={
        'fields.summary':'Summary',
        'fields.customfield_10006': 'ParentLink', #Epic Link
      # 'fields.description':'Work Description',
        'fields.customfield_31800.value':'Story Type',
        'key':'Key',
         'fields.issuetype.name':'IssueType',
        'fields.customfield_10002':'StoryPoint',
        'fields.status.name':'Status',
        'fields.project.name':'ProjectName',
        'fields.project.key':'ProjectKey',})





def getDates(key,state):
    dates=''
    filterdata = importTransactionFile[importTransactionFile['key']==key]
    if state == 'close':    
     state= filterdata[filterdata['to'].isin(close_state)]
    else:
     state= filterdata[filterdata['to'].isin(development_state)]
    
    if state is not None and not state.empty:
        dates= state['created_at'].values[0]

    return dates


def data_clean(dataframe, issueType):
    if dataframe is not None and not dataframe.empty:
       if issueType == 'Portfolio Initiative':
         dataframe = portoFolioDataframe(dataframe)
       elif issueType == 'Product Initiative':
         dataframe = productInitDataframe(dataframe)
       elif issueType == 'Epic':
          dataframe = epicDataframe(dataframe)
          dataframe['Affected Banners']=dataframe['Affected Banners'].apply(lambda x: getBaselineLst(x) )
          dataframe['Affected Geographies']=dataframe['Affected Geographies'].apply(lambda x: getBaselineLst(x) )
          # dataframe['ProjectName'] = dataframe['ProjectName'].str.replace('&', '%26')
          header = list(dataframe.columns)
          
          if 'T-Shirt size' not in header:
            dataframe['T-Shirt size'] =""
            # dataframe = dataframe.reindex(columns=header, fill_value="")
          
          if 'Epic Type' not in header:
            dataframe['Epic Type'] =""

       elif issueType == 'Story':
          dataframe = StoryDataframe(dataframe)
          dataframe['In-Progress Date'] = dataframe['Key'].apply(lambda x: getDates(x,'inProgress'))
          dataframe['Completed Date'] = dataframe['Key'].apply(lambda x: getDates(x,'close'))
          # dataframe['ProjectName'] = dataframe['ProjectName'].str.replace('&', '%26')

          # header = list(dataframe.columns)
          # if 'In-Progress Date' not in header:
          #   dataframe['In-Progress Date'] =""
          # if 'Completed Date' not in header:
          #   dataframe['Completed Date'] =""
    return dataframe



def fetch( project, issueType):
    global fetch_df 
    logger.info(f"Fetch {issueType}...")

    # if issueType == 'Story':
    #     attribute =' &expand=projects.issuetypes.fields,changelog'
    # else:
    attribute =' '
    development_state_Lst = '", "'.join(development_state)
    deployment_state_Lst = '", "'.join(deployment_state)
    qa_state_Lst = '", "'.join(qa_state)

    liststr = '"{0}","{1}","{2}"'.format(development_state_Lst, deployment_state_Lst,qa_state_Lst )
    

    searchQry=f'?jql= project in ({project}) and issueType="{issueType}" and status in ({liststr}) and updated > -30d'

    # if cond_flg == True: 
    #     searchQry=f"{searchQry} and status not in (Cancelled, Done)  {attribute}"
    # else:
    searchQry=f"{searchQry} {attribute}"

    fetch_df = getDataSet(searchQry, project, issueType, api_token )

    return data_clean(fetch_df,issueType )    
    


def getChangeLog(element):
    changelog_Rec = [] 
    for history in element:
        created_at = history.get('created',None)
        for item in history['items']:
            if item['field'] =='status' :
                changelog_Rec.append({
                    'created_at': created_at,
                    'field': item['field'],
                    'from': item['fromString'],
                    'to': item['toString'],
                })
    return changelog_Rec


# clean folder
csv_files = Path(filepath).glob("*_working.csv")
for file in csv_files:
    filename=file.name
    os.remove(os.path.join(filepath, filename))

global importTransactionFile 
importTransactionFile = pd.read_csv(importfile)

# strings =pd.Series(["PCR"])
strings = projectList(api_token)

for issueList in issueLists:
    split_string_list = split_list(strings, issueList['partitionCnt'])
    filtered_list = [series for series in split_string_list if not series.empty]
    for partition in filtered_list:
        logger.warning(f"processing %s - %s",partition.values[0],issueList['issueType'])

        df=fetch(', '.join(['"{}"'.format(value) for value in partition]),issueList['issueType'])
        if df is not None and not df.empty:
            try:     
                    output_path = os.path.join(filepath, f"{issueList['issueType']}_working.csv")            
                    # df.to_csv(output_path ,mode='a', header=not os.path.exists(output_path), index=False)
                    append_to_csv(df,output_path)
            except Exception as e:  
                logger.error(f"Failed to upload files to {filepath}: {e}")
                pass
                
            logger.info("Move working files to current files")
            # Delete the old DataFrame 
            del(df)

            # Perform garbage collection                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
            gc.collect()
            # break
    clean_folder(filepath,"_working.csv")

del(importTransactionFile)
# Perform garbage collection                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
gc.collect()
logger.warning(f"Script ended accumulating data from JIRA ")
logger.warning("=======================================")