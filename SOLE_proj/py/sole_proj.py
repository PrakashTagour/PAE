
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
    maxRslt = 500
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
        else:
            logger.error(f'Dataset was empty....')
            break
        # break       

    return df
    


def getUrlDataSet(url):
    pd_obj = None

    dataset= get_report(url)
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








run_proj = sys.argv[1]
# run_proj ='SOLE'
if run_proj == 'SOLE':
    print('SOLE')
    partitionCnt=1
    filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/SOLE/'
else:
    print('ALL')
    partitionCnt=30
    filepath='/Users/u1002018/Library/CloudStorage/OneDrive-SharedLibraries-FootLocker/Global Technology Services - DASH Doc Library/AllProjects/'

# filepath='./output/'


datestr = ""


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
    "S&D Request Drafts",
    "S&D To Do",
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
    "S&D Stakeholder Review",
    "Ready for test",
    "First Priority",
    "Results Ready",
    "Analysis",
    "ToDo",
    "Ready",
    "Integration Test",
    "Approved & Prioritized",
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
    "Ready for Dev Grooming & Estimation",
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
    "WorkflowState",      
            ]
development_state = ["In Development",
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
    "S&D In Progress",
    "S&D Blocked",
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
    "Remedy In Progress",
    "In Design",
    "Staged",
    "Development in Progress",
    "Merged",
    "Reivew",
    "In Code Review",
    "Needs Style Review",
    "Develop",
    "Working",
    "Style/Code Review",
    "Dev Done",
    "Pull Request Review",
    "Executing",
    "Needs Revision",
    "Active",
    "Wireframe Review",
    "Mockup IP",
    "Mockup Review",
    "PO - Needs Approval"]

deployment_state = ["Monitoring",
    "Production Review",
    "Ready for Prod",
    "Deployed",
    "Deploy",
    "Validate/Verify",
    "S&D Complete",
    "Deployed to Production",
    "Deploying Increments",
    "Approved",
    "Validation",
    "Ready to Deploy",
    "Reporter Approval",
    "Implemented",
    "Production",
    "CR Submitted",
    "Deploy to Production",
    "In Production",
    "Post-Production",
    "Validation Complete",
    "Deployed to Prod",
    "Staged for Deploy",
    "Released Live",
    "Not a Bug",
    "Ready for Production", 
    "Deploy to Prod"]

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
    "Closed - Duplicate",]

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
    "QA Automation",
    "QA Icebox",
    "Quality Assurance",
    "Deployed to UAT",
    "User Acceptance Testing",
    "UAT - In Review",
    "To Be Tested",
    "Sandbox",
    "UAT - Complete",
    "Stores UAT",
    "QA / Testing"]

close_state = ["Closed",
    "Complete",
    "Done",
    "Resolved",
    "Pass",
    "Fixed",
    "Closed - Pending Review",
    "Archived",
    "Archive",
    "Completed",
    "Completed - Change(s) Made",
    "Closed - Archive",]


if run_proj == 'SOLE' :
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
else:
    issueLists = [
            #   {'issueType':"Portfolio Initiative",'partitionCnt':1},
            #   {'issueType':"Product Initiative", 'partitionCnt':1 },
            #   {'issueType':"Epic",'partitionCnt':5},
            #   {'issueType':"Task",'partitionCnt':partitionCnt},
            #   {'issueType':"Sub-task",'partitionCnt':partitionCnt},
            #   {'issueType':"Bug",'partitionCnt':partitionCnt},
            #   {'issueType':"Incident",'partitionCnt':partitionCnt},
            #   {'issueType':"Production Defects",'partitionCnt':partitionCnt},
            #   {'issueType':"Defect",'partitionCnt':partitionCnt},
            #   {'issueType':"Issue",'partitionCnt':partitionCnt},
            #   {'issueType':"Test",'partitionCnt':partitionCnt},
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
    'IssueCreator',
    'creator.emailAddress',
    'creator.displayName',
    #  'SubTasks',
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
    'The Rudy Special',
    'Changelog',
    'CT_leadTime',
    'CT_development',
    'CT_discovery',
    'CT_deployment',
    'CT_cancelled',
    'CT_qa',
    'Changelog_Rec'
    ])


global logger
logger = setup_logger()
logger.info("=======================================")
logger.info("Script start accumulating data from JIRA")


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
                    # 'fields.subtasks',
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
                    # 'fields.customfield_16902' ,
                    'fields.customfield_32900.value' ,
                    'fields.customfield_27503' ,
                    'fields.customfield_16902.value',
                    'fields.customfield_33000',
                    'changelog.histories']).rename(columns={'fields.fixVersions': 'FixVersion', 
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
                                  # 'fields.subtasks':'SubTasks',
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
                                  'fields.customfield_33000':'Baseline Scope',
                                  'changelog.histories':'Changelog'
                                  })
    return dataframe



def fetch( project, issueType):
    global fetch_df 
    logger.info(f"Fetch {issueType}...")

    if issueType == 'Story':
        attribute =' &expand=projects.issuetypes.fields,changelog'
    else:
        attribute =' &expand=projects.issuetypes.fields'

    searchQry=f'?jql= project in ({project}) and issueType="{issueType}" and updated > startOfMonth(-11)'

    # if cond_flg == True: 
    #     searchQry=f"{searchQry} and status not in (Cancelled, Done)  {attribute}"
    # else:
    searchQry=f"{searchQry} {attribute}"

    fetch_df = getDataSet(searchQry)

    return data_clean(fetch_df)    
    


def extractSprintDetails(l_Df):
    l_Df['Sprint'] = l_Df['Sprint'].apply(lambda x: convert_to_key_value(x))

    l_Df['Sprint_state'] = l_Df['Sprint'].apply(lambda x: sprintvalue(x,'state') if x is not None else "NA" )
    l_Df['Sprint_name']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'name') if x is not None else "NA" )
    l_Df['Sprint_startDate']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'startDate') if x is not None else "NA" )
    l_Df['Sprint_endDate']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'endDate') if x is not None else "NA" )
    l_Df['Sprint_completeDate']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'completeDate') if x is not None else "NA" )
    l_Df['Sprint_activatedDate']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'activatedDate') if x is not None else "NA" )
    # l_Df['Sprint_sequence']= epic_Df['Sprint'].apply(lambda x: sprintvalue(x,'sequence') if x is not None else "NA" )
    # l_Df['Sprint_goal']= l_Df['Sprint'].apply(lambda x: sprintvalue(x,'goal') if x is not None else "NA" )
    l_Df= l_Df.drop(columns=['Sprint'])

    return l_Df


def getBaselineLst(element):
   baselinelst =[]
   if element != None:
      if  element != 'NA':
         for idx in element:
            baselinelst.append(idx['value'])
   return baselinelst


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


def split_list(lst, n):
    """Splits a list into n approximately equal parts."""
    k, m = divmod(len(lst), n)
    
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


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


def calculateCycleTime(l_Df):
    cycleTime = {}
    for index, row in l_Df.iterrows():
        # display(row)
        _status_change = pd.json_normalize(row['Changelog_Rec'])
        if len(_status_change) > 0:
            logger.debug(row['Status'])
            if (row['Status'] not in close_state):
                # Get the current time in UTC
                now_utc = datetime.now(pytz.utc)
                # Convert to Eastern Time
                eastern = pytz.timezone('US/Eastern')
                now_est = now_utc.astimezone(eastern)
                new_node = {
                            'created_at': now_est.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                             'field': 'status',
                             'from': row['Status'],
                             'to' : 'Active'
                            #  'proj'=row['projectKey'],
                            #  'key'=row['key']

                            }
                _status_change.loc[len(_status_change)] = new_node

            newCreate_node = {
                        # 'created_at': row['created_ts'].to_list()[0], 
                            'created_at': row['created_ts'],
                            'field': 'status',
                            'from': 'created',
                            'to' : 'WorkflowState'
                            # 'proj'=row['projectKey'],
                            # 'key'=row['key']
                        }

            _status_change.loc[len(_status_change)] = newCreate_node   
            _status_change.sort_values(by=['created_at'],inplace=True)
            _status_change['created_at']= pd.to_datetime(_status_change['created_at'])
            _status_change['time_diff'] = _status_change['created_at'].diff()



            row['CT_leadTime'] = format_timedelta(_status_change['time_diff'].sum())

            row['CT_discovery'] = format_timedelta(_status_change[_status_change['to'].isin( discovery_state )]['time_diff'].sum())
            
            row['CT_development'] = format_timedelta(_status_change[_status_change['to'].isin( development_state)]['time_diff'].sum())                                        
            row['CT_deployment'] = format_timedelta(_status_change[_status_change['to'].isin(deployment_state)]['time_diff'].sum())
            row['CT_cancelled'] = format_timedelta(_status_change[_status_change['to'].isin(cancel_state )]['time_diff'].sum())   
            row['CT_qa'] = format_timedelta(_status_change[_status_change['to'].isin(qa_state)]['time_diff'].sum())

            
            _status_change['created_at'] = pd.to_datetime(_status_change['created_at'],utc=True)
            _status_change['created_at'] = _status_change['created_at'].dt.date
            _status_change['proj']=row['projectKey']
            _status_change['key']=row['key']
            
            # display(_status_change)
            output_path = os.path.join(filepath, 'transition_history_Story_working.csv')
            _status_change.to_csv(output_path ,mode='a', header=not os.path.exists(output_path), index=False)

            
    return l_Df


# strings = projectList()
# # strings ='"SOLMerch","SOLEFIN"'
# split_string_list = split_list(strings, issueList['partitionCnt'])


# Get all .csv files in the 'data' directory
csv_files = Path(filepath).glob("*_working.csv")
for file in csv_files:
    filename=file.name
    os.remove(os.path.join(filepath, filename))


if run_proj == 'SOLE':
   strings =pd.Series(["SOLMerch","SOLEFIN"])
else:
   strings = projectList()

for issueList in issueLists:
    split_string_list = split_list(strings, issueList['partitionCnt'])
    filtered_list = [series for series in split_string_list if not series.empty]
    for partition in filtered_list:
        logger.warning(f"processing %s - %s",partition,issueList['issueType'])
        output_path = os.path.join(filepath, f"{issueList['issueType']}_working.csv")
        df=fetch(', '.join(['"{}"'.format(value) for value in partition]),issueList['issueType'])
        if df is not None and not df.empty:
            g_Df = initDataframe()
            g_Df = pd.concat([g_Df, df ], ignore_index=True)
            g_Df = extractSprintDetails(g_Df)
            g_Df=g_Df.fillna("NA")

            g_Df['FixVersion']= g_Df['FixVersion'].apply(lambda x: keyvalue(x,'name') if x is not None else "" )
            if run_proj == 'SOLE':
               g_Df['Baseline Scope']=g_Df['Baseline Scope'].apply(lambda x: getBaselineLst(x) )
               g_Df['Intial SOW']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 1')] )
               g_Df['After Global Design']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 2')] )
               g_Df['The Rudy Special']= g_Df['Baseline Scope'].apply(lambda x: {True:'YES',False:'NO'}[x.__contains__('Baseline 3')] )
            



            logger.info("Files uploaded")

            logger.info("Working on Stories...")
            if issueList['issueType'] == 'Story':
               g_Df['Changelog_Rec'] = g_Df['Changelog'].apply(lambda x: getChangeLog(x) )
               g_Df = calculateCycleTime(g_Df)
               if run_proj == 'SOLE':
                  g_Df= g_Df.drop(columns=['Changelog','Changelog_Rec','Labels','Components'])
               else:
                  g_Df= g_Df.drop(columns=['Changelog','Changelog_Rec','Labels','summary','Components'])
               try:
                  g_Df[g_Df['issueType'] == 'Story'].to_csv(output_path,mode='a', header=not os.path.exists(output_path), index=False)
               except Exception as e:  
                  logger.error(f"Failed to upload files to {filepath}: {e}")
                  pass
            else:
               logger.info("Upload file to sharepoint...")
               if run_proj == 'SOLE':
                  g_Df= g_Df.drop(columns=['Changelog','Changelog_Rec','Labels','Components'])
               else:
                  g_Df= g_Df.drop(columns=['Changelog','Labels','summary','Components'])
               try:
                  if issueList['issueType'] in ['Bug','Incident','Production Defects','Defect','Issue']:
                     output_path = os.path.join(filepath, 'Bug_working.csv')
                     g_Df.loc[g_Df['issueType'].isin(['Bug','Incident','Production Defects','Defect','Issue'])].to_csv(output_path, mode='a', header=not os.path.exists(output_path),index=False)
                  else:                  
                     g_Df[g_Df['issueType'] == issueList['issueType']].to_csv(output_path ,mode='a', header=not os.path.exists(output_path), index=False)
                  
               except Exception as e:  
                  logger.error(f"Failed to upload files to {filepath}: {e}")
                  pass

            # Delete the old DataFrame 
            del(g_Df)

            # Perform garbage collection                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
            gc.collect()
            # break
logger.info("Story Files uploaded")

logger.info("Move working files to current files")

# Get all .csv files in the 'data' directory
csv_files = Path(filepath).glob("*.csv")                                   
for file in csv_files:
    filename=file.name
    newname= filename.replace('_working.csv','.csv')
    os.rename(os.path.join(filepath, filename), os.path.join(filepath, newname))

logger.info("Script ended accumulating data from JIRA")
logger.info("=======================================")






# g_Df[g_Df['Intial SOW']=='YES'][['Intial SOW','After Global Design','The Rudy Special','Baseline Scope']]






# 15/* * * * *  /usr/bin/python3 /Users/u1002018/src/PAE/JIRA/py/sole_proj.py



# def getLinkedIssues(key):
#     check= epic_Df[epic_Df['key'] == key].Issuelinks.astype(bool)
#     if check.to_list()[0] == True:
#         issueLink_str=epic_Df[epic_Df['key'] == key]['Issuelinks']
#         issueLink_str=issueLink_str.to_list()[0][0]

#         searchQry=f'?jql= issue in linkedIssues("{key}")'
#         searchQry_DS = getDataSet(searchQry)
#         linkedIssue=pd.json_normalize(searchQry_DS)

#         specficfields = linkedIssue[['key','fields.status.name','fields.priority.name','fields.summary']]
#         specficfields=specficfields.rename(columns={'fields.status.name': 'status','fields.priority.name':'priority', 'fields.summary':'summary'})
#         json_specficfields =specficfields.to_json(orient='records', lines=True)
#         # print(json_specficfields)
#         return json_specficfields
#     else:
#         return ""



