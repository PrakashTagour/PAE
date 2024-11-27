# %%
import pandas as pd

transHistory = pd.read_csv('/Users/u1002018/src/PAE/JIRA/output/transistion_history.csv',names=['proj','key','created_ts','field','from','to','duration'])
tHCnt = transHistory.groupby('proj').count().reset_index()
# transHistory[transHistory[]]
# tHCnt[tHCnt['key']< 10]

# %%
from datetime import datetime

def format_timedelta(td):
    """Formats a timedelta object to 'day.hours:min:sec' format."""

    seconds = td.total_seconds()
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    return f"{int(days)}.{int(hours):02}:{int(minutes):02}:{int(seconds):02}"


# datetime in string format
str_dt1 = '2021/10/20 09:15:32.36980'
str_dt2 = '2022/2/20 04:25:42.120450'

# convert string to datetime
dt1 = datetime.strptime(str_dt1, "%Y/%m/%d %H:%M:%S.%f")
dt2 = datetime.strptime(str_dt2, "%Y/%m/%d %H:%M:%S.%f")

delta = (dt2 - dt1)
formatted_td = format_timedelta(delta)
print(formatted_td)  

# %%
# 122 days, 19:10:09.750650


