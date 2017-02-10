
# coding: utf-8

# In[1]:

import sqlalchemy as alch
from pandas.io import sql
import pandas as pd


# In[2]:

engine = alch.create_engine('postgresql://postgres:postgres@localhost:5432/blendo')


# In[3]:

#check for duplicate data
check_activity_duplicates="select action, campaign_id, email_address, email_id, list_id, timestamp, type, count(*) from email_activity group by action, campaign_id, email_address, email_id, list_id, timestamp, type having count(*) > 1"
check_campaign_duplicates="select create_time, delivery_status_enabled, emails_sent, id, recipients_list_id, recipients_recipient_count, report_summary_click_rate, report_summary_clicks, report_summary_open_rate, report_summary_opens, report_summary_unique_opens, send_time, count(*) from email_campaigns group by   create_time, delivery_status_enabled, emails_sent, id, recipients_list_id, recipients_recipient_count, report_summary_click_rate, report_summary_clicks, report_summary_open_rate, report_summary_opens, report_summary_unique_opens, send_time having count(*) > 1"
check_lists_duplicates="select email_address, email_client, email_type, id, language, last_changed, list_id, location_country_code, location_timezone, stats_avg_click_rate, stats_avg_open_rate, status, timestamp_signup, count(*) from email_lists group by email_address, email_client, email_type, id, language, last_changed, list_id, location_country_code, location_timezone, stats_avg_click_rate, stats_avg_open_rate, status, timestamp_signup having count(*) > 1"


# In[4]:

print(pd.read_sql(check_activity_duplicates, engine))
print(pd.read_sql(check_campaign_duplicates, engine))
print(pd.read_sql(check_lists_duplicates, engine))


# In[5]:

#load data
lists=sql.read_sql("SELECT * FROM email_lists", engine,['id'])
activity=sql.read_sql("SELECT * FROM email_activity", engine)
campaigns=sql.read_sql("SELECT * FROM email_campaigns", engine,['id'])


# In[6]:

#count user actions from email_activity(there are campaigns which are not in email_campaigns)
unique_clicks=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='click' GROUP BY campaign_id",engine,['campaign_id'])
#print(unique_clicks)
unique_opens=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='open' GROUP BY campaign_id",engine,['campaign_id'])
#print(unique_opens)
unique_bounces=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='bounce' AND type='hard' GROUP BY campaign_id",engine,['campaign_id'])
#print(unique_bounces)


# In[7]:

#count actions for users that were active(opened or clicked an email) the last 4 months
params={'date_limit':'2016-10-1'}
unique_clicks_engaged=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='click' AND email_id IN (SELECT DISTINCT(email_id) FROM email_activity WHERE timestamp>%(date_limit)s) GROUP BY campaign_id",engine,['campaign_id'],params=params)
#print(unique_clicks_engaged)
unique_opens_engaged=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='open' AND email_id IN (SELECT DISTINCT(email_id) FROM email_activity WHERE timestamp>%(date_limit)s) GROUP BY campaign_id",engine,['campaign_id'],params=params)
#print(unique_opens_engaged)
unique_bounces_engaged=sql.read_sql("SELECT campaign_id,COUNT(DISTINCT(email_id)) FROM email_activity WHERE action='bounce' AND type='hard' AND email_id IN (SELECT DISTINCT(email_id) FROM email_activity WHERE timestamp>%(date_limit)s) GROUP BY campaign_id",engine,['campaign_id'],params=params)
#print(unique_bounces_engaged)


# In[8]:

result=pd.DataFrame(unique_opens)
result.columns=['open']
result['clicks']=unique_clicks
result['bounces']=unique_bounces


# In[9]:

result_eng=pd.DataFrame(unique_opens_engaged)
result_eng.columns=['open']
result_eng['clicks']=unique_clicks_engaged
result_eng['bounces']=unique_bounces_engaged


# In[10]:

result=result.fillna(0)
#print(result)
result_eng=result_eng.fillna(0)
#print(result_eng)


# In[11]:

print(result['clicks']/result['open'])


# In[12]:

print(result_eng['clicks']/result_eng['open'])


# In[13]:

bounces_per_campaign=sql.read_sql("SELECT campaign_id,count(*) FROM email_activity where action='bounce' group by campaign_id", engine,'campaign_id')
#print(bounces_per_campaign)
campaigns_bounce=sql.read_sql("SELECT id,report_summary_click_rate FROM email_campaigns", engine,'id')
campaigns_bounce['bounces']=bounces_per_campaign
print(campaigns_bounce.fillna(0))

import matplotlib.pyplot as plt
get_ipython().magic('matplotlib inline')
plt.scatter(campaigns_bounce['bounces'],campaigns_bounce['report_summary_click_rate'])
plt.xlabel("Bounces")
plt.ylabel("Click rate")
plt.title("Campaign Click rate vs Bounces")


# In[14]:

#check clicks per month vs campaigns per month for each user
#clicks per month for each user
clicks_per_month=sql.read_sql("SELECT email_id,extract(year from timestamp) as year, extract(month from timestamp) as month,count(*) as clicks FROM email_activity WHERE action='click' GROUP BY email_id,extract(year from timestamp) , extract(month from timestamp) order by email_id,extract(year from timestamp) , extract(month from timestamp)",engine,['email_id','year','month'])
#print(clicks_per_month)


# In[15]:

#campaigns per month for each user
campaigns_per_month=sql.read_sql("SELECT email_id,extract(year from timestamp) as year,extract(month from timestamp) as month,count(DISTINCT(campaign_id)) as campaigns FROM email_activity GROUP BY email_id,extract(year from timestamp) , extract(month from timestamp) order by email_id,extract(year from timestamp) , extract(month from timestamp)",engine,['email_id','year','month'])
#print(campaigns_per_month)


# In[16]:

campaigns_per_month['clicks']=clicks_per_month['clicks']
campaigns_per_month=campaigns_per_month.fillna(0)


# In[17]:

#check avg clicks per number of campaigns(only the first 4 results have significant number of data,so i will ignore the rest)
print(campaigns_per_month.groupby('campaigns')['clicks'].mean())


# In[18]:

plt.plot(campaigns_per_month.groupby('campaigns')['clicks'].mean())
plt.xlim(0,4)
plt.ylim(0,1)
plt.xlabel("Campaigns per month")
plt.ylabel("Clicks")
plt.title("No of Campaigns vs Clicks")


# In[19]:

#check clicks per month vs campaigns per month for all users that clicked one email at most 5 months ago
params={'date_limit':'2016-09-1'}
clicks_per_month_eng=sql.read_sql("SELECT email_id,extract(year from timestamp) as year, extract(month from timestamp) as month,count(*) as clicks FROM email_activity WHERE action='click' AND email_id IN (SELECT DISTINCT(email_id) FROM email_activity WHERE timestamp>%(date_limit)s) GROUP BY email_id,extract(year from timestamp) , extract(month from timestamp) order by email_id,extract(year from timestamp) , extract(month from timestamp)",engine,['email_id','year','month'],params=params)
#print(clicks_per_month_eng)
campaigns_per_month_eng=sql.read_sql("SELECT email_id,extract(year from timestamp) as year,extract(month from timestamp) as month,count(DISTINCT(campaign_id)) as campaigns FROM email_activity WHERE email_id IN (SELECT DISTINCT(email_id) FROM email_activity WHERE timestamp>%(date_limit)s) GROUP BY email_id,extract(year from timestamp) , extract(month from timestamp) order by email_id,extract(year from timestamp) , extract(month from timestamp)",engine,['email_id','year','month'],params=params)
#print(campaigns_per_month_eng)
campaigns_per_month_eng['clicks']=clicks_per_month_eng['clicks']
campaigns_per_month_eng=campaigns_per_month_eng.fillna(0)
#print(campaigns_per_month_eng)


# In[20]:

#there are sufficient data up to 4 campaigns
print(campaigns_per_month_eng.groupby('campaigns')['clicks'].mean())


# In[21]:

plt.plot(campaigns_per_month.groupby('campaigns')['clicks'].mean())
plt.plot(campaigns_per_month_eng.groupby('campaigns')['clicks'].mean())
plt.xlim(0,4)
plt.ylim(0,1)
plt.xlabel("Campaigns per month(users engaged <5 months)")
plt.ylabel("Clicks")
plt.title("No of Campaigns vs Clicks")
plt.legend(['clicks','clicks-eng'])


# In[ ]:




# In[22]:

#check unique clicks vs opens per campaign in email_campaigns (for those i have a start date)
unique_clicks=pd.read_sql("SELECT id,count(distinct(email_id)) FROM email_campaigns camp LEFT OUTER JOIN email_activity act ON camp.id=act.campaign_id WHERE action='click' GROUP BY id", engine,['id'])
#print(unique_clicks)
unique_opens=pd.read_sql("SELECT id,count(distinct(email_id)) FROM email_campaigns camp LEFT OUTER JOIN email_activity act ON camp.id=act.campaign_id WHERE action='open' GROUP BY id", engine,['id'])
#print(unique_opens)


# In[23]:

#checking unique clicks vs opens only including users that were active at most 4 months before the campaign start
# text to help with the subsequent queries
engaged_emails_text="SELECT act1.email_id FROM email_campaigns camp1,email_activity act1 WHERE act1.action='click' AND (12*(DATE_PART('year', camp1.send_time) - DATE_PART('year', act1.timestamp))+ (DATE_PART('month', camp1.send_time) - DATE_PART('month', act1.timestamp)))<4 "


# In[24]:

unique_clicks_engaged=pd.read_sql("SELECT id,count(distinct(email_id)) FROM email_campaigns camp LEFT OUTER JOIN email_activity act ON camp.id=act.campaign_id WHERE action='click' AND email_id in ("+engaged_emails_text+" AND camp.id=camp1.id)GROUP BY id", engine,['id'])
#print(unique_clicks_engaged)
unique_opens_engaged=pd.read_sql("SELECT id,count(distinct(email_id)) FROM email_campaigns camp LEFT OUTER JOIN email_activity act ON camp.id=act.campaign_id WHERE action='open' AND email_id in ("+engaged_emails_text+" AND camp.id=camp1.id)GROUP BY id", engine,['id'])
#print(unique_opens_engaged)


# In[25]:

print(unique_clicks/unique_opens-unique_clicks_engaged/unique_opens_engaged)


# In[26]:

plt.hist((unique_clicks/unique_opens-unique_clicks_engaged/unique_opens_engaged)['count'])
plt.title("Difference(%) clicks/opens vs clicks/opens(engaged)")
plt.xlabel("Difference")
plt.ylabel("Count")


# In[27]:

#check avg click and open rate per list
list_click_rate=pd.read_sql("SELECT list_id,avg(stats_avg_click_rate) FROM email_lists GROUP BY list_id order by avg(stats_avg_click_rate) desc", engine,['list_id'])
print(list_click_rate)
list_open_rate=pd.read_sql("SELECT list_id,avg(stats_avg_open_rate) FROM email_lists GROUP BY list_id order by avg(stats_avg_open_rate) desc", engine,['list_id'])
print(list_open_rate)


# In[28]:

#check click rate of each campaign vs recipient's list
lists_avg_click_rate=pd.read_sql("SELECT recipients_list_id,AVG(report_summary_click_rate) FROM email_campaigns GROUP BY recipients_list_id", engine,['recipients_list_id'])
print(lists_avg_click_rate)


# In[29]:

#check list size per month(will be used later)
list_subscribers_per_month=pd.read_sql("SELECT list_id,DATE_PART('year', last_changed) as year,DATE_PART('month', last_changed) as month,count(*) FROM email_lists WHERE status='subscribed' GROUP BY list_id,DATE_PART('year', last_changed),DATE_PART('month', last_changed) order by list_id,DATE_PART('year', last_changed),DATE_PART('month', last_changed)", engine,['list_id','year','month'])
#print(list_subscribers_per_month)
list_unsubscribers_per_month=pd.read_sql("SELECT list_id,DATE_PART('year', last_changed) as year,DATE_PART('month', last_changed) as month,count(*) FROM email_lists WHERE status='unsubscribed' GROUP BY list_id,DATE_PART('year', last_changed),DATE_PART('month', last_changed) order by list_id,DATE_PART('year', last_changed),DATE_PART('month', last_changed)", engine,['list_id','year','month'])
#print(list_unsubscribers_per_month)
print(list_subscribers_per_month.subtract(list_unsubscribers_per_month,fill_value=0))


# In[ ]:




# In[30]:

#get data from tables
data=pd.read_sql('SELECT * FROM email_campaigns', engine,['id'])
#print(data.head())


# In[31]:

#add data calculated before
click_rates={'cd055c6fe3':0.043190,'180b7eeb41':0.033256}
open_rates={'180b7eeb41':0.325968,'cd055c6fe3':0.216917}
data['list_click_rate']=data['recipients_list_id'].map(click_rates)
data['list_open_rate']=data['recipients_list_id'].map(open_rates)


# In[32]:

#remove non useful columns
del data['delivery_status_enabled'] # all 'f'


# In[33]:

del data['recipients_recipient_count'] # same as 'emails_sent'
del data['create_time'] #not needed


# In[34]:

#remove rows of list 9375e3c354 - not enough data
data=data[data['recipients_list_id']!='9375e3c354']


# In[35]:

#remove rows of campaings after 12/2016 - not enough time has passed to evaluate the campaign
data=data[data['send_time']<'2016-12']


# In[36]:

# functions to calculate the 'size growth' of the list during the first 3 months of the campaign
growth_18_2016= {2:-3.0, 3:-10,4:-10, 5:-22, 6:-26,7:-26, 8:-27, 9:-12,10:-9,11:780,12:41}
growth_18_2017={1: 98,2:2,3:0,4:0}

growth_cd_2016= {5:56, 6:41, 7:31, 8:90, 9:17, 10:18, 11:19, 12:39}
growth_cd_2017={1:50,2:1,3:0,4:0}

def get_growth(list_id,send_time,offset):
    lst=[]
    for i in range(0,len(list_id)):
        year1=send_time[i].year
        month1=send_time[i].month+offset
        if(month1==13):
            year1=year1+1
            month1=1
        elif(month1==14):
            year1=year1+1
            month1=2
        
        result1=get_growth_data(list_id[i],year1,month1)
        lst.append(result1)       
    return lst

def get_growth_data(list_id,year,month):
    if list_id=='180b7eeb41' :
        if year==2016:
            return growth_18_2016[month]
        elif year==2017:
            return growth_18_2017[month]
        else:
            print("wrong year "+year)
    elif list_id=='cd055c6fe3':
        if year==2016:
            return growth_cd_2016[month]
        elif year==2017:
            growth_cd_2017[month]
        else:
            print("wrong year "+year)
    else:
        print("wrong id : "+list_id)
        
      
        


# In[37]:

data['month1']=(get_growth(data['recipients_list_id'],data['send_time'],0))
data['month2']=(get_growth(data['recipients_list_id'],data['send_time'],1))
data['month3']=(get_growth(data['recipients_list_id'],data['send_time'],2))


# In[38]:

data['engaged_due_to_clicks']=unique_clicks_engaged
data['engaged_due_to_opens']=unique_opens_engaged
data['engaged_due_to_clicks']=data['engaged_due_to_clicks'].fillna(0)
data['engaged_due_to_opens']=data['engaged_due_to_opens'].fillna(0)


# In[39]:

#send_time is too complicated i ll keep just year and month
data['year']=data['send_time'].dt.year
data['month']=data['send_time'].dt.month


# In[40]:

#remove send_time
del data['send_time']


# In[41]:

#change list id to int
lists={'180b7eeb41':1, 'cd055c6fe3':2}
data['list_int_id']=data['recipients_list_id'].map(lists)
#remove recipients_list_id
del data['recipients_list_id']


# In[42]:

# to make things simpler i create a column that indicates whether a campaign was succesful or not
#(i chose 7% as the limit of success for the click rate)
data['success']=(data['report_summary_click_rate']>0.07).astype(int)


# In[43]:

#Check the correlations to find possible connections between the data
data.corr()


# In[44]:

import seaborn as sns
corr=data.corr()
sns.heatmap(corr, xticklabels=corr.columns.values, yticklabels=corr.columns.values)


# In[45]:

plt.scatter(data['engaged_due_to_clicks'],data['report_summary_clicks'])


# In[46]:

plt.scatter(data['list_click_rate'],data['report_summary_clicks'])


# In[47]:

from statsmodels.formula.api import logit


# In[48]:

email_logit = logit("success ~ emails_sent + list_click_rate + engaged_due_to_clicks + list_int_id", data).fit()
email_logit.summary()


# In[50]:

from sklearn.tree import DecisionTreeClassifier

clf = DecisionTreeClassifier()
clf = clf.fit(data[['emails_sent','month1','month2','engaged_due_to_clicks','list_int_id']], data['success'])
clf


# In[51]:

from sklearn import tree

from sklearn.externals.six import StringIO
from IPython.display import Image  
import pydotplus


# In[52]:

dot_data = StringIO()  
tree.export_graphviz(clf, out_file=dot_data,  
                     feature_names=['emails_sent','month1','month2','engaged_due_to_clicks','list_int_id'],  
                     #class_names=iris.target_names,  
                     filled=True, rounded=True,  
                     special_characters=True)  
graph = pydotplus.graph_from_dot_data(dot_data.getvalue())  
Image(graph.create_png())  

