# -*- coding: utf-8 -*-
"""Python Implementation.ipynb


"""

#Libraries
import pandas as pd

"""## Getting the Data"""
file_path= '/content/drive/MyDrive/Semester 2/Big Data/Project'

data= pd.read_csv('before.csv',low_memory=False)

"""## Data Manipulation"""

# Custom function to extract given matching column
def extract(str_row, col_name_to_match):

  #Reading the data and making it a string
  s = str(str_row)
  #Removing the parantheses and Quotes from input
  s = s.strip('{[]}')
  s = s.replace('"','')
  #Splitting into different parameters
  lst_s = s.split(',')
  #Matching with the required column name
  for col in lst_s:
    if col.split(':')[0] == col_name_to_match:
      if col.split(':')[1] != 'null':
        return col.split(':')[1]


# Initializing n for column insertion
n =1
# 1. Deleting 'visitorId'
data = data.drop(columns=[ 'visitorId'])

# 2-a. Changing field 'date' to date type
data['date'] = pd.to_datetime(data["date"], errors='coerce', format='%Y%m%d')
# 2-b. Changing format of 'date' to M/d/yyyy
data['date'] = data.date.dt.strftime("%m/%d/%Y")

# 3-a. Creating new column 'visitStart' from 'visitStartTime' at index 2
n+=1
data.insert(n, 'visitStart', pd.to_datetime(data["visitStartTime"], errors='coerce', unit='s'))
n+=1
# 3-b. Extracting only the time part
data['visitStart'] = data.visitStart.dt.strftime("%H:%M:%S")

# 4. Dropping 'visitStartTime' column
data = data.drop(columns=[ 'visitStartTime'])

# 5. Converting 'visitId' to type String
data['visitId'] = data['visitId'].astype('str') 
n+=1

# 6. Creating a new column 'hits1' extracted from totals
data.insert(n,'hits1',data['totals'].apply(lambda x: extract(x,'hits')))
n+=1

# 7. Creating a new column 'newVisits' extracted from totals
data.insert(n,'newVisits',data['totals'].apply(lambda x: extract(x,'newVisits')))
n+=1

# 8. Creating a new column 'timeOnSite' extracted from totals
data.insert(n,'timeOnSite',data['totals'].apply(lambda x: extract(x,'timeOnSite')))
n+=1

# 9. Creating a new column 'transactionRevenue' extracted from totals
data.insert(n,'transactionRevenue',data['totals'].apply(lambda x: extract(x,'transactionRevenue')))
n+=1

# 10. Dropping column 'totals'
data = data.drop(columns=[ 'totals'])

# 11. Replacing missing values in 'transactionRevenue'  with 0
data['transactionRevenue'] = data['transactionRevenue'].fillna(0)

# 12. Replacing missing values in 'timeOnSite'  with 0
data['timeOnSite'] = data['timeOnSite'].fillna(0)

# 13. Replacing missing values in 'newVisits'  with 0
data['newVisits'] = data['newVisits'].fillna(0)

# 14. Creating a new column 'referralPath' extracted from 'trafficSource'
data.insert(n,'referralPath',data['trafficSource'].apply(lambda x: extract(x,'referralPath')))
n+=1

# 15. Creating a new column 'browser' extracted from 'device'
data.insert(n,'browser',data['device'].apply(lambda x: extract(x,'browser')))
n+=1

# 16. Creating a new column 'deviceCategory' extracted from 'device'
data.insert(n,'deviceCategory',data['device'].apply(lambda x: extract(x,'deviceCategory')))
n+=1

# 17. Creating a new column 'isMobile' extracted from 'device'
data.insert(n,'isMobile',data['device'].apply(lambda x: extract(x,'isMobile')))
n+=1

# 18. Dropping column 'trafficSource'
data = data.drop(columns=[ 'trafficSource'])

# 19. Dropping column 'device'
data = data.drop(columns=[ 'device'])

# 20. Creating a new column 'city' extracted from 'geoNetwork'
data.insert(n,'city',data['geoNetwork'].apply(lambda x: extract(x,'city')))
n+=1

# 21. Creating a new column 'country' extracted from 'geoNetwork'
data.insert(n,'country',data['geoNetwork'].apply(lambda x: extract(x,'country')))
n+=1

# 22. Dropping column 'geoNetwork'
data = data.drop(columns=[ 'geoNetwork'])

# 23. Creating a new column 'value' extracted from 'customDimensions'
data.insert(n,'value',data['customDimensions'].apply(lambda x: extract(x,'value')))
n+=1

# 24. Renaming column 'value' to 'region'
data = data.rename(columns={"value": "region"})

# 25. Dropping column 'customDimensions'
data = data.drop(columns=[ 'customDimensions'])

# 26. Dropping column 'userId'
data = data.drop(columns=[ 'userId'])

# 27. Dropping column 'clientId'
data = data.drop(columns=[ 'clientId'])

# 28. Creating a new column 'page/pageTitle' extracted from 'hits'
data.insert(n,'pageTitle',data['hits'].apply(lambda x: extract(x,'pageTitle')))

# 29. Dropping column 'hits'
data = data.drop(columns=[ 'hits'])

# 30. Dropping all 'Unnamed' columns
data = data.loc[:, :'socialEngagementType']

#Cleaning out unnecessary data
#Removing String values from 'visitId'
data['visitId'] = pd.to_numeric(data['visitId'], errors='coerce')
data = data.dropna(subset=['visitId'])
#Removing string values from 'visitNumber'
data['visitNumber'] = pd.to_numeric(data['visitNumber'], errors='coerce' )
data = data.dropna(subset=['visitNumber'])



data.to_csv('after.csv', index=False)