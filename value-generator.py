import pandas as pd
import random
import datetime
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
df=pd.DataFrame(columns=['deviceid','timestamp','datatype','value'])
df2=pd.DataFrame(columns=['deviceid','timestamp','datatype','value'])
df3=pd.DataFrame(columns=['deviceid','timestamp','datatype','value'])
# Function to Generate temperature values
def temperature(loopCount):

    message = {}
    message['deviceid'] = 'BSM_G101'

    dates = pd.date_range('20200301', periods=loopCount, freq='15S')
    timestamp = pd.Series(range(len(dates)), index=dates)
    df.loc[:, 'timestamp'] = dates


    for x in range(loopCount):
        value = float(random.normalvariate(99, 1.5))
        value = round(value, 1)


        df.loc[x,'deviceid'] = 'BSM_G101'
        df.loc[x,'datatype'] = 'Temperature'
        df.loc[x,'value'] = value

# Function to Generate Blood O2 values
def blood(loopCount):

    dates = pd.date_range('20200301', periods=loopCount, freq='10S')
    timestamp = pd.Series(range(len(dates)), index=dates)
    df2.loc[:, 'timestamp'] = dates
    for x in range(loopCount):
        value = int(random.normalvariate(90, 3.0))
        timestamp = str(datetime.datetime.now())
        df2.loc[x, 'deviceid'] = 'BSM_G101'
        df2.loc[x, 'datatype'] = 'Sp02'
        df2.loc[x, 'value'] = value
# Function to Generate Heartrate values
def heartrate(loopCount):
    dates = pd.date_range('20200301', periods=loopCount, freq='S')
    timestamp = pd.Series(range(len(dates)), index=dates)
    df3.loc[:, 'timestamp'] = dates
    for x in range(loopCount):

        value = int(random.normalvariate(85, 12))
        df3.loc[x, 'deviceid'] = 'BSM_G101'
        df3.loc[x, 'datatype'] = 'HeartRate'
        df3.loc[x, 'value'] = value


## Passing values to generate 1 hour data at different time intervals
temperature(240)
blood(360)
heartrate(3600)
## Combining the dataframes to one
result = pd.concat([df, df2,df3])
df4=result.sort_values(by=['timestamp'])
df4=df4.reset_index()
df4=df4.drop(['index'],axis=1)

print(df4)

## Block to insert values to SQL Database
import mysql.connector

mydb= mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql032991",
    database='timeseries'

)

#mycursor = mydb.cursor()

#mycursor.execute("CREATE TABLE sensor (deviceid VARCHAR(255), timestamp datetime(6) ,datatype VARCHAR(255), value DECIMAL(20,1))")
for i, row in df4.iterrows():
    sql="INSERT INTO sensor (deviceid,timestamp,datatype,value) VALUES(%s,%s,%s,%s)"
    val=(row.deviceid,str(row.timestamp),row.datatype,row.value)
   # mycursor.execute(sql,val)

#mydb.commit()













