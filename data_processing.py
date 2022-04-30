# -*- coding: utf-8 -*-
"""
Created on Thu Apr  7 09:16:29 2022

@author: nimda
"""

import os
import numpy as np
import pandas as pd
import glob

#Percipitation

Percipitation_path= "C:/Works/WNV/Weather/python/Precipitation/"
csv_files = glob.glob(os.path.join(Percipitation_path, "*.csv"))

for files in csv_files:

    percipitation_data= pd.read_csv(files, header=None)
    # percipitation_data= pd.read_csv("C:/Works/WNV/Weather/python/Precipitation/AK-013-pcp-all-1-2000-2022.csv", header=None)
    percipitation_data=percipitation_data.rename(columns = {0:'Date', 1: 'Precipitation', 2:'Anomaly' })
    
    name= os.path.splitext(os.path.split(files)[1])[0]
    
    
    Date= ['200005', '200006', '200007', '200008', '200009', '200010', '200105', '200106', '200107',
           '200108', '200109', '200110', '200205', '200206', '200207', '200208', '200209','200210',
           '200305', '200306', '200307', '200308', '200309', '200310', '200405', '200406', '200407',
           '200408', '200409', '200410', '200505', '200506', '200507', '200508', '200509','200510',
           '200605', '200606', '200607', '200608', '200609', '200610', '200705', '200706', '200707',
           '200708', '200709', '200710', '200805', '200806', '200807', '200808', '200809','200810',
           '200905', '200906', '200907', '200908', '200909', '200910', '201005', '201006', '201007',
           '201008', '201009', '201010', '201105', '201106', '201107', '201108', '201109','201110',
           '201205', '201206', '201207', '201208', '201209', '201210', '201305', '201306', '201307',
           '201308', '201309','201310', '201405', '201406', '201407', '201408', '201409', '201410',
           '201505', '201506', '201507', '201508', '201509','201510', '201605', '201606', '201607',
           '201608', '201609', '201610', '201705', '201706', '201707', '201708', '201709','201710',
           '201805', '201806', '201807', '201808', '201809', '201810', '201905', '201906', '201907',
           '201908', '201909','201910', '202005', '202006', '202007', '202008', '202009', '202010',
           '202105', '202106', '202107', '202108', '202109','202110', '202205', '202206', '202207',
           '202208', '202209', '202210', '201705', '201706', '201707', '201708', '201709','201710']
    
    for i in range(5, len(percipitation_data)):
        percipitation_data.loc[i, 'State']= percipitation_data.iloc[0, 1]
        percipitation_data.loc[i, 'County']= percipitation_data.iloc[0, 0]
         
    percipitation_data= percipitation_data.reset_index()
    percipitation_data= percipitation_data[5:269]
    percipitation_data= percipitation_data.reset_index()
    percipitation_data= percipitation_data.drop(columns=['level_0', 'index', 'Anomaly'])
    state = percipitation_data.pop('State')
    percipitation_data.insert(1, 'State', state)
    county = percipitation_data.pop('County')
    percipitation_data.insert(2, 'County', county)
    percipitation_data= percipitation_data[percipitation_data['Date'].isin(Date)]
    percipitation_data= percipitation_data.reset_index()
    percipitation_data= percipitation_data.drop(columns=[ 'index'])
    
    percipitation_data.to_csv("C:/Works/WNV/Weather/python/Precipitation/Processed/"+name+".csv", index= False)
    
    
#####################################################################################################################


Processed_percipitation_path= "C:/Works/WNV/Weather/python/Precipitation/Processed"
Processed_files = glob.glob(os.path.join(Processed_percipitation_path, "*.csv"))

final_percipitation_data= pd.read_csv(Processed_files[0])

for files in range(1, len(Processed_files)):

    processed_percipitation_data= pd.read_csv(Processed_files[files])
    final_percipitation_data= pd.concat([final_percipitation_data, processed_percipitation_data])


final_percipitation_data[['State']]=final_percipitation_data[['State']].apply(lambda x : x.str.strip())   
final_percipitation_data.to_csv("C:/Works/WNV/Weather/python/final_percipitation_data.csv", index= False)

#####################################################################################################################

abrr= pd.read_csv("C:/Works/WNV/Weather/python/Abbrev.csv")
fips= pd.read_csv("C:/Works/WNV/Weather/python/Fips.csv")

final_percipitation_data= pd.read_csv("C:/Works/WNV/Weather/python/final_percipitation_data.csv")


fips= fips.rename(columns = {'State':'State_Code'})

final_percipitation_data= final_percipitation_data.merge(abrr, how= "left", on= "State")
Abbrev = final_percipitation_data.pop('Abbrev')
final_percipitation_data.insert(2, 'State_Code', Abbrev)


df= final_percipitation_data.merge(fips)

mask = df.apply(lambda x: str(x["Name"]) in x["County"], axis=1)

df = df[mask][["Date", "State", "State_Code", "County", "Precipitation", "FIPS"]]

fips_code = df.pop('FIPS')
df.insert(1, 'Fips', fips_code)
df.to_csv("C:/Works/WNV/Weather/python/final_percipitation_data.csv", index= False)


#####################################################################################################################

#Temprature

Temprature_path= "C:/Works/WNV/Weather/python/Temprature/"
csv_files = glob.glob(os.path.join(Temprature_path, "*.csv"))

for files in csv_files:

    Temprature_data= pd.read_csv(files, header=None)
    Temprature_data= Temprature_data.rename(columns = {0:'Date', 1: 'Temprature', 2:'Anomaly' })
    
    name= os.path.splitext(os.path.split(files)[1])[0]
    
    
    for i in range(5, len(Temprature_data)):
        Temprature_data.loc[i, 'State']= Temprature_data.iloc[0, 1]
        Temprature_data.loc[i, 'County']= Temprature_data.iloc[0, 0]
         
    Temprature_data= Temprature_data.reset_index()
    Temprature_data= Temprature_data[5:269]
    Temprature_data= Temprature_data.reset_index()
    Temprature_data= Temprature_data.drop(columns=['level_0', 'index', 'Anomaly'])
    state = Temprature_data.pop('State')
    Temprature_data.insert(1, 'State', state)
    county = Temprature_data.pop('County')
    Temprature_data.insert(2, 'County', county)
    Temprature_data= Temprature_data[Temprature_data['Date'].isin(Date)]
    Temprature_data= Temprature_data.reset_index()
    Temprature_data= Temprature_data.drop(columns=[ 'index'])
    
    Temprature_data.to_csv("C:/Works/WNV/Weather/python/Temprature/Processed/"+name+".csv", index= False)
    
    
#####################################################################################################################


Processed_Temprature_path= "C:/Works/WNV/Weather/python/Temprature/Processed"
Processed_files = glob.glob(os.path.join(Processed_Temprature_path, "*.csv"))

final_Temprature_data= pd.read_csv(Processed_files[0])

for files in range(1, len(Processed_files)):

    processed_Temprature_data= pd.read_csv(Processed_files[files])
    final_Temprature_data= pd.concat([final_Temprature_data, processed_Temprature_data])


final_Temprature_data[['State']]=final_Temprature_data[['State']].apply(lambda x : x.str.strip())   

#final_Temprature_data= final_Temprature_data.drop(columns=['level_0'])
# dt = final_Temprature_data.pop('Date')
# final_Temprature_data.insert(0, 'Date', dt)

final_Temprature_data.to_csv("C:/Works/WNV/Weather/python/final_Temprature_data.csv", index= False)

#####################################################################################################################

abrr= pd.read_csv("C:/Works/WNV/Weather/python/Abbrev.csv")
fips= pd.read_csv("C:/Works/WNV/Weather/python/Fips.csv")

final_Temprature_data= pd.read_csv("C:/Works/WNV/Weather/python/final_Temprature_data.csv")


fips= fips.rename(columns = {'State':'State_Code'})

final_Temprature_data= final_Temprature_data.merge(abrr, how= "left", on= "State")
Abbrev = final_Temprature_data.pop('Abbrev')
final_Temprature_data.insert(2, 'State_Code', Abbrev)


df= final_Temprature_data.merge(fips)

mask = df.apply(lambda x: str(x["Name"]) in x["County"], axis=1)

df = df[mask][["Date", "State", "State_Code", "County", "Temprature", "FIPS"]]

fips_code = df.pop('FIPS')
df.insert(1, 'Fips', fips_code)
df.to_csv("C:/Works/WNV/Weather/python/final_Temprature_data.csv", index= False)

#####################################################################################################################

temp= pd.read_csv("C:/Works/WNV/Weather/python/final_Temprature_data.csv")
preci= pd.read_csv("C:/Works/WNV/Weather/python/final_percipitation_data.csv")

preci= preci[['Fips','County', "Date", 'Precipitation']]

temp= temp.reset_index(drop=True)
preci= preci.reset_index(drop=True)


weather_data = pd.merge(temp, preci,  how='left', on = ['Fips','County', "Date"])


