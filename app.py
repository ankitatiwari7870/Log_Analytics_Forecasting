import pandas as pd
import numpy as np
import datetime
import os
import glob
import logging
logging.basicConfig(level=logging.DEBUG)

from Forecasting.forecasted_data import forecasting_prepration
from Data_Extraction.dataprepration import data_prepration
from Data_Extraction.hourly_tableDynamic import hourly_trend



def data_merger(path_req,path_page_v):
    use_cols_req=['timestamp [UTC]','name', 'url', 'success',
       'resultCode', 'performanceBucket','operation_Id','cloud_RoleName']
    use_cols_pv=["url", 'name','performanceBucket','operation_Id','user_AuthenticatedId','cloud_RoleName']
    logging.debug('glob task started')
    csv_files_req = glob.glob(os.path.join(path_req, "*.csv"))
    csv_file_page = glob.glob(os.path.join(path_page_v, "*.csv"))
    
    req=pd.concat([pd.read_csv(f,usecols=use_cols_req) for f in csv_files_req ],axis=0,ignore_index=True)
    logging.debug('request files concantened succesfully')
    pv=pd.concat([pd.read_csv(f,usecols=use_cols_pv) for f in csv_file_page ],axis=0,ignore_index=True)
    logging.debug('pageView files concantened succesfully')
    
    mdf=pd.merge(req,pv,on='operation_Id',how='inner')
    logging.debug('Merged completed succesfully')
    return mdf


path = os.getcwd()
csv_files = glob.glob(os.path.join(path, "*.csv"))
csv_files
path_req=path+r'\RAW_DATA\request_raw'
path_pv=path+r'\RAW_DATA\page_viewRaw'
print(path)
print(path_req)
print(path_pv)


merged_data = data_merger(path_req,path_pv)
print(merged_data.shape)

ob1=data_prepration(merged_data=merged_data)
cleaned_data=ob1.preprocessing()

ob2=hourly_trend(dataframe=cleaned_data,start_time='09:00:00',end_time='10:00:00',step=30)
trend_table=ob2.dynamic()

print(trend_table.columns)
print(trend_table)



newpath = f'.\OUTPUT\TREND\{trend_table.date.values[0][5:7]}' 
if not os.path.exists(newpath):
    os.makedirs(newpath)
    trend_table.to_csv(f'OUTPUT\TREND\{trend_table.date.values[0][5:7]}\{trend_table.date.values[0]}.csv')









