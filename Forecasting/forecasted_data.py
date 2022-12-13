from Forecasting.RandomForestAR import RandomForestARModel
import numpy as np
import pandas as pd
import pickle
import os
from pathlib import Path

class forecasting_prepration(RandomForestARModel):

    def __init__(self,data,analyzer,split,time):
        self._data = data
        self._analyzer = analyzer
        self._split = split
        self._time = time
        self._loaded_model = None
    
    def train_test_split(self):


        data = self._data
        req_data = data[[self._analyzer,'Time']]
        req_data.Time = pd.to_datetime(req_data.Time)
        req_data.index = req_data.Time 
        req_data.drop(columns='Time',inplace=True)
        

        if self._split==1:
            if self._time == 12:
                s_req_data = req_data[:181]
                train_df = s_req_data[:151]
                test_df = s_req_data[150:]
                return(train_df,test_df)
        if self._split==5:
            if self._time == 12:
                s_req_data = req_data[:37]
                train_df = s_req_data[:32]
                test_df = s_req_data[31:]
                return(train_df,test_df)
        
    def model(self,forecast_period):
        self.forecast_period = forecast_period
        train,test=self.train_test_split()

        if self._analyzer=='no_user':
            self.loaded_model = pickle.load(open(r'C:\Users\ankita.tiwari\Desktop\Omnia_Log_Analystics\saved_models\_model_no_user_pickle.pkl','rb'))
        else:
            self.loaded_model = pickle.load(open(r'C:\Users\ankita.tiwari\Desktop\Omnia_Log_Analystics\saved_models\_model_no_user_pickle.pkl','rb'))


        # Doing 30 minutes forecasting from 12pm till 12:30pm
        predictions_forest = self.loaded_model.sample_forecast(n_periods=self.forecast_period, n_samples=94,random_seed=10000)
        means_forest = np.mean(predictions_forest,1)
        df = pd.DataFrame([test.index,means_forest]).T
        df.columns = ['Time','30_minAheadForecasted']
        if self._analyzer=='load':
            df.to_csv(r'.\FORECASTING\load\future_30_minload.csv')
        else:
           df.to_csv(r'.\FORECASTING\no_user\future_30_minNo_user.csv') 


# df = pd.read_csv(r'C:\Users\ankita.tiwari\Desktop\Omnia_Log_Analystics\one_minute_final_table.csv')
# b= forecasting_prepration(data= df,analyzer='load',split=1,time=12)
# b.model(forecast_period=30)



# with open("../FutureBookList/file.txt") as file:
#    data = file.read()
            

     






        