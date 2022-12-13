import datetime
import pandas as pd
import numpy as np


import hourly_tableDynamic
from hourly_tableDynamic import hourly_trend

class page_view_daily(hourly_trend):
    def __init__(self,dataframe):
        self._dataframe = dataframe

        self.time_list_all = ['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00','09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00','18:00','19:00',\
    '20:00','21:00','22:00','23:00']

    def logic(self):
        '''
        logic for generating trend table in 24 hours as a dict

        '''

        len_t=len(self.time_list_all)
        list_range=list(range(len_t))
        strt=[i for i in list_range]
        end=[j for j in list_range[1:]]
        zip_dict=dict(zip(strt,end))
        df_list = []
        
        for key,value in zip_dict.items():
            start_time=self.time_list_all[key]
            end_time=self.time_list_all[value] 
            self._start_time = start_time
            self._end_time = end_time         
            object = hourly_trend(dataframe=self._dataframe,start_time=start_time,end_time=end_time)
            rdf = object.row_return()
            df_list.append(rdf)

        return(pd.concat(df_list))

    # Converting string into time.
    def str_to_time(self,str):
        spl=str.split(':')
        jn=''.join(spl)
        time=pd.to_datetime(jn,format='%H%M%S')
        return time

    def time_calc(self,time,step):

        inc_time = str(datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)+datetime.timedelta(minutes=step))
        return inc_time
            
    def range_finder(self,start,end,step):

        diff = datetime.timedelta(hours= end.hour, minutes= end.minute, seconds=end.second)-\
            datetime.timedelta(hours= start.hour, minutes= start.minute, seconds=start.second)
        step_sec = int(step * 60)
        range = (diff.seconds)/step_sec
        return int(range)

    def iterator_logic(self):

        s_time = self._start_time
        e_time = self._end_time
        steps = self._step

        st_li=[]
        et_li=[]
        counter=[s_time]
        rg=self.range_finder(start=self.str_to_time(s_time),end=self.str_to_time(e_time),step=steps)

        for i in range(1,rg+1):

            st_li.append(counter[-1])
            et_li.append(self.time_calc(self.str_to_time(counter[-1]),step=steps))
            counter.append(et_li[-1])

        dc=dict(zip(st_li,et_li))