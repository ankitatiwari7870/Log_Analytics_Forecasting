import pandas as pd
import numpy as np
import datetime

import logging
logging.basicConfig(level=logging.DEBUG)

class server_hourly:

    def __init__(self,dataframe,start_time,end_time,server_name,step=None):

        self._dataframe = dataframe
        self._start_time = start_time
        self._end_time = end_time
        self._serverName = server_name
        self._step = step
        date_log=dataframe['date'][0]
        logging.debug(f'Operation started for date {date_log}')

    def filtered_dataframe(self):
        logging.debug(f'Dataframe Filtration started  {self._start_time}---{self._end_time} for {self._step} intervals')
        start_time = self._start_time
        end_time=self._end_time
        df=self._dataframe
        logging.debug(f'Dataframe Filtration Completed  {self._start_time}---{self._end_time} for {self._step} intervals')
        return (df[df['Time'].between(start_time,end_time)])

    def grouped_server_return(self):
        server_name = self._serverName
        df= self.filtered_dataframe()
        serv=[server_name]
        c=[df[df.Time.between(self._start_time,self._end_time)&(df.url_resgrp_page==server_name)]['url_resgrp_page'].count()]
        tm=[self._start_time+'-'+self._end_time]
        mx=pd.DataFrame([tm,serv,c]).T
        mx.columns=['time_range','server','hits']
        grouped_server1= mx.replace(np.nan,0)
        return grouped_server1

    # Converting string into time.
    def str_to_time(self,str):
        spl=str.split(':')
        jn=''.join(spl)
        time=pd.to_datetime(jn,format='%H%M%S')
        return time

    def time_calc(self,time,step):

        inc_time = str(datetime.timedelta(hours=time.hour, minutes=time.minute, seconds=time.second)+datetime.timedelta(minutes=step))
        l_=inc_time.split(':')
        if len(l_[0])==1:
            ns = '0'+l_[0]
            l_[0]=ns
            strx=':'.join(l_)
            return strx
        else:
            return(inc_time)
            
    def range_finder(self,start,end,step):
        # start = self._start_time
        # end = self._end_time
        # step = self._step
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
        return dc

    def multiple_table(self):
        
        zip_dict=self.iterator_logic()
        df_list = []
        
        for key,value in zip_dict.items(): 
            self._start_time = key
            self._end_time = value         
            rdf = self.grouped_server_return()        
            df_list.append(rdf)

        return(pd.concat(df_list))

    def dynamic_server(self):
        df= self.multiple_table()
        org_df=self.filtered_dataframe()
        li=df['time_range'].values
        li2=[]
        for i in li:
            li2.append(i.split('-')[0])
        df['Time']=li2
        df['date']=org_df.date.values[0]
        df['date']=df['date'].apply(lambda x : x.strftime('%Y-%m-%d'))
        df['date_time']=df['date'] + ' ' + df.Time
        df['date_time']=pd.to_datetime(df['date_time'],format='%Y-%m-%d %H:%M:%S')
        logging.debug(f'Dataframe Filtration Task Completed for {self._step} intervals')
        return df[['Time','date','date_time','time_range','server','hits']]
