import datetime
import pandas as pd
import numpy as np


class cloudrolename_hourly:

    def __init__(self,dataframe,start_time,end_time,Cloud_RoleName,step=None):

        self._dataframe = dataframe
        self._start_time = start_time
        self._end_time = end_time
        self._cloud_role_name = Cloud_RoleName
        self._step = step

    def filtered_dataframe(self):
        start_time = self._start_time
        end_time=self._end_time
        df=self._dataframe
        return (df[df['Time'].between(start_time,end_time)])

    def grouped_cloud_return(self):
        cloud_role_name = self._cloud_role_name
        df= self.filtered_dataframe()
        cnm=[cloud_role_name]
        c=[df[df.Time.between(self._start_time,self._end_time)&(df.cloud_RoleName_x==cloud_role_name)]['user_AuthenticatedId'].count()]
        tm=[self._start_time+'-'+self._end_time]
        mx=pd.DataFrame([tm,cnm,c]).T
        mx.columns=['time_range','cloudrolename','numbers_of_users']
        grouped_cloud1= mx.replace(np.nan,0)
        return grouped_cloud1

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
            rdf = self.grouped_cloud_return()        
            df_list.append(rdf)

        return(pd.concat(df_list))

    def dynamic_cloud(self):
        df= self.multiple_table()
        li=df['time_range'].values
        li2=[]
        for i in li:
            li2.append(i.split('-')[0])
        df['Time']=li2
        return df[['Time','time_range','cloudrolename','numbers_of_users']]
