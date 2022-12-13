import datetime
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.DEBUG)

class hourly_trend:

    def __init__(self,dataframe,start_time,end_time,step=None):

        self._dataframe = dataframe
        self._start_time = start_time
        self._end_time = end_time
        self._step = step

    def filtered_dataframe(self):
        logging.debug(f'Operation started for {self._start_time}--- {self._end_time}')
        start_time = self._start_time
        end_time=self._end_time
        df=self._dataframe
        logging.debug(f'Table Generation completed for {self._start_time} --- {self._end_time}')
        return (df[df['Time'].between(start_time,end_time)])

    def user_calc(self):
        df = self.filtered_dataframe()
        no_of_user = df['user_AuthenticatedId'].nunique()
        return no_of_user

    def load_cal(self):
        df = self.filtered_dataframe()
        load = df.name_x.value_counts().reset_index(name='count')['count'].sum()
        return load


    def cloud_role_vol(self):
        df = self.filtered_dataframe()
        cloud_vol = df.cloud_RoleName_x.nunique()
        return cloud_vol
    
    def api_vol(self):
        df = self.filtered_dataframe()
        api_volm=df.name_x.nunique()
        return api_volm

    def fail_count(self):
        df= self.filtered_dataframe()
        failure_count= df[df.success==False]['success'].count()
        return failure_count
    
    def succes_count(self):
        df= self.filtered_dataframe()
        pass_count= df[df.success==True]['success'].count()
        return pass_count

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

    def row_return_singular(self):
        time = self._start_time + '-' + self._end_time
        user_calc = self.user_calc()
        load_cal = self.load_cal()
        cloud_vol = self.cloud_role_vol()
        api_volm = self.api_vol()
        failure_count = self.fail_count()
        pass_count = self.succes_count()
        list = [time,user_calc,load_cal,cloud_vol,api_volm,failure_count,pass_count]
        row_df = pd.DataFrame([list],columns=('time_range','no_user','load','cloud_vol','api_volm','failure_count','pass_count'))
        return row_df

    def multiple_table(self):

        zip_dict=self.iterator_logic()
        df_list = []
        
        for key,value in zip_dict.items(): 
            self._start_time = key
            self._end_time = value         
            rdf = self.row_return_singular()        
            df_list.append(rdf)

        return(pd.concat(df_list))

    def dynamic(self):
        df= self.multiple_table()
        org_df=self.filtered_dataframe()
        li=df['time_range'].values
        li2=[]
        for i in li:
            li2.append(i.split('-')[0])
        df['Time']=li2
        date=org_df.date.values[0]
        logging.debug(f'Date Value is {date}')
        df['date']=date
        df['date']=df['date'].apply(lambda x : x.strftime('%Y-%m-%d'))
        df['date_time']=df['date'] + ' ' + df.Time
        df['date_time']=pd.to_datetime(df['date_time'],format='%Y-%m-%d %H:%M:%S')
        return df[['Time','date','date_time','time_range','no_user','load','cloud_vol','api_volm','failure_count','pass_count']]
    