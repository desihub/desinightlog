#Imports
import os, sys
import base64
import glob
import time, sched
import datetime 
from datetime import timezone
from datetime import timedelta
from collections import OrderedDict
import numpy as np
import pandas as pd
import socket
import psycopg2
import subprocess
import pytz
import json
import ephem

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from bokeh.io import curdoc, save, export_png  # , output_file, save
from bokeh.models import (TextInput, ColumnDataSource, DateFormatter, RadioGroup,CheckboxButtonGroup,Paragraph, Button, TextAreaInput, Select,CheckboxGroup, RadioButtonGroup, DateFormatter,CheckboxGroup)
from bokeh.models.widgets.markups import Div
from bokeh.layouts import layout, column, row
from bokeh.models.widgets import Panel, Tabs, FileInput
from bokeh.models.widgets.tables import DataTable, TableColumn
from bokeh.plotting import figure
import logging
from astropy.time import TimezoneInfo
import astropy.units.si as u

#from util import sky_calendar

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

sys.path.append(os.getcwd())
sys.path.append('./ECLAPI-8.0.12/lib')
import nightlog as nl
from layout import Layout

class Report(Layout):
    def __init__(self):
        Layout.__init__(self)

        self.test = False 

        self.report_type = None 
        self.kp_zone = TimezoneInfo(utc_offset=-7*u.hour)

        self.datefmt = DateFormatter(format="%m/%d/%Y %H:%M:%S")
        self.timefmt = DateFormatter(format="%m/%d %H:%M")

        # Figure out where the App is being run: KPNO or NERSC
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        if 'desi' in hostname:
            self.location = 'kpno'
            self.conn = psycopg2.connect(host="desi-db", port="5442", database="desi_dev", user="desi_reader", password="reader")
        elif 'app' in hostname: #this is not true. Needs to change.
            self.location = 'nersc'
            self.conn = psycopg2.connect(host="db.replicator.dev-cattle.stable.spin.nersc.org", port="60042", database="desi_dev", user="desi_reader", password="reader")
        else:
            self.location = 'nersc'
        print(os.environ['NL_DIR'])
        self.nw_dir = os.environ['NW_DIR']
        self.nl_dir = os.environ['NL_DIR']     

        self.intro_subtitle = Div(text="Connect to Night Log", css_classes=['subt-style'])
        self.time_note = Div(text="<b> Note: </b> Enter all times as HH:MM (18:18 = 1818 = 6:18pm) in Kitt Peak local time. Either enter the time or hit the <b> Now </b> button if it just occured.", css_classes=['inst-style'])
        self.exp_info = Div(text="Mandatory fields have an asterisk*.", css_classes=['inst-style'],width=500)
        
        self.img_upinst = Div(text="Include images in the Night Log by uploading a png image from your local computer. Select file, write a comment and click Add", css_classes=['inst-style'], width=1000)
        self.img_upinst2 = Div(text="           Choose image to include with comment:  ", css_classes=['inst-style'])
        self.img_upload = FileInput(accept=".png")
        self.img_upload.on_change('value', self.upload_image)

        self.img_upload_comments_os = FileInput(accept=".png")
        self.img_upload_comments_os.on_change('filename', self.upload_image_comments_os)
        self.img_upload_comments_dqs = FileInput(accept=".png")
        self.img_upload_comments_dqs.on_change('filename', self.upload_image_comments_dqs)
        self.img_upload_problems = FileInput(accept=".png")
        self.img_upload_problems.on_change('filename', self.upload_image_problems)

        self.current_img_name = None

        self.nl_file = None
        self.milestone_time = None
        self.plan_time = None
        self.full_time = None

        self.DESI_Log = None
        self.save_telem_plots = False
        self.buffer = Div(text=' ')
        self.my_name = 'None'

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)


    def clear_input(self, items):
        """ After submitting something to the log, this will clear the form.
        """
        if isinstance(items, list):
            for item in items:
                item.value = ' '
        else:
            items.value = ' '

    def get_exposure_list(self):
        try:
            current_exp = self.exp_select.value
            dir_ = os.path.join(self.nw_dir,self.night)
            exposures = []
            for path, subdirs, files in os.walk(dir_): 
                for s in subdirs: 
                    exposures.append(s)  
            x = list([str(int(e)) for e in list(exposures)])
            x = np.sort(x)[::-1]
            self.exp_select.options = list(x) 
            if current_exp in ['',' ',np.nan,None]:
                self.exp_select.value = x[0]
            else:
                self.exp_select.value = current_exp
        except:
            self.exp_select.options = []

    def update_nl_list(self):
        days = [f for f in os.listdir(self.nl_dir) if os.path.isdir(os.path.join(self.nl_dir,f))]
        days_ = []
        for day in days:
            try:
                int(day)
                days_.append(day)
            except:
                pass
        init_nl_list = np.sort([day for day in days_])[::-1][0:10]
        self.date_init.options = list(init_nl_list)
        self.date_init.value = init_nl_list[0]

    def select_exp(self, attr, old, new):
        self.exp_enter.value = self.exp_select.value 
    
    def add_all_to_bad_list(self):
        self.bad_all = True
        self.exp_layout_1.children[11] = self.exp_btn
        self.bad_exp_add()
        self.exp_alert.text = 'The whole exposure {} has been added to the bad exposure list'.format(self.bad_exp_val)
        self.clear_input([self.exp_time, self.exp_enter, self.exp_select, self.exp_comment])

    def add_some_to_bad_list(self):
        self.bad_all = False
        self.exp_layout_1.children[11] = self.bad_layout_2

    def get_nightsum(self):
        ns_date = self.ns_date_input.value
        ns = {}          
        ns_html = ''                                                                                                                
        for dir_, sdir, f in os.walk(self.nl_dir): 
            for x in f: 
                if 'NightSummary' in x: 
                    date = dir_.split('/')[-1]
                    ns[date] = os.path.join(dir_,x)
        try:
            filen = ns[ns_date]
            ns_html += open(filen).read()
            self.ns_html.text = ns_html
        except:
            self.ns_html.text = 'Cannot find NightSummary for this date'

    def ns_next_date(self):
        current_date = datetime.datetime.strptime(self.ns_date_input.value,'%Y%m%d') 
        next_night = current_date + timedelta(days=1)
        self.ns_date_input.value = next_night.strftime('%Y%m%d')
        self.get_nightsum()

    def ns_last_date(self):
        current_date = datetime.datetime.strptime(self.ns_date_input.value,'%Y%m%d')
        last_night = current_date - timedelta(days=1)
        self.ns_date_input.value = last_night.strftime('%Y%m%d')
        self.get_nightsum()

    def get_time(self, time):
        """Returns strptime with utc. Takes time zone selection
        """
        date = datetime.datetime.strptime(self.night,'%Y%m%d')
        try:
            b = datetime.datetime.strptime(time, '%H:%M')
        except:
            try:
                b = datetime.datetime.strptime(time, '%H%M')
            except:
                try:
                    b = datetime.datetime.strptime(time, '%I%M%p')
                except:
                    print(time)
                    print('need format %H%M, %H:%M, %H:%M%p')
        t = datetime.time(hour=b.hour, minute=b.minute)
        if t < datetime.time(hour=12,minute=0):
            d = date + datetime.timedelta(days=1)
        else:
            d = date
        tt = datetime.datetime.combine(d, t)
        try:
            return tt.strftime("%Y%m%dT%H:%M")
        except:
            return time

    def get_strftime(self, time):
        date = self.night
        d = datetime.datetime.strptime(date, "%Y%m%d")
        dt = datetime.datetime.combine(d,time)
        return dt.strftime("%Y%m%dT%H:%M")

    def get_night(self):
        try:
            date = datetime.datetime.strptime(self.date_init.value, '%Y%m%d')
        except:
            date = datetime.datetime.now().date()

        self.night = date.strftime("%Y%m%d")
        self.DESI_Log = nl.NightLog(self.night, self.location, self.logger)
        self.logger.info('Obsday is {}'.format(self.night))

    def _dec_to_hm(self,hours):
        #dec in seconds
        seconds = hours*3600
        hour = seconds // 3600
        minutes = (seconds % 3600) // 60
        sec = seconds % 60
        str_ = '{}:{}'.format(int(hours), str(int(minutes)).zfill(2))
        return str_

    def _hm_to_dec(self,hm):
        #hm is a str H:M
        tt = datetime.datetime.strptime(hm,'%H:%M')
        dt = tt - datetime.datetime.strptime('00:00','%H:%M')
        seconds = dt.total_seconds()
        dec = seconds/3600
        return dec

    def connect_log(self):
        """Connect to Existing Night Log with Input Date
        """
        self.get_night()
        if not os.path.exists(self.DESI_Log.obs_dir):
            for dir_ in [self.DESI_Log.obs_dir]:
                os.makedirs(dir_)
                self.connect_txt.text = 'Connected to Night Log for {}'.format(self.night)

        #Load appropriate layout for each observer
        self.observer = self.obs_type.active #0=LO; 1=SO
        if self.observer == 0:
            self.title.text = 'DESI Nightly Intake - Lead Observer'
            self.layout.tabs = [self.intro_tab, self.plan_tab, self.milestone_tab_0, self.exp_tab_0, self.prob_tab, self.weather_tab_0, self.check_tab,  self.nl_tab_0, self.ns_tab]
            self.time_tabs = [None, None, None, self.exp_time, self.prob_time, None, None, None]
            self.connect_txt.text = 'Connected to Night Log for {}'.format(self.night)
            self.report_type = 'LO'
        elif self.observer == 1:
            self.title.text = 'DESI Nightly Intake - Support Observer'
            self.layout.tabs = [self.intro_tab, self.milestone_tab_0, self.exp_tab_1, self.prob_tab, self.weather_tab_1, self.nl_tab_1, self.ns_tab]
            self.time_tabs = [None, None, self.exp_time, self.prob_time, None, None, None]
            self.connect_txt.text = 'Connected to Night Log for {}'.format(self.night)
            self.report_type = 'SO'
        elif self.observer == 2:
            self.title.text = 'DESI Nightly Intake - Non-Observer'
            self.layout.tabs = [self.intro_tab, self.exp_tab_2, self.prob_tab_1, self.weather_tab_1, self.nl_tab_1, self.ns_tab]
            self.time_tabs = [None, self.exp_time, self.prob_time, None, None, None]
            self.connect_txt.text = 'Connected to Night Log for {}'.format(self.night)
            self.report_type = 'NObs'
        else:
            self.connect_txt.text = 'Please identify if you are an observer'
            self.report_type = 'NObs'
        
       
        #Connec to NightLog       
        self.nl_file = self.DESI_Log.nightlog_html
        self.nl_subtitle.text = "Current DESI Night Log: {}".format(self.nl_file)

        meta_dict_file = self.DESI_Log._open_kpno_file_first(self.DESI_Log.meta_json)

        if os.path.exists(meta_dict_file):
            try:
                meta_dict = json.load(open(meta_dict_file,'r'))
                plan_txt_text="https://desi.lbl.gov/trac/wiki/DESIOperations/ObservingPlans/OpsPlan{}".format(self.night)
                self.plan_txt.text = '<a href={}>Tonights Plan Here</a>'.format(plan_txt_text)
                self.so_name_1.value = meta_dict['so_1_firstname']+' '+meta_dict['so_1_lastname']
                self.so_name_2.value = meta_dict['so_2_firstname']+' '+meta_dict['so_2_lastname']
                self.LO_1.value = meta_dict['LO_firstname_1']+' '+meta_dict['LO_lastname_1']
                self.LO_2.value = meta_dict['LO_firstname_2']+' '+meta_dict['LO_lastname_2']
                self.OA.value = meta_dict['OA_firstname']+' '+meta_dict['OA_lastname']
                self.plots_start = meta_dict['dusk_10_deg']
                self.plots_end = meta_dict['dawn_10_deg']
                self.display_current_header()
                self.current_nl()
                self.get_exposure_list()
            except Exception as e:
                self.connect_txt.text = 'Error with Meta Data File: {}'.format(e)
        else:
            print('here')
            self.connect_txt.text = 'Fill Out Observer Info'
            self.intro_layout.children[9] = self.update_layout
            self.update_log_status = True

        contributer_file = self.DESI_Log._open_kpno_file_first(self.DESI_Log.contributer_file)
        if os.path.exists(contributer_file):
            try:
               cont_txt = ''
               f =  open(contributer_file, "r")
               for line in f:
                   cont_txt += line
               self.contributer_list.value = cont_txt
            except Exception as e:
               self.connect_txt.text = 'Error with Contributer File: {}'.format(e)
        time_use_file = self.DESI_Log._open_kpno_file_first(self.DESI_Log.time_use)
        if not os.path.exists(time_use_file):
            self.update_log_status = True
            self.add_observer_info()
        try:
            df = pd.read_csv(time_use_file)
            data = df.iloc[0]
            self.obs_time.value =  self._dec_to_hm(data['obs_time'])
            self.test_time.value = self._dec_to_hm(data['test_time'])
            self.inst_loss_time.value = self._dec_to_hm(data['inst_loss'])
            self.weather_loss_time.value = self._dec_to_hm(data['weather_loss'])
            self.tel_loss_time.value = self._dec_to_hm(data['tel_loss'])
            self.total_time.text = 'Time Documented (hrs): {}'.format(self._dec_to_hm(data['total']))
            self.full_time = (datetime.datetime.strptime(meta_dict['dawn_18_deg'], '%Y%m%dT%H:%M') - datetime.datetime.strptime(meta_dict['dusk_18_deg'], '%Y%m%dT%H:%M')).seconds/3600
            self.full_time_text.text = 'Total time between 18 deg. twilights (hrs): {}'.format(self._dec_to_hm(self.full_time))
            self.milestone_alert.text = 'Time Use Data Updated'
        except Exception as e:
            self.milestone_alert.text = 'Issue with Time Use Data: {}'.format(e)


    def nonobs_entry_exp(self):
        self.my_name = str(self.nonobs_input_exp.value)
        self.layout.tabs[1] = self.exp_tab_0 

    def nonobs_entry_prob(self):
        self.my_name = str(self.nonobs_input_prob.value)
        self.layout.tabs[2] = self.prob_tab

    def get_ephemeris(self, date):
        kpno = ephem.Observer()
        observatory = {'TELESCOP':'KPNO 4.0-m telescope',
           'OBSERVAT':'KPNO',
           'OBS-LAT':31.9640293,
           'OBS-LONG':-111.5998917,
           'OBS-ELEV':2123.0}
        kpno.lon = observatory['OBS-LONG']*ephem.degree
        kpno.lat = observatory['OBS-LAT']*ephem.degree
        kpno.elevation = observatory['OBS-ELEV']
        kpno.epoch = ephem.J2000
        kpno.pressure = 0
        kpno.horizon = '-1:30'

        date = '{}-{}-{} 19:00:00'.format(date[0:4],date[4:6],date[6:])
        kpno.date = date

        obs_info = OrderedDict()
        sun = ephem.Sun()
        sun.compute(kpno.date)
        moon = ephem.Moon()
        moon.compute(kpno.date)
        obs_info['sunset'] = kpno.next_setting(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
        #Sunset
        obs_info['sunrise'] = kpno.next_rising(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
        #Sunrise
        try:
            obs_info['moonrise'] = kpno.next_rising(moon).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
        except:
            obs_info['moonrise'] = None
        try:
            obs_info['moonset'] = kpno.next_setting(moon).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
        except:
            obs_info['moonset'] = None

        for horizon, name in [('-6','civil'),('-10','ten'),('-12','nautical'),('-18','astronomical')]:
            kpno.horizon = horizon
            dusk =  kpno.next_setting(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
            t = pd.to_datetime(dusk)
            round_t = pd.Series(t).dt.round('min').dt.strftime("%Y%m%dT%H:%M")
            obs_info[f'dusk_{name}'] = str(round_t.values[0])
            dawn =  kpno.next_rising(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
            t = pd.to_datetime(dawn)
            round_t = pd.Series(t).dt.round('min')
            round_t = round_t.dt.strftime("%Y%m%dT%H:%M")
            obs_info[f'dawn_{name}'] = str(round_t.values[0])
        try:
            obs_info['illumination'] = round(moon.moon_phase, 3)
        except:
            obs_info['illumination'] = None
        return obs_info



    def add_observer_info(self):
        """ Initialize Night Log with Input Date
        """
        if self.update_log_status:
            meta = OrderedDict()
            meta['LO_firstname_1'], meta['LO_lastname_1'] = self.LO_1.value.split(' ')[0], ' '.join(self.LO_1.value.split(' ')[1:])
            meta['LO_firstname_2'], meta['LO_lastname_2'] = self.LO_2.value.split(' ')[0], ' '.join(self.LO_2.value.split(' ')[1:])
            meta['so_1_firstname'], meta['so_1_lastname'] = self.so_name_1.value.split(' ')[0], ' '.join(self.so_name_1.value.split(' ')[1:])
            meta['so_2_firstname'], meta['so_2_lastname'] = self.so_name_2.value.split(' ')[0], ' '.join(self.so_name_2.value.split(' ')[1:])
            meta['OA_firstname'], meta['OA_lastname'] = self.OA.value.split(' ')[0], ' '.join(self.OA.value.split(' ')[1:])

            eph = self.get_ephemeris(self.night)
            meta['time_sunset'] = eph['sunset']
            meta['time_sunrise'] = eph['sunrise']
            meta['time_moonrise'] = eph['moonrise']
            meta['time_moonset'] = eph['moonset']
            meta['illumination'] = eph['illumination']
            meta['dusk_10_deg'] = eph['dusk_ten']
            meta['dusk_12_deg'] = eph['dusk_nautical']
            meta['dusk_18_deg'] = eph['dusk_astronomical']
            meta['dawn_18_deg'] = eph['dawn_astronomical']
            meta['dawn_12_deg'] = eph['dawn_nautical']
            meta['dawn_10_deg'] = eph['dawn_ten']

            self.full_time = (datetime.datetime.strptime(meta['dawn_18_deg'], '%Y%m%dT%H:%M') - datetime.datetime.strptime(meta['dusk_18_deg'], '%Y%m%dT%H:%M')).seconds/3600
            self.full_time_text.text = 'Total time between 18 deg. twilights (hrs): {}'.format(self._dec_to_hm(self.full_time))
            self.plots_start = meta['dusk_10_deg']
            self.plots_end = meta['dawn_10_deg']
            self.DESI_Log.get_started_os(meta)

            self.connect_txt.text = 'Night Log Observer Data is Updated'
            self.DESI_Log.write_intro()
            #self.connect_log()
            self.update_log_status = False
            self.intro_layout.children[9] = self.init_btn
        else:
            self.intro_layout.children[9] = self.update_layout
            self.update_log_status = True


    def display_current_header(self):

        path = self.DESI_Log._open_kpno_file_first(self.DESI_Log.header_html)
        nl_file = open(path, 'r')
        intro = '<h2> NightLog Info: {}</h2>'.format(self.night)
        for line in nl_file:
            intro =  intro + line + '\n'
        self.intro_txt.text = intro
        nl_file.closed

    def current_nl(self):
        try:
            now = datetime.datetime.now()
            self.DESI_Log.finish_the_night()
            path = self.DESI_Log.nightlog_html 
            nl_file = open(path,'r')
            nl_txt = ''
            for line in nl_file:
                nl_txt +=  line 
            nl_txt += '<h3> All Exposures </h3>'
            self.nl_text.text = nl_txt
            nl_file.closed
            self.nl_alert.text = 'Last Updated on this page: {}'.format(now)
            self.nl_subtitle.text = "Current DESI Night Log: {}".format(path)
            self.get_exp_list()
            self.get_weather()
            try:
                self.make_telem_plots()
                return True
            except:
                self.logger.info('Something wrong with making telemetry plots')
                return True 
        except Exception as e:
            self.logger.info('current_nl Exception: %s' % str(e))
            self.nl_alert.text = 'You are not connected to a Night Log'
            return False

    def get_exp_list(self):
        try:
            exp_df = pd.read_sql_query(f"SELECT * FROM exposure WHERE night = '{self.night}'", self.conn)
            if len(exp_df.date_obs) >  0:
                time = exp_df.date_obs.dt.tz_convert('US/Arizona')
                exp_df['date_obs'] = time

                self.explist_source.data = exp_df[['date_obs','id','tileid','program','sequence','flavor','exptime','airmass','seeing']].sort_values(by='id',ascending=False) 

                exp_df = exp_df.sort_values(by='id')
                exp_df.to_csv(self.DESI_Log.explist_file, index=False)
            else:
                self.exptable_alert.text = f'No exposures available for night {self.night}'
        except Exception as e:
            self.exptable_alert.text = 'Cannot connect to Exposure Data Base. {}'.format(e)

    def get_weather(self):
        if os.path.exists(self.DESI_Log.weather):
            obs_df = pd.read_csv(self.DESI_Log.weather)
            t = [datetime.datetime.strptime(tt, "%Y%m%dT%H:%M") for tt in obs_df['Time']]
            obs_df['Time'] = t
            self.weather_source.data = obs_df.sort_values(by='Time')
        else:
            pass

    def get_telem_list(self, df, l, item):
        list_ = []
        for r in list(df[l]):
            try:
                list_.append(r[item])
            except:
                list_.append(None)
        return list_
        
    def make_telem_plots(self):
        start = datetime.datetime.strptime(self.plots_start, "%Y%m%dT%H:%M")
        start_utc = start.astimezone(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        end = datetime.datetime.strptime(self.plots_end, "%Y%m%dT%H:%M")
        end_utc = end.astimezone(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        exp_df = pd.read_sql_query(f"SELECT * FROM exposure WHERE date_obs > '{start_utc}' AND date_obs < '{end_utc}'", self.conn) #night = '{self.night}'", self.conn)

        telem_data = pd.DataFrame(columns =
        ['time','exp','mirror_temp','truss_temp','air_temp','temp','humidity','wind_speed','airmass','exptime','seeing','tput','skylevel'])
        if len(exp_df) > 0:
            exp_df.sort_values('date_obs',inplace=True)
            telem_data.time = exp_df.date_obs.dt.tz_convert('US/Arizona')
            telem_data.exp = exp_df.id 
            telem_data.mirror_temp = self.get_telem_list(exp_df, 'telescope','mirror_temp') #[r['mirror_temp'] for r in list(exp_df['telescope'])] #['mirror_temp']
            telem_data.truss_temp = self.get_telem_list(exp_df, 'telescope','truss_temp') #[r['truss_temp'] for r in list(exp_df['telescope'])] #exp_df['telescope']['truss_temp']
            telem_data.air_temp = self.get_telem_list(exp_df, 'telescope','air_temp')#[r['air_temp'] for r in list(exp_df['telescope'])] #['air_temp']
            telem_data.temp = self.get_telem_list(exp_df, 'tower','temperature') #[r['temperature'] for r in list(exp_df['tower'])] #['temperature']
            telem_data.humidity = self.get_telem_list(exp_df, 'tower','humidity') #[r['humidity'] for r in list(exp_df['tower'])] #['humidity']
            telem_data.wind_speed = self.get_telem_list(exp_df, 'tower','wind_speed') #[r['wind_speed'] for r in list(exp_df['tower'])] #['wind_speed']
            telem_data.airmass = exp_df.airmass
            telem_data.exptime = exp_df.exptime
            telem_data.seeing = exp_df.seeing

            tput = []
            for x in exp_df['etc']:
               if x is not None:
                   tput.append(x['transp'])
               else:
                   tput.append(None)
            telem_data.tput = tput #exp_df['etc']['transp']

            telem_data.skylevel = exp_df.skylevel

        self.telem_source.data = telem_data
        #export_png(self.bk_plots)
        if self.save_telem_plots:
            plt.style.use('ggplot')
            plt.rcParams.update({'axes.labelsize': 'small'})
            from matplotlib.pyplot import cm
            color=iter(cm.tab10(np.linspace(0,1,8)))


            fig = plt.figure(figsize=(10,15))
            ax1 = fig.add_subplot(8,1,1)
            ax1.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), self.get_telem_list(exp_df,'telescope','mirror_temp'), 'o-', label='mirror temp')    
            ax1.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), self.get_telem_list(exp_df,'telescope','truss_temp'),'o-',  label='truss temp')  
            ax1.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), self.get_telem_list(exp_df,'telescope','air_temp'),'o-',  label='air temp') 
            ax1.set_ylabel("Telescope Temperature (C)")
            ax1.legend()
            ax1.grid(True)
            ax1.tick_params(labelbottom=False)

            ax2 = fig.add_subplot(8,1,2, sharex = ax1)
            c=next(color)
            ax2.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), self.get_telem_list(exp_df,'tower','humidity'),'o-',  color=c, label='humidity') 
            ax2.set_ylabel("Humidity %")
            ax2.grid(True)
            ax2.tick_params(labelbottom=False)

            ax3 = fig.add_subplot(8,1,3, sharex=ax1) 
            c=next(color)
            ax3.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), self.get_telem_list(exp_df,'tower','wind_speed'), 'o-', color=c, label='wind speed')
            ax3.set_ylabel("Wind Speed (mph)")
            ax3.grid(True)
            ax3.tick_params(labelbottom=False)

            ax4 = fig.add_subplot(8,1,4, sharex=ax1)
            c=next(color)
            ax4.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), exp_df.airmass, 'o-', color=c, label='airmass')
            ax4.set_ylabel("Airmass")
            ax4.grid(True)
            ax4.tick_params(labelbottom=False)

            ax5 = fig.add_subplot(8,1,5, sharex=ax1)
            c=next(color)
            ax5.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), exp_df.exptime, 'o-', color=c, label='exptime')
            ax5.set_ylabel("Exposure time (s)")
            ax5.grid(True)
            ax5.tick_params(labelbottom=False)

            ax6 = fig.add_subplot(8,1,6,sharex=ax1)
            c=next(color)
            ax6.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), exp_df.seeing,'o-',  color=c, label='seeing')   
            ax6.set_ylabel("Seeing")
            ax6.grid(True)
            ax6.tick_params(labelbottom=False)

            ax7 = fig.add_subplot(8,1,7,sharex=ax1)
            c=next(color)
            ax7.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), tput, 'o-', color=c, label='transparency')
            ax7.set_ylabel("Transparency (%)")
            ax7.grid(True)
            ax7.tick_params(labelbottom=False)

            ax8 = fig.add_subplot(8,1,8,sharex=ax1)
            c=next(color)
            ax8.plot(exp_df.date_obs.dt.tz_convert('US/Arizona'), exp_df.skylevel, 'o-', color=c, label='Sky Level')      
            ax8.set_ylabel("Sky level (AB/arcsec^2)")
            ax8.grid(True)

            ax8.set_xlabel("Local Time (MST)")
            ax8.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M', tz=pytz.timezone("US/Arizona")))
            ax8.tick_params(labelrotation=45)
            fig.suptitle("Telemetry for obsday {}".format(self.night),fontsize=14)
            plt.subplots_adjust(top=0.85)
            fig.tight_layout()

            plt.savefig(self.DESI_Log.telem_plots_file)
            self.save_telem_plots = False
                
    def exp_to_html(self):
        exp_df = pd.read_csv(self.DESI_Log.explist_file)
        exp_df = exp_df[['date_obs','id','tileid','program','sequence','flavor','exptime','airmass','seeing']].sort_values(by='id',ascending=False) 
        exp_df = exp_df.rename(columns={"date_obs": "Time", "id":
        "Exp","tileid":'Tile','program':'Program','sequence':'Sequence','flavor':'Flavor','exptime':'Exptime','airmass':'Airmass','seeing':'Seeing'})
        exp_html = exp_df.to_html()
        return exp_html

    def bad_exp_add(self):
        exp = self.bad_exp_val
        cams_dict = {0:'a',1:'b',2:'r',3:'z'}
        if self.bad_all:
            bad = True
            cameras = None
        elif self.bad_all == False:
            bad = False
            cameras = ''
            for i, cams in enumerate([self.bad_cams_0, self.bad_cams_1, self.bad_cams_2, self.bad_cams_3, self.bad_cams_4, self.bad_cams_5, self.bad_cams_6, self.bad_cams_7, self.bad_cams_8, self.bad_cams_9]):
                if len(cams.active) == 0:
                    pass
                else:
                    for c in cams.active:
                        cameras += '{}{}'.format(cams_dict[int(c)],i)
            self.exp_layout_1.children[11] = self.exp_btn
            self.exp_alert.text = 'Part of the exposure {} has been added to the bad exposure list'.format(exp)

        comment = self.bad_comment
        data = {}
        data['NIGHT'] = [self.night]
        data['EXPID'] = [exp]
        data['BAD'] = [bad]
        data['BADCAMS'] = [cameras]
        data['COMMENT'] = [comment]
        #self.bad_alert.text = 'Submitted Bad Exposure {} @ {}'.format(exp, datetime.datetime.now().strftime('%H:%M.%S'))
        self.bad_cams_0.active = []
        self.bad_cams_1.active = []
        self.bad_cams_2.active = []
        self.bad_cams_3.active = []
        self.bad_cams_4.active = []
        self.bad_cams_5.active = []
        self.bad_cams_6.active = []
        self.bad_cams_7.active = []
        self.bad_cams_8.active = []
        self.bad_cams_9.active = []
        
        self.DESI_Log.add_bad_exp(data)
        self.clear_input([self.exp_time, self.exp_enter, self.exp_select, self.exp_comment])

    def plan_add_new(self):
        self.plan_time = None
        self.plan_add()

    def milestone_add_new(self):
        self.milestone_time = None
        self.milestone_add()

    def plan_add(self):
        if self.plan_time is None:
            ts = datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S")
        else: 
            ts = self.plan_time
        if str(self.plan_input.value) in ['nan',' ','']:
            self.plan_alert.text = 'Trying to add an item with NaN'
        else:
            data = [ts, self.plan_input.value]
            self.DESI_Log.add_input(data, 'plan')
            self.plan_alert.text = 'Last item input: {}'.format(self.plan_input.value)
        self.clear_input([self.plan_order, self.plan_input])
        self.plan_time = None

    def milestone_add(self):
        if self.milestone_time is None:
            ts = datetime.datetime.now().strftime("%Y%m%dT%H:%M:%S")
        else:
            ts = self.milestone_time
        data = [ts, self.milestone_input.value, self.milestone_exp_start.value, self.milestone_exp_end.value, self.milestone_exp_excl.value,
        self.report_type]
        self.DESI_Log.add_input(data,'milestone')
        self.milestone_alert.text = 'Last Milestone Entered: {}'.format(self.milestone_input.value)
        self.clear_input([self.milestone_input, self.milestone_exp_start, self.milestone_exp_end, self.milestone_exp_excl])
        self.milestone_time = None

    def prob_add(self):
        """Adds problem to nightlog
        """
        note = ' '
        #try:
        if self.prob_time.value in [None, 'None'," ",""]:
            note = 'Enter a time'
        else:
            img_name, img_data, preview = self.image_uploaded('problem')
            if self.report_type  == 'NObs':
                my_name = self.my_name
            else:
                my_name = self.report_type
            data = [my_name, self.get_time(self.prob_time.value.strip()), self.prob_input.value.strip(), self.prob_alarm.value.strip(),
            self.prob_action.value.strip()]
            self.DESI_Log.add_input(data, 'problem',img_name=img_name, img_data=img_data)

            self.prob_alert.text = "Last Problem Input: '{}' at {}".format(self.prob_input.value.strip(), self.prob_time.value.strip())

        self.clear_input([self.prob_time, self.prob_input, self.prob_alarm, self.prob_action])

        #except Exception as e:
        #    self.prob_alert.text = "Problem with your Input: {} - {}".format(note, e)

    def exp_add(self):
        quality = None
        if self.os_exp_option.active == 0: #Time
            if self.exp_time.value not in [None, 'None'," ", ""]:
                try:
                    time = self.get_time(self.exp_time.value.strip())
                    comment = self.exp_comment.value.strip()
                    exp = None
                    submit = True
                except Exception as e:
                    self.exp_alert.text = 'There is something wrong with your input @ {}: {}'.format(datetime.datetime.now().strftime('%H:%M'),e)
            else:
                self.exp_alert.text = 'Fill in the time'
                
        elif self.os_exp_option.active == 1: #Exposure
            try:
                exp = int(float(self.exp_enter.value))
                comment = self.exp_comment.value.strip()
                if self.report_type == 'SO':
                    quality = self.quality_list[self.quality_btns.active]

                if str(self.exp_time.value.strip()) in ['',' ','None','nan']:
                    time = self.get_time(datetime.datetime.now().strftime("%H:%M"))
                else:
                    try:
                        time = self.get_time(self.exp_time.value.strip())
                    except:
                        time = self.get_time(datetime.datetime.now().strftime("%H:%M"))
                submit = True
            except Exception as e:
                self.exp_alert.text = "Problem with the Exposure you Selected @ {}: {}".format(datetime.datetime.now().strftime('%H:%M'), e)

        if self.report_type == 'NObs':
            your_name = self.my_name
        elif self.report_type in ['LO','SO']:
            your_name = self.report_type

        img_name, img_data, preview = self.image_uploaded('comment')
        now = datetime.datetime.now().astimezone(tz=self.kp_zone).strftime("%H:%M")
        if submit:
            data = [time, exp, quality, self.exp_comment.value.strip(), your_name]
            self.DESI_Log.add_input(data, 'exp', img_name=img_name, img_data=img_data)
            self.exp_alert.text = 'Last Input was made @ {}: {}'.format(datetime.datetime.now().strftime("%H:%M"),self.exp_comment.value)

            if quality == 'Bad':
                self.exp_layout_1.children[11] = self.bad_layout_1
                self.bad_exp_val = exp
                self.bad_comment = self.exp_comment.value.strip()
            else:
                self.clear_input([self.exp_time, self.exp_enter, self.exp_comment])

    def check_add(self):
        """add checklist time to Night Log
        """
        complete = self.checklist.active
        check_time = datetime.datetime.now().strftime("%Y%m%dT%H:%M")
        if len(complete) == len(self.checklist.labels):
            data = [self.report_type, check_time, self.check_comment.value]
            self.DESI_Log.add_input(data, 'checklist')
            self.check_alert.text = "Checklist last submitted at {}".format(check_time[-5:])
        else:
            self.check_alert.text = "Must complete all tasks before submitting checklist"
        self.clear_input(self.check_comment)
        self.checklist.active = []

    def weather_add(self):
        """Adds table to Night Log
        """
        now = datetime.datetime.now().astimezone(tz=self.kp_zone).strftime("%Y%m%dT%H:%M")
        try:
            self.make_telem_plots()
            telem_df = pd.DataFrame(self.telem_source.data)
            this_data = telem_df.iloc[-1]
            desc = self.weather_desc.value
            temp = self.get_latest_val(telem_df.temp) #.dropna())[-1] #list(telem_df)[np.isfinite(list(telem_df.temp))][-1] #this_data.temp
            wind = self.get_latest_val(telem_df.wind_speed) #list(telem_df.wind_speed.dropna())[-1]
            humidity = self.get_latest_val(telem_df.humidity) #list(telem_df.humidity.dropna())[-1] #this_data.humidity
            seeing = self.get_latest_val(telem_df.seeing) #list(telem_df.seeing.dropna())[-1] #this_data.seeing
            tput = self.get_latest_val(telem_df.tput) #list(telem_df.tput.dropna())[-1]
            skylevel = self.get_latest_val(telem_df.skylevel)  #list(telem_df.skylevel.dropna())[-1]
            data = [now, desc, temp, wind, humidity, seeing, tput, skylevel]

        except: 
            data = [now, self.weather_desc.value, None, None, None, None, None, None]
            
            self.weather_alert.text = 'Not connected to the telemetry DB. Only weather description will be recorded.'
        df = self.DESI_Log.add_input(data,'weather')
        self.clear_input([self.weather_desc])
        self.get_weather()

    def get_latest_val(self, l):
        try:
            x = list(l.dropna())[-1]
        except:
            x = np.nan
        return x

    def image_uploaded(self, mode='comment'):
        img_data = None
        img_name = None

        if mode == 'comment':         
            if self.exp_comment.value not in [None, ''] and hasattr(self, 'img_upload_comments_os') and self.img_upload_comments_os.filename not in [self.current_img_name, None,'','nan',np.nan]:
                img_data = self.img_upload_comments_os.value.encode('utf-8')
                input_name = os.path.splitext(str(self.img_upload_comments_os.filename))
                img_name = input_name[0] + '_{}'.format(self.location) + input_name[1]
                self.current_img_name = self.img_upload_comments_os.filename

        elif mode == 'problem':
            if hasattr(self, 'img_upload_problems') and self.img_upload_problems.filename not in [self.current_img_name, None, '',np.nan, 'nan']:
                img_data = self.img_upload_problems.value.encode('utf-8')
                input_name = os.path.splitext(str(self.img_upload_problems.filename))
                self.current_img_name = self.img_upload_problems.filename
                img_name = input_name[0] + '_{}'.format(self.location) + input_name[1]
        self.image_location_on_server = f'http://desi-www.kpno.noao.edu:8090/{self.night}/images/{img_name}'
        width=400
        height=400 #http://desi-www.kpno.noao.edu:8090/nightlogs
        preview = '<img src="%s" width=%s height=%s alt="Uploaded image %s">\n' % (self.image_location_on_server,str(width),str(height),img_name)
        return img_name, img_data, preview

    def plan_delete(self):
        time = self.plan_time
        self.DESI_Log.delete_item(time, 'plan')
        self.plan_alert.text = 'Deleted item @ {}: {}'.format(datetime.datetime.now().strftime('%H:%M'),self.plan_input.value)
        self.clear_input([self.plan_input, self.plan_order])
        self.plan_time = None

    def milestone_delete(self):
        time = self.milestone_time
        self.DESI_Log.delete_item(time, 'milestone')
        self.milestone_alert.text = 'Deleted item @ {}: {}'.format(datetime.datetime.now().strftime('%H:%M'), self.milestone_input.value)
        self.clear_input([self.milestone_input, self.milestone_load_num])
        self.milestone_time = None

    def progress_delete(self):
        time = self.get_time(self.exp_time.value.strip())
        self.DESI_Log.delete_item(time, 'progress', self.report_type)
        self.exp_alert.text = 'Deleted item @ {}: {}'.format(datetime.datetime.now().strftime('%H:%M'), self.exp_comment.value)
        self.clear_input([self.exp_time, self.exp_comment, self.exp_time, self.exp_enter])

    def problem_delete(self):
        time = self.get_time(self.prob_time.value.strip())
        self.DESI_Log.delete_item(time, 'problem',self.report_type)
        self.prob_alert.text = 'Deleted item @ {}: {}'.format(datetime.datetime.now().strftime('%H:%M'), self.prob_input.value)
        self.clear_input([self.prob_time, self.prob_input, self.prob_alarm, self.prob_action])

        
    def plan_load(self):
        try:
            b, item = self.DESI_Log.load_index(self.plan_order.value, 'plan')
            if b:
                self.plan_input.value = str(item['Objective'])
                self.plan_time = item['Time']
            else:
                self.plan_alert.text = "That plan item doesn't exist yet. {}".format(item)
        except Exception as e:
            self.plan_alert.text = "Issue with loading that plan item: {}".format(e)


    def milestone_load(self):
        try:
            b, item = self.DESI_Log.load_index(int(self.milestone_load_num.value), 'milestone')
            if b:
                self.milestone_input.value = str(item['Desc'])
                if str(item['Exp_Start']) not in ['nan','',' ']:
                    self.milestone_exp_start.value = str(int(item['Exp_Start']))
                if str(item['Exp_Stop']) not in ['nan','',' ']:
                    self.milestone_exp_end.value = str(int(item['Exp_Stop']))
                if str(item['Exp_Excl']) not in ['nan','',' ']:
                    self.milestone_exp_excl.value = str(int(item['Exp_Excl']))
                self.milestone_time = item['Time']
            else:
                self.milestone_alert.text = "That milestone index doesn't exist yet. {}".format(item)
        except Exception as e:
            self.milestone_alert.text = "Issue with loading that milestone: {}".format(e)

    def exposure_load(self):
        option = self.os_exp_option.active
        if option == 0: #time
            try:
                _exists, item = self.DESI_Log.load_timestamp(self.get_time(self.exp_time.value.strip()), self.report_type, 'exposure')
            except Exception as e:
                self.exp_alert.text = 'Issue loading that exposure using the timestamp: {}'.format(e)
        elif option == 1: #exposure
            try:
                _exists, item = self.DESI_Log.load_exp(self.exp_enter.value)
            except Exception as e:
                self.exp_alert.text = 'Issue loading that exposure using the EXPID: {}'.format(e)
        try:
            if not _exists:
                self.exp_alert.text = 'This input either does not exist or was input by another user: {}'.format(item)
            else:
                try:
                    self.exp_time.value = self.DESI_Log.write_time(str(item['Time']), kp_only=True)
                    self.exp_comment.value = str(item['Comment'])
                    if str(item['Exp_Start']) not in ['', ' ','nan']:
                        self.exp_enter.value = str(int(item['Exp_Start']))
                        self.exp_option.active = 1
                        self.os_exp_option.active = 1
                    if str(item['Quality']) not in ['',' ','nan','None']:
                        idx = np.where(np.array(self.quality_list) == np.array([item['Quality']]))[0][0]
                        self.quality_btns.active = idx

                except Exception as e:
                    self.exp_alert.text = "Issue with loading that exposure: {}".format(e)
        except:
            pass

    def problem_load(self):
        #Check if progress has been input with a given timestamp
        try:
            _exists, item = self.DESI_Log.load_timestamp(self.get_time(self.prob_time.value.strip()), self.report_type, 'problem')

            if not _exists:
                self.prob_alert.text = 'This timestamp does not yet have an input from this user. {}'.format(item)
            else:
                self.prob_input.value = str(item['Problem'])
                if item['alarm_id'] not in ['nan','',' ']:
                    self.prob_alarm.value = str(int(item['alarm_id']))
                self.prob_action.value = str(item['action'])
        except Exception as e:
            self.prob_alert.text = "Issue with loading that problem: {}".format(e)

    def add_contributer_list(self):
        cont_list = self.contributer_list.value
        self.DESI_Log.add_contributer_list(cont_list)

    def add_time(self):
        data = OrderedDict()

        time_items = OrderedDict({'obs_time':self.obs_time,'test_time':self.test_time,'inst_loss':self.inst_loss_time,
            'weather_loss':self.weather_loss_time,'tel_loss':self.tel_loss_time})
        total = 0
        for name, item in time_items.items():
            try:
                data[name] = float(item.value)
                total += float(item.value)
            except:
                try:
                    dec = self._hm_to_dec(str(item.value))
                    data[name] = dec
                    total += float(dec)
                except:
                    data[name] = 0
                    total += 0
        data['18deg'] = float(self.full_time)
        data['total'] = total
        self.total_time.text = 'Time Documented (hrs): {}'.format(str(self._dec_to_hm(total)))
        df = pd.DataFrame(data, index=[0])
        df.to_csv(self.DESI_Log.time_use, index=False)

    def summary_add(self):
        if self.summary_input.value in ['',' ','nan','None']:
            self.milestone_alert.text = 'Nothing written in the summary so not submitted. Try Loading again.'
        else:
            now = datetime.datetime.now().strftime("%H:%M")
            half = self.summary_option.active
            data = OrderedDict()
            data['SUMMARY_{}'.format(half)] = self.summary_input.value
            self.DESI_Log.add_summary(data)
            self.milestone_alert.text = 'Summary Information Entered at {}: {}'.format(now, self.summary_input.value)
            self.clear_input([self.summary_input])

    def summary_load(self):
        half = self.summary_option.active
        f = self.DESI_Log.summary_file
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                d = df.iloc[0]
                self.summary_input.value = d['SUMMARY_{}'.format(half)]
            except Exception as e:
                print('Issue loading summary: {}'.format(e))
        else:
            self.milestone_alert.text = 'That summary does not yet exist'

    def upload_image(self, attr, old, new):
        self.logger.info(f'Local image file upload: {self.img_upload.filename}')

    def upload_image_comments_os(self, attr, old, new):
        self.logger.info(f'Local image file upload (OS comments): {self.img_upload_comments_os.filename}')

    def upload_image_comments_other(self, attr, old, new):
        self.logger.info(f'Local image file upload (Other comments): {self.img_upload_comments_other.filename}')

    def upload_image_comments_dqs(self, attr, old, new):
        self.logger.info(f'Local image file upload (Other comments): {self.img_upload_comments_dqs.filename}')

    def upload_image_problems(self, attr, old, new):
        self.logger.info(f'Local image file upload (Other comments): {self.img_upload_problems.filename}')

    def time_is_now(self):
        now = datetime.datetime.now().astimezone(tz=self.kp_zone).strftime("%H:%M")

        tab = self.layout.active
        time_input = self.time_tabs[tab]
        try:
            time_input.value = now
        except:
            return time_input

    def nl_submit(self):

        if not self.current_nl():
            self.nl_text.text = 'You cannot submit a Night Log to the eLog until you have connected to an existing Night Log or initialized tonights Night Log'
        else:
            self.logger.info("Starting Nightlog Submission Process")

            f = self.DESI_Log._open_kpno_file_first(self.DESI_Log.nightlog_html)
            nl_file=open(f,'r')
            lines = nl_file.readlines()
            nl_html = ' '
            for line in lines:
                nl_html += line

            #make Paul's plot
            try:
                os.system("{}/bin/plotnightobs -n {}".format(os.environ['SURVEYOPSDIR'],self.night))
            except Exception as e:
                self.logger.info('Issues with Pauls plot: {}'.format(e))

            if self.test:
                pass
            else:
                try:
                    from ECLAPI import ECLConnection, ECLEntry
                    e = ECLEntry('Synopsis_Night', text=nl_html, textile=True)

                    subject = 'Night Summary {}'.format(self.night)
                    e.addSubject(subject)
                    url = 'http://desi-www.kpno.noao.edu:8090/ECL/desi'
                    user = 'dos'
                    pw = 'dosuser'

                    elconn = ECLConnection(url, user, pw)
                    response = elconn.post(e)
                    elconn.close()
                    if response[0] != 200:
                        raise Exception(response)
                        self.submit_text.text = "You cannot post to the eLog on this machine"
                except:
                    ECLConnection = None
                    self.nl_text.text = "Can't connect to eLog"

            #Add bad exposures
            try:
                survey_dir = os.path.join(os.environ['NL_DIR'],'ops')
                bad_filen = 'bad_exp_list.csv'
                bad_path = os.path.join(survey_dir, bad_filen)
                bad_df = pd.read_csv(bad_path)
                new_bad = self.DESI_Log._combine_compare_csv_files(self.DESI_Log.bad_exp_list, bad=True)
                bad_df = pd.concat([bad_df, new_bad])
                bad_df = bad_df.drop_duplicates(subset=['EXPID'], keep='last')
                bad_df = bad_df.astype({"NIGHT":int, "EXPID": int,"BAD":bool,"BADCAMS":str,"COMMENT":str})
                bad_df.to_csv(bad_path,index=False)
                err1 = os.system('svn update --non-interactive {}'.format(bad_path))
                self.logger.info('SVN added bad exp list {}'.format(err1))
                err2 = os.system('svn commit --non-interactive -m "autocommit from night summary submission" {}'.format(bad_path))
                self.logger.info('SVN commited bad exp list {}'.format(err2))

            except Exception as e:
                self.logger.info('Cant post to the bad exp list: {}'.format(e))


            self.save_telem_plots = True
            self.current_nl()

            if self.test:
                self.email_nightsum(user_email = ["james.lasker3@gmail.com","jlasker@smu.edu"])
            else:
                self.email_nightsum(user_email = ["james.lasker3@gmail.com","satya.gontcho@gmail.com","desi-nightlog@desi.lbl.gov"])

            self.submit_text.text = "Night Log posted to eLog and emailed to collaboration at {}".format(datetime.datetime.now().strftime("%Y%m%d%H:%M")) + '</br>'

    def email_nightsum(self,user_email = None):

        try:
            self.make_telem_plots()
        except:
            self.logger.info("Something wrong with telem plots")

        #sender = "noreply-ecl@noao.edu"
        sender = "noreply-ecl@noirlab.edu"

        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('html')
        msg['Subject'] = "Night Summary %s" % self.date_init.value #mjd2iso(mjd)
        msg['From'] = sender
        if len(user_email) == 1:
            msg['To'] = user_email[0]
        else:
            msg['To'] = ', '.join(user_email)

        # Create the body of the message (a plain-text and an HTML version).
        f = self.DESI_Log._open_kpno_file_first(self.DESI_Log.nightlog_html)
        nl_file=open(f,'r')
        lines = nl_file.readlines()
        nl_html = "" 
        img_names = []
        for line in lines:
            nl_html += line

        # Add exposures
        if os.path.exists(self.DESI_Log.explist_file):
            exp_list = self.exp_to_html()
            nl_html += ("<h3 id='exposures'>Exposures</h3>")
            for line in exp_list:
                nl_html += line

        nl_text = MIMEText(nl_html, 'html')
        msg.attach(nl_text)
        Html_file = open(os.path.join(self.DESI_Log.root_dir,'NightSummary{}.html'.format(self.night)),"w")
        Html_file.write(nl_html)

        # Add Paul's plot
        try:
            nightops = open(os.path.join(os.environ['DESINIGHTSTATS'],'nightstats{}.png'.format(self.night)),'rb').read()
            msgImage = MIMEImage(nightops)
            data_uri = base64.b64encode(nightops).decode('utf-8')
            img_tag = '<img src="data:image/png;base64,%s" \>' % data_uri
            msgImage.add_header('Content-Disposition', 'attachment; filename=nightstats{}.png'.format(self.night))
            msg.attach(msgImage)
            Html_file.write(img_tag)
        except Exception as e:
            self.logger.info('Problem attaching pauls plot: {}'.format(e))
        # Add images
        if os.path.exists(self.DESI_Log.telem_plots_file):
            telemplot = open(self.DESI_Log.telem_plots_file, 'rb').read()
            msgImage = MIMEImage(telemplot)
            data_uri = base64.b64encode(telemplot).decode('utf-8')
            img_tag = '<img src="data:image/png;base64,%s" \>' % data_uri
            msgImage.add_header('Content-Disposition', 'attachment; filename=telem_plots_{}.png'.format(self.night))
            msg.attach(msgImage)
            Html_file.write(img_tag)
        Html_file.close()
        
        text = msg.as_string()

        # Send the message via local SMTP server.
        #yag = yagmail.SMTP(sender)
        #yag.send("parfa30@gmail.com",nl_html,self.DESI_Log.telem_plots_file)
        s = smtplib.SMTP('localhost')
        
        s.set_debuglevel(2)
        s.sendmail(sender, user_email, text)
        s.quit()
        self.logger.info("Email sent")
