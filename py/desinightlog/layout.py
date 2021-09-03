"""
Created on July 21, 2021

@author: Parker Fagrelius

details all layout for the DNI Reports

"""
import os
import datetime
import pandas as pd

from bokeh.models import TextInput, ColumnDataSource, CheckboxButtonGroup, Paragraph, Button, TextAreaInput, Select, CheckboxGroup, RadioButtonGroup
from bokeh.models.widgets.markups import Div
from bokeh.layouts import layout, column
from bokeh.models.widgets import Panel
from bokeh.models.widgets.tables import DataTable, TableColumn
from bokeh.plotting import figure

import nightlog as nl

class Layout():
    def __init__(self):
        self.line = Div(text='-----------------------------------------------------------------------------------------------------------------------------', width=1000)
        self.line2 = Div(text='-----------------------------------------------------------------------------------------------------------------------------', width=1000)
        self.lo_names = ['None ','Liz Buckley-Geer','Ann Elliott','Parker Fagrelius','Satya Gontcho A Gontcho','James Lasker','Martin Landriau','Claire Poppett','Michael Schubnell','Luke Tyas','Other ']
        self.oa_names = ['None ','Karen Butler','Amy Robertson','Anthony Paat','Thaxton Smith','Dave Summers','Doug Williams','Other ']
        self.connect_txt = Div(text=' ', css_classes=['alert-style'])
        self.intro_txt = Div(text=' ')
        self.comment_txt = Div(text=" ", css_classes=['inst-style'], width=1000)


        self.time_title = Paragraph(text='Time (Kitt Peak local time)', align='center')
        self.now_btn = Button(label='Now', css_classes=['now_button'], width=75)

        self.full_time_text = Div(text='Total time between 18 deg. twilights (hrs): ', width=100) #Not on intro slide, but needed

        self.nw_dir = os.environ['NW_DIR']
        self.nl_dir = os.environ['NL_DIR']  

    def get_intro_layout(self):
        self.contributer_list = TextAreaInput(placeholder='Contributer names (include all)', rows=2, cols=1, title='Names of all contributers')
        self.contributer_btn = Button(label='Update Contributer List', css_classes=['add_button'], width=200)

        self.connect_hdr = Div(text="Connect to Night Log", css_classes=['subt-style'], width=800)
        self.obs_type = RadioButtonGroup(labels=["Lead Observer", "Support Observer", "Non-Observer"], css_classes=['add_button'], active=None)
        self.connect_btn = Button(label="Connect to  Night Log", css_classes=['connect_button'], width=200)
        self.date_init = Select(title="Night Logs")

        self.so_name_1 = TextInput(title='Support Observing Scientist 1', placeholder='Sally Ride')
        self.so_name_2 = TextInput(title='Support Observing Scientist 2', placeholder="Mae Jemison")
        self.LO_1 = Select(title='Lead Observer 1', value='None', options=self.lo_names)
        self.LO_2 = Select(title='Lead Observer 2', value='None', options=self.lo_names)
        self.OA = Select(title='Observing Assistant', value='Choose One', options=self.oa_names)

        self.update_log_status = False
        self.init_btn = Button(label="Update Night Log Info", css_classes=['init_button'], width=200)
        self.update_layout = layout([[self.so_name_1, self.so_name_2], [self.LO_1, self.LO_2], self.OA, self.init_btn])

        self.intro_layout = layout(children=[self.buffer,
                                    self.title,
                                    [self.page_logo, self.instructions],
                                    self.connect_hdr,
                                    [self.date_init, [self.obs_type, self.connect_btn]],
                                    self.connect_txt,
                                    self.line,
                                    [self.contributer_list, self.contributer_btn],
                                    self.line2,
                                    self.init_btn,
                                    self.intro_txt], width=1000)
        self.intro_tab = Panel(child=self.intro_layout, title="Connect")

    def get_nonobs_layout(self):
        inst = """If you want to make an entry into the NightLog, please enter your name"""
        self.nonobs_inst = Div(text=inst, css_classes=['inst-style'], width=1000)
        self.nonobs_input_exp = TextInput(title='Your Name', placeholder='Vera Rubin', width=150)
        self.nonobs_btn_exp = Button(label='Submit', css_classes=['init_button'], width=150)
        self.nonobs_input_prob = TextInput(title='Your Name', placeholder='Nancy Grace Roman', width=150)
        self.nonobs_btn_prob = Button(label='Submit', css_classes=['init_button'], width=150)

        self.nonobs_layout_exp = layout([self.nonobs_inst, self.nonobs_input_exp, self.nonobs_btn_exp], width=1000)
        self.nonobs_layout_prob = layout([self.nonobs_inst, self.nonobs_input_prob, self.nonobs_btn_prob], width=1000)

    def get_plan_layout(self):
        self.plan_subtitle = Div(text="Night Plan", css_classes=['subt-style'])
        inst = """<ul>
        <li>Add the major elements of the night plan found at the link below in the order expected for their completion using the <b>Add/New</b> button. Do NOT enter an index for new items - they will be generated.</li>
        <li>You can recall submitted plans using their index, as found on the Current DESI Night Log tab.</li>
        <li>If you'd like to modify a submitted plan item, <b>Load</b> the index (these can be found on the Current NL), make your modifications, and then press <b>Update</b>.</li>
        </ul>
        """
        self.plan_inst = Div(text=inst, css_classes=['inst-style'], width=1000)
        self.plan_txt = Div(text='<a href="https://desi.lbl.gov/trac/wiki/DESIOperations/ObservingPlans/">Tonights Plan Here</a>', css_classes=['inst-style'], width=500)
        self.plan_order = TextInput(title='Plan Index:', placeholder='0', width=75)
        self.plan_input = TextAreaInput(placeholder="description", rows=2, cols=2, title="Enter item of the night plan:",max_length=5000, width=800)
        self.plan_btn = Button(label='Update', css_classes=['update_button'], width=150)
        self.plan_new_btn = Button(label='Add New', css_classes=['add_button'], width=150)
        self.plan_load_btn = Button(label='Load', css_classes=['load_button'], width=75)
        self.plan_delete_btn = Button(label='Delete', css_classes=['delete_button'], width=150)
        self.plan_alert = Div(text=' ', css_classes=['alert-style'])

        plan_layout = layout([self.buffer,
                            self.title,
                            self.plan_subtitle,
                            self.plan_inst,
                            self.plan_txt,
                            [self.plan_input,[self.plan_order, self.plan_load_btn]],
                            [self.plan_new_btn, self.plan_btn, self.plan_delete_btn],
                            self.plan_alert], width=1000)
        self.plan_tab = Panel(child=plan_layout, title="Night Plan")

    def get_milestone_layout(self):
        self.milestone_subtitle = Div(text="Milestones & Major Accomplishments", css_classes=['subt-style'])
        inst = """<ul>
        <li>Record any major milestones or accomplishments that occur throughout a night. These should correspond with the major elements input on the 
        <b>Plan</b> tab. Include exposure numbers that correspond with the accomplishment, and if applicable, indicate any exposures to ignore in a series.
        Do NOT enter an index for new items - they will be generated.</li>
        <li>If you'd like to modify a submitted milestone, <b>Load</b> the index (these can be found on the Current NL), make your modifications, and then press <b>Update</b>.</li>
        <li>At the end of your shift - either at the end of the night or half way through - summarize the activities of the night in the <b>End of Night Summary</b>. 
        You can Load and modify submissions.</li>
        <li>At the end of the night, record how the time was spent between 12 degree twilight:
        <ul>
        <li><b>ObsTime:</b> time in hours spent on sky not - open shutter time, but the total time spent “observing” science targets. This will include the overheads of positioning, acquisition, telescope slews, etc.</li>
        <li><b>TestTime:</b> When Klaus or the FP team are running tests at night. Dither tests, etc. should be logged under this heading, not Obs</li>
        <li><b>InstLoss:</b> Time which is lost to instrument problems. That is, when the acquisition fails; or observing has to stop due to a problem with DESI; or an image is lost after integrating for a while.</li>
        <li><b>WeathLoss:</b> Time lost to weather issues, including useless exposures. If the entire night is lost to weather, please enter the time between 18 deg twilights, even if some time was used for closed dome tests (which you can enter under "TestTime"), and if you quit early.</li>
        <li><b>TelLoss:</b> time lost to telescope / facility issues (e.g.,floor cooling problem that causes stoppage; or dome shutter breaks; or mirror cover issues; etc.). Personnel issues (e.g., no LOS available) should be logged here.</li>
        </ul>
        </ul>
        """
        self.milestone_inst = Div(text=inst, css_classes=['inst-style'],width=1000)
        self.milestone_input = TextAreaInput(placeholder="Description", title="Enter a Milestone:", rows=2, cols=3, max_length=5000, width=800)
        self.milestone_exp_start = TextInput(title ='Exposure Start', placeholder='12345',  width=200)
        self.milestone_exp_end = TextInput(title='Exposure End', placeholder='12345', width=200)
        self.milestone_exp_excl = TextInput(title='Excluded Exposures', placeholder='12346', width=200)
        self.milestone_btn = Button(label='Update', css_classes=['update_button'],width=150)
        self.milestone_new_btn = Button(label='Add New', css_classes=['add_button'], width=150)
        self.milestone_load_num = TextInput(title='Index', placeholder='0',  width=75)
        self.milestone_load_btn = Button(label='Load', css_classes=['load_button'], width=75)
        self.milestone_delete_btn = Button(label='Delete', css_classes=['delete_button'], width=150)
        self.milestone_alert = Div(text=' ', css_classes=['alert-style'])

        self.summary_input = TextAreaInput(rows=8, placeholder='End of Night Summary', title='End of Night Summary', max_length=5000)
        self.summary_option = RadioButtonGroup(labels=['First Half','Second Half'], active=0, width=200)
        self.summary_load_btn = Button(label='Load', css_classes=['load_button'], width=75)
        self.summary_btn = Button(label='Add/Update Summary', css_classes=['add_button'], width=150)

        self.obs_time = TextInput(title ='ObsTime', placeholder='10', width=100)
        self.test_time = TextInput(title ='TestTime', placeholder='0', width=100)
        self.inst_loss_time = TextInput(title ='InstLoss', placeholder='0', width=100)
        self.weather_loss_time = TextInput(title ='WeathLoss', placeholder='0', width=100)
        self.tel_loss_time = TextInput(title ='TelLoss', placeholder='0', width=100)
        self.total_time = Div(text='Time Documented (hrs): ', width=100) #add all times together
        self.time_btn = Button(label='Add/Update Time Use', css_classes=['add_button'], width=150)
        
        #For Lead Observer
        milestone_layout_0 = layout([self.buffer,
                                self.title,
                                self.milestone_subtitle,
                                self.milestone_inst,
                                [[self.milestone_input,[self.milestone_exp_start, self.milestone_exp_end, self.milestone_exp_excl]],[self.milestone_load_num, self.milestone_load_btn]],
                                [self.milestone_new_btn, self.milestone_btn, self.milestone_delete_btn] ,
                                self.milestone_alert,
                                self.line,
                                [self.summary_option,self.summary_load_btn],
                                self.summary_input,
                                self.summary_btn,
                                [self.obs_time, self.test_time, self.inst_loss_time, self.weather_loss_time, self.tel_loss_time, self.total_time, self.full_time_text],
                                self.time_btn], width=1000)
        #For Support Observer
        milestone_layout_1 = layout([self.buffer,
                                self.title,
                                self.milestone_subtitle,
                                self.milestone_inst,
                                [[self.milestone_input,[self.milestone_exp_start, self.milestone_exp_end, self.milestone_exp_excl]],[self.milestone_load_num, self.milestone_load_btn]],
                                [self.milestone_new_btn, self.milestone_btn, self.milestone_delete_btn] ,
                                self.milestone_alert,
                                self.line,
                                [self.summary_option,self.summary_load_btn],
                                self.summary_input,
                                self.summary_btn], width=1000)
        self.milestone_tab_0 = Panel(child=milestone_layout_0, title='Milestones')
        self.milestone_tab_1 = Panel(child=milestone_layout_1, title='Milestones')

    def get_exp_layout(self):

        exp_subtitle = Div(text="Nightly Progress", css_classes=['subt-style'])
        inst="""<ul>
        <li>Throughout the night record the progress, including comments on calibrations and exposures. 
        All exposures are recorded in the eLog, so only enter information that can provide additional information.</li>
        <li> You can make a comment that is either associated with a <b>Time</b> or <b>Exposure</b>. Select which you will use.
        <ul class="square">
         <li> If you want to comment on a specific Exposure Number, the Night Log will include data from the eLog and combine it with any inputs
        from the Data Quality Scientist for that exposure. </li>
         <li> Note: The timestamp for your input may change to align with the final timestamp of the exposure. </li>
         <li> You can either select an exposure from the drop down menu or enter it yourself. Your comment will be attached to whichever exposure is shown in the `Exposure` window. </li> 
         </ul>
        </li>
        <li>If you'd like to modify a submitted comment, enter the Time of the submission and hit the <b>Load</b> button. 
        If you forget when a comment was submitted, check the Current NL. This will be the case for submissions made by Exposure number as well.
        After making your modifications, resubmit using the <b>Add/Update</b>.</li>
                <li>If you identify an exposure that should not be processed, e.g. a "bad" exposure, submit it below. If all cameras/spectrographs are bad, select <b>All Bad</b>. If
        only a few cameras have problems, select <b>Select Cameras</b> and identify which have the problem.</li> 
        </ul>
        """
        exp_inst = Div(text=inst, css_classes=['inst-style'], width=1000)
        self.exp_comment = TextAreaInput(title='Comment/Remark', placeholder='Humidity high for calibration lamps',rows=6, cols=5, width=800, max_length=10000)
        self.exp_time = TextInput(placeholder='20:07', width=100) #title ='Time in Kitt Peak local time*', 
        self.exp_btn = Button(label='Add/Update', css_classes=['add_button'], width=200)
        self.exp_load_btn = Button(label='Load', css_classes=['load_button'], width=75)
        self.exp_delete_btn = Button(label='Delete', css_classes=['delete_button'], width=75)
        self.exp_alert = Div(text=' ', css_classes=['alert-style'], width=500)

        self.exp_select = Select(title='List of Exposures', options=['None'],width=150)
        self.exp_enter = TextInput(title='Exposure', placeholder='12345', width=150)
        self.exp_update = Button(label='Update Selection List', css_classes=['connect_button'], width=200)
        self.exp_option = RadioButtonGroup(labels=['(1) Select','(2) Enter'], active=0, width=200)
        self.os_exp_option = RadioButtonGroup(labels=['Time','Exposure'], active=0, width=200)

        self.exp_comment.placeholder = 'CCD4 has some bright columns'
        self.quality_title = Div(text='Data Quality: ', css_classes=['inst-style'])
        self.quality_list = ['Good','Not Sure','No Data','Bad']
        self.quality_btns = RadioButtonGroup(labels=self.quality_list, active=0)

        #bad exposures
        self.bad_subt = Div(text='Please Select Which is True about the Bad Exposure: ', css_classes=['subt-style'], width=500)
        self.bad_subt_2 = Div(text='Select Which Cameras are Bad: ', css_classes=['subt-style'], width=500)

        self.bad_alert = Div(text='', css_classes=['alert-style'], width=500)
        self.all_button = Button(label='Full exposure will be added to "bad exp list"', width=500, css_classes=['add_button'])
        self.partial_button = Button(label='Exposure is only partially bad (only certain cameras)', width=500, css_classes=['add_button'])

        hdrs = [Div(text='Spectrograph {}: '.format(i), width=150) for i in range(10)]
        self.bad_cams_0 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_1 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_2 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_3 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_4 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_5 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_6 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_7 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_8 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)
        self.bad_cams_9 = CheckboxButtonGroup(labels=['All','B','R','Z'], active=[], orientation='horizontal', width=300)

        self.bad_add = Button(label='Add to bad exposure list', css_classes=['add_button'], width=200)
        self.bad_layout_1 = layout(self.bad_subt,
                                    self.all_button,
                                    self.partial_button)
        self.bad_layout_2 = layout(self.bad_subt_2,
                                    [hdrs[0], self.bad_cams_0],
                                    [hdrs[1], self.bad_cams_1],
                                    [hdrs[2], self.bad_cams_2],
                                    [hdrs[3], self.bad_cams_3],
                                    [hdrs[4], self.bad_cams_4],
                                    [hdrs[5], self.bad_cams_5],
                                    [hdrs[6], self.bad_cams_6],
                                    [hdrs[7], self.bad_cams_7],
                                    [hdrs[8], self.bad_cams_8],
                                    [hdrs[9], self.bad_cams_9],
                                    self.bad_add)
        #For Lead Observer
        self.exp_layout_0 = layout(children=[self.buffer, self.title,
                                            exp_subtitle,
                                            exp_inst,
                                            self.time_note,
                                            self.os_exp_option,
                                            [self.time_title, self.exp_time, self.now_btn, self.exp_load_btn, self.exp_delete_btn],
                                            [self.exp_select, self.exp_enter],
                                            [self.exp_comment],
                                            [self.img_upinst2, self.img_upload_comments_os],
                                            [self.exp_btn],
                                            self.exp_alert], width=1000)
        self.exp_tab_0 = Panel(child=self.exp_layout_0, title="Exposures")

        #For Support Observer
        self.exp_layout_1 = layout(children=[self.buffer, self.title,
                                            exp_subtitle,
                                            exp_inst,
                                            self.time_note,
                                            self.os_exp_option,
                                            [self.time_title, self.exp_time, self.now_btn, self.exp_load_btn, self.exp_delete_btn],
                                            [self.exp_select, self.exp_enter],
                                            [self.quality_title, self.quality_btns],
                                            [self.exp_comment],
                                            [self.img_upinst2, self.img_upload_comments_os],
                                            [self.exp_btn],
                                            self.exp_alert], width=1000)
        self.exp_tab_1 = Panel(child=self.exp_layout_1, title="Exposures")
        self.exp_tab_2 = Panel(child=self.nonobs_layout_exp, title="Exposures")

    def get_prob_layout(self):
        self.prob_subtitle = Div(text="Problems", css_classes=['subt-style'])
        inst = """<ul>
        <li>Describe problems as they come up, the time at which they occur, the resolution, and how much time was lost as a result. If there is an Alarm ID associated with the problem, 
        include it, but leave blank if not. </li>
        <li>Please enter the time when the problem began, or use the “Now” button if it just occurred.</li>
        <li>If you'd like to modify or add to a submission, you can <b>Load</b> it using its timestamp. 
        If you forget when a comment was submitted, check the Current NL. After making the modifications 
        or additions, press the <b>Add/Update</b> button.</li>
        </ul>
        """
        self.prob_inst = Div(text=inst, css_classes=['inst-style'], width=1000)
        self.prob_time = TextInput(placeholder = '20:07', width=100) #title ='Time in Kitt Peak local time*', 
        self.prob_input = TextAreaInput(placeholder="NightWatch not plotting raw data", rows=10, cols=5, title="Problem Description*:", width=400, max_length=10000)
        self.prob_alarm = TextInput(title='Alarm ID', placeholder='12', width=100)
        self.prob_action = TextAreaInput(title='Resolution/Action', placeholder='description', rows=10, cols=5, width=400, max_length=10000)
        self.prob_btn = Button(label='Add/Update', css_classes=['add_button'])
        self.prob_load_btn = Button(label='Load', css_classes=['load_button'], width=75)
        self.prob_delete_btn = Button(label='Delete', css_classes=['delete_button'], width=75)
        self.prob_alert = Div(text=' ', css_classes=['alert-style'])

        prob_layout = layout([self.buffer,self.title,
                            self.prob_subtitle,
                            self.prob_inst,
                            self.time_note,
                            self.exp_info,
                            [self.time_title, self.prob_time, self.now_btn, self.prob_load_btn, self.prob_delete_btn], 
                            self.prob_alarm,
                            [self.prob_input, self.prob_action],
                            [self.img_upinst2, self.img_upload_problems],
                            [self.prob_btn],
                            self.prob_alert], width=1000)

        self.prob_tab = Panel(child=prob_layout, title="Problems")
        self.prob_tab_1 = Panel(child=self.nonobs_layout_prob, title="Problems")

    def get_weather_layout(self):
    
        self.weather_subtitle = Div(text="Observing Conditions", css_classes=['subt-style'])
        inst = """<ul>
        <li>Every hour, as part of the OS checklist, include a description of the weather and observing conditions.</li>
        <li>The most recent weather and observing condition information will be added to the table below and the Night Log when you <b>Add Weather Description</b>.
        Please note that the additional information may not correlate exactly with the time stamp but are just the most recent recorded values</li>
        <li>If you are not the LO, you can only see their inputs.</li>
        <li>SCROLL DOWN! There are plots of the ongoing telemetry for the observing conditions. These will be added to the Night Log when submitted at the end of the night.</li> 
        </ul>
        """
        self.weather_inst = Div(text=inst, width=1000, css_classes=['inst-style'])

        data = pd.DataFrame(columns = ['Time','desc','temp','wind','humidity','seeing','tput','skylevel'])
        self.weather_source = ColumnDataSource(data)
        obs_columns = [TableColumn(field='Time', title='Time (Local)', width=100, formatter=self.timefmt),
                   TableColumn(field='desc', title='Description', width=250),
                   TableColumn(field='temp', title='Temperature (C)', width=100),
                   TableColumn(field='wind', title='Wind Speed (mph)', width=100),
                   TableColumn(field='humidity', title='Humidity (%)', width=100),
                   TableColumn(field='seeing', title='Seeing (arcsec)', width=100),
                   TableColumn(field='tput', title='Throughput', width=100),
                   TableColumn(field='skylevel', title='Sky Level', width=100)] #, 

        self.weather_table = DataTable(source=self.weather_source, columns=obs_columns, fit_columns=False, width=1000, height=300)

        telem_data = pd.DataFrame(columns =
        ['time','exp','mirror_temp','truss_temp','air_temp','humidity','wind_speed','airmass','exptime','seeing','tput','skylevel'])
        self.telem_source = ColumnDataSource(telem_data)

        plot_tools = 'pan,wheel_zoom,lasso_select,reset,undo,save'
        self.p1 = figure(plot_width=800, plot_height=300, x_axis_label='UTC Time', y_axis_label='Temp (C)',x_axis_type="datetime", tools=plot_tools)
        self.p2 = figure(plot_width=800, plot_height=300, x_axis_label='UTC Time', x_range=self.p1.x_range, y_axis_label='Humidity (%)', x_axis_type="datetime", tools=plot_tools)
        self.p3 = figure(plot_width=800, plot_height=300, x_axis_label='UTC Time', x_range=self.p1.x_range, y_axis_label='Wind Speed (mph)', x_axis_type="datetime", tools=plot_tools)
        self.p4 = figure(plot_width=800, plot_height=300, x_axis_label='UTC Time', x_range=self.p1.x_range, y_axis_label='Airmass', x_axis_type="datetime", tools=plot_tools)
        self.p5 = figure(plot_width=800, plot_height=300, x_axis_label='UTC Time', x_range=self.p1.x_range, y_axis_label='Exptime (sec)', x_axis_type="datetime", tools=plot_tools)
        self.p6 = figure(plot_width=800, plot_height=300, x_axis_label='Exposure', y_axis_label='Seeing (arcsec)', tools=plot_tools)
        self.p7 = figure(plot_width=800, plot_height=300, x_axis_label='Exposure', y_axis_label='Transparency', tools=plot_tools, x_range = self.p6.x_range)
        self.p8 = figure(plot_width=800, plot_height=300, x_axis_label='Exposure', y_axis_label='Sky Level', tools=plot_tools, x_range=self.p6.x_range)

        self.p1.circle(x='time',y='mirror_temp', source=self.telem_source, color='orange', size=10, alpha=0.5)
        self.p1.circle(x='time',y='truss_temp', source=self.telem_source, size=10, alpha=0.5) 
        self.p1.circle(x='time',y='air_temp', source=self.telem_source, color='green', size=10, alpha=0.5) 
        self.p1.legend.location = "top_right"

        self.p2.circle(x='time', y='humidity', source=self.telem_source, size=10, alpha=0.5)
        self.p3.circle(x='time', y='wind_speed', source=self.telem_source, size=10, alpha=0.5)
        self.p4.circle(x='time', y='airmass' ,source=self.telem_source, size=10, alpha=0.5)
        self.p5.circle(x='time', y='exptime', source=self.telem_source, size=10, alpha=0.5)
        self.p6.circle(x='exp', y='seeing', source=self.telem_source, size=10, alpha=0.5)
        self.p7.circle(x='exp', y='tput', source=self.telem_source, size=10, alpha=0.5)
        self.p8.circle(x='exp', y='skylevel', source=self.telem_source, size=10, alpha=0.5)
        self.bk_plots = column(self.p1, self.p2, self.p3, self.p4, self.p5, self.p6, self.p7, self.p8)

        self.weather_desc = TextInput(title='Weather Description', placeholder='description', width=500)
        self.weather_btn = Button(label='Add Weather Description', css_classes=['add_button'], width=100)
        self.weather_alert = Div(text=' ', css_classes=['alert-style'])
        self.plots_subtitle = Div(text='Telemetry Plots', css_classes=['subt-style'],width=800)

        #For Lead Observer
        weather_layout_0 = layout([self.buffer,self.title,
                        self.weather_subtitle,
                        self.weather_inst,
                        [self.weather_desc, self.weather_btn],
                        self.weather_alert,
                        self.weather_table,
                        self.plots_subtitle,
                        self.bk_plots], width=1000)

        self.weather_tab_0 = Panel(child=weather_layout_0, title="Observing Conditions")

        #For Support Observer
        weather_layout_1 = layout([self.buffer,self.title,
                        self.weather_subtitle,
                        self.weather_inst,
                        self.weather_alert,
                        self.weather_table,
                        self.plots_subtitle,
                        self.bk_plots], width=1000)

        self.weather_tab_1 = Panel(child=weather_layout_1, title="Observing Conditions")

    def get_checklist_layout(self):
        
        self.checklist = CheckboxGroup(labels=["Did you check the weather?", "Did you check the guiding?", "Did you check the positioner temperatures?","Did you check the FXC?", "Did you check the Spectrograph Cryostat?","Did you check the FP Chiller?"])
        
        inst="""
        <ul>
        <li>Once an hour, complete the checklist below.</li>
        <li>In order to <b>Submit</b>, you must check each task. You do not need to include a comment.</li>
        </ul>
        """
        self.checklist_inst = Div(text=inst, css_classes=['inst-style'], width=1000)

        self.check_time = TextInput(placeholder='20:07')
        self.check_alert = Div(text=" ", css_classes=['alert-style'])
        self.check_btn = Button(label='Submit', css_classes=['connect_button'])
        self.check_comment = TextAreaInput(title='Comment', placeholder='comment if necessary', rows=3, cols=3)
        
        self.check_subtitle = Div(text="LO Checklist", css_classes=['subt-style'])
        
        checklist_layout = layout(self.buffer,self.title,
                                self.check_subtitle,
                                self.checklist_inst,
                                self.checklist,
                                self.check_comment,
                                [self.check_btn],
                                self.check_alert, width=1000)
        self.check_tab = Panel(child=checklist_layout, title="Checklist")

    def get_nl_layout(self):
        self.nl_subtitle = Div(text="Current DESI Night Log: {}".format(self.nl_file), css_classes=['subt-style'])
        self.nl_text = Div(text=" ", width=800)
        self.nl_alert = Div(text='You must be connected to a Night Log', css_classes=['alert-style'], width=500)
        self.nl_submit_btn = Button(label='Submit NightLog & Publish NightSummary (Only Press Once - this takes a few minutes)', width=800, css_classes=['add_button'])
        self.submit_text = Div(text=' ', css_classes=['alert-style'], width=800)
        
        self.exptable_alert = Div(text=" ", css_classes=['alert-style'], width=500)

        exp_data = pd.DataFrame(columns=['date_obs','id','program','sequence','flavor','exptime','airmass','seeing'])
        self.explist_source = ColumnDataSource(exp_data)

        exp_columns = [TableColumn(field='date_obs', title='Time (UTC)', width=50, formatter=self.datefmt),
                   TableColumn(field='id', title='Exposure', width=50),
                   TableColumn(field='sequence', title='Sequence', width=100),
                   TableColumn(field='flavor', title='Flavor', width=50),
                   TableColumn(field='exptime', title='Exptime', width=50),
                   TableColumn(field='program', title='Program', width=300),
                   TableColumn(field='airmass', title='Airmass', width=50),
                   TableColumn(field='seeing', title='Seeing', width=50)]

        self.exp_table = DataTable(source=self.explist_source, columns=exp_columns, width=1000)

        #For Lead Observer
        nl_layout_0 = layout([self.buffer,self.title,
                    self.nl_subtitle,
                    self.nl_alert,
                    self.nl_text,
                    self.exptable_alert,
                    self.exp_table,
                    self.submit_text,
                    self.nl_submit_btn], width=1000)

        self.nl_tab_0 = Panel(child=nl_layout_0, title="Current DESI Night Log")

        #For Support Observer
        nl_layout_1 = layout([self.buffer,self.title,
                    self.nl_subtitle,
                    self.nl_alert,
                    self.nl_text,
                    self.exptable_alert,
                    self.exp_table], width=1000)

        self.nl_tab_1 = Panel(child=nl_layout_1, title="Current DESI Night Log")

    def get_ns_layout(self):
        self.ns_subtitle = Div(text='Night Summaries', css_classes=['subt-style'])
        self.ns_inst = Div(text='Enter a date to get previously submitted NightLogs', css_classes=['inst-style'])
        self.ns_date_btn = Button(label='Get NightLog', css_classes=['init_button'])
        self.ns_date = datetime.datetime.now().strftime('%Y%m%d') 
        self.ns_date_input = TextInput(title='Date', value=self.ns_date)
        self.ns_next_date_btn = Button(label='Next Night', css_classes=['add_button'])
        self.ns_last_date_btn = Button(label='Previous Night', css_classes=['load_button'])
        self.ns_html = Div(text='',width=800)

        ns_layout = layout([self.buffer,
                            self.ns_subtitle,
                            self.ns_inst,
                            [self.ns_date_input, self.ns_date_btn],
                            [self.ns_last_date_btn, self.ns_next_date_btn],
                            self.ns_html], width=1000)
        self.ns_tab = Panel(child=ns_layout, title='Night Summary Index')

