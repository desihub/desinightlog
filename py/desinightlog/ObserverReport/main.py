"""
Created on July 21, 2021

@author: Parker Fagrelius

Updated DESI_Night_Log/OS_Report for single observer platform

"""

import os
import sys

from bokeh.io import curdoc
from bokeh.models.widgets.markups import Div
from bokeh.models.widgets import Tabs

sys.path.append(os.getcwd())
sys.path.append('./ECLAPI-8.0.12/lib')
from report import Report

if os.environ['USER'].lower() == 'desiobserver':
    os.environ['NL_DIR'] = '/n/home/desiobserver/nightlogs'
    os.environ['NW_DIR'] = '/exposures/desi'
elif os.environ['USER'].lower() == 'msdos':
    assert(os.environ['NL_DIR'])
    assert(os.environ['NW_DIR'])

class Obs_Report(Report):
    def __init__(self):
        Report.__init__(self)

        self.title = Div(text="DESI Nightly Intake", css_classes=['h1-title-style'], width=1000)

        desc = """
        To begin, connect to the observing night Night Log using the list of Existing Night Logs. Add information about the Observers and press the 
        Update Tonight's Log. 
        Throughout the night, enter information about the exposures, problems that occur, and observing conditions. Complete the 
        Checklist at least once every hour. NOTE: If inputs are being made into a DNI at both KPNO and NERSC, the inputs
        made at KPNO for certain things (meta data, plan, milestones), will be prioritized over those made at NERSC.
        """
        self.instructions = Div(text=desc, css_classes=['inst-style'], width=500)
        
        self.page_logo = Div(text="<img src='ObserverReport/static/logo.png'>", width=350, height=300)

    def get_layout(self):
        self.get_intro_layout()
        self.update_nl_list()
        self.get_nonobs_layout()
        self.get_plan_layout()
        self.get_milestone_layout()
        self.get_exp_layout()
        self.get_prob_layout()
        self.get_weather_layout()
        self.get_checklist_layout()
        self.get_nl_layout()
        self.get_ns_layout()
        
        self.layout = Tabs(tabs=[self.intro_tab, self.ns_tab], css_classes=['tabs-header'], sizing_mode="scale_both")

    def run(self):
        self.get_layout()

        self.now_btn.on_click(self.time_is_now)
        self.init_btn.on_click(self.add_observer_info)
        self.connect_btn.on_click(self.connect_log)
        self.exp_btn.on_click(self.exp_add)
        self.exp_load_btn.on_click(self.exposure_load)
        self.prob_load_btn.on_click(self.problem_load)
        self.weather_btn.on_click(self.weather_add)
        self.prob_btn.on_click(self.prob_add)
        self.nl_submit_btn.on_click(self.nl_submit)
        self.check_btn.on_click(self.check_add)
        self.milestone_btn.on_click(self.milestone_add)
        self.milestone_new_btn.on_click(self.milestone_add_new)
        self.milestone_load_btn.on_click(self.milestone_load)
        self.plan_btn.on_click(self.plan_add)
        self.plan_new_btn.on_click(self.plan_add_new)
        self.plan_load_btn.on_click(self.plan_load)
        self.plan_delete_btn.on_click(self.plan_delete)
        self.milestone_delete_btn.on_click(self.milestone_delete)
        self.exp_delete_btn.on_click(self.progress_delete)
        self.prob_delete_btn.on_click(self.problem_delete)
        self.contributer_btn.on_click(self.add_contributer_list)
        self.exp_select.on_change('value',self.select_exp)
        self.summary_btn.on_click(self.summary_add)
        self.time_btn.on_click(self.add_time)
        self.summary_load_btn.on_click(self.summary_load)
        self.ns_date_btn.on_click(self.get_nightsum)
        self.ns_next_date_btn.on_click(self.ns_next_date)
        self.ns_last_date_btn.on_click(self.ns_last_date)
        self.nonobs_btn_exp.on_click(self.nonobs_entry_exp)
        self.nonobs_btn_prob.on_click(self.nonobs_entry_prob)
        self.all_button.on_click(self.add_all_to_bad_list)
        self.partial_button.on_click(self.add_some_to_bad_list)
        self.bad_add.on_click(self.bad_exp_add)
        
OBS = Obs_Report()
OBS.run()
curdoc().title = 'DESI Night Log'
curdoc().add_root(OBS.layout)
curdoc().add_periodic_callback(OBS.current_nl, 30000) #Every 30 seconds
curdoc().add_periodic_callback(OBS.get_exposure_list, 30000) #Every 30 seconds
