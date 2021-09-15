# desinightlog

This is referred to as both the DESI NightLog as well as the DESI Nightly Input (DNI)

This code is running at KPNO on the desi-server and at NERSC. The applications can be accessed here:
* https://nightlog.desi.lbl.gov/ObserverReport
* http://desi-www.kpno.noao.edu:5006/ObserverReport

Details below describe additional information on how these applications are being run.

KPNO:
* Running under dosmanager@desi-server. This is only accessible by the DESI ICS team
* Nightlogs are saved to /software/www2/html/nightlogs/
* Code running from 
* Logs for running the Nightlog: /data/msdos/desinightlog/logs
* Directory structure is set up daily at 12pm MST using a cronjob to ensure permissions are correctly set.
 * msdos@desi-observer:/data_local/home/msdos/parkerf/desilo/nl_dir.sh

NERSC:
* Running as a Rancher2 Spin Application
* Nightlogs are saved to /global/cfs/cdirs/desi/survey/ops/nightlogs
* Code running from /global/common/software/desi/users/parkerf/desinightlog/py/desinightlog
* Logs for running Nightlog: 


Files are transferred between NERSC and KPNO according to https://github.com/desihub/desitransfer
