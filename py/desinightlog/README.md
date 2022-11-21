# desinightlog python code

This directory includes:
* **ObserverReport/main.py**: Runs the Bokeh application. Contains all the widgets used in application.
* **ObserverReport/templates**: contains css and html info
* **layout.py**: Contains the Bokeh layout info. This is where the layout elements and widgets are initialized
* **report.py**: Contains the functions of the Bokeh application. Send inputs on the Bokeh application to the NightLog. Also submits NightLog
* **nightlog.py**: Takes inputs from Report(), saves them to csv files, and compiles and publishes the NightLog

To run the Bokeh application for testing purposes, best to do so on the desi server:
* `ssh -XY desiobserver@desi-14.kpno.noao.edu` (requires VPN)
* `cd ~/obsops/desinightlog`
* Make sure that NL_DIR and NW_DIR are defined in ObserverReport.py
* Set self.test=True in report.py
* To run:
 * `cd py/desinightlog `
 * `bokeh serve ObserverReport --allow-websocket-origin=desi-14.kpno.noao.edu:5006`
 * access in browser at http://desi-14.kpno.noao.edu/5006

