from __future__ import print_function
from ECLAPI import ECLConnection
import getopt, sys, os
from datetime import datetime

Usage = """
python ecl_list.py [<options>]
Options are:
    -c <category>
    -t <tag>
    -f <form>
    -a "yyyy-mm-dd hh:mm:ss"
    -a <n>days
    -a <n>hours
    -a <n>minutes
    -l <limit>
    -U <url>
"""

if not sys.argv[1:]:
    print(Usage)
    sys.exit(1)

URL = "http://dbweb6.fnal.gov:8080/ECL/demo"
user = "xml"
password = "password"

cat = None
form = None
limit = None
tag = None
after = None

opts, args = getopt.getopt(sys.argv[1:], 'c:f:l:t:a:U:')
for opt, val in opts:
    if opt == '-c':
        cat = val
    elif opt == '-f':
        form = val
    elif opt == '-l':
        limit = int(val)
    elif opt == '-t':
        tag = val
    elif opt == '-a':
        after = val
    elif opt == '-U':
        URL = val

conn = ECLConnection(URL, user, password)
lst = conn.list(category=cat, form=form, limit=limit, tag=tag, after=after)
for i in lst:
    print(i)
