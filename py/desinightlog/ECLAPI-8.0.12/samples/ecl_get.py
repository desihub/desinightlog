from __future__ import print_function
from ECLAPI import ECLConnection
import getopt, sys

Usage = """
python ecl_get.py [-U <url>] <entry-id>
"""

if not sys.argv[1:]:
    print(Usage)
    sys.exit(1)
    
URL = "http://dbweb6.fnal.gov:8080/ECL/demo"
user = "xml"
password = "password"

opts, args = getopt.getopt(sys.argv[1:], '-U')
for opt, val in opts:
    if opt == '-U': URL = val

conn = ECLConnection(URL, user, password)
xml = conn.get(int(sys.argv[1]))
print(xml)
