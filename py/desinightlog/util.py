#############################################################################
#
#        $Id: util.py
#       DOS Utilities
#
#       This is a collection of utility functions
#           ArgParser     - parse command line arguments
#           parse         - parse PML commands
#           float2ra      - Convert RA (dec) from degrees (float) to HH:MM:SS.SS format
#           float2dec
#           ra2float      - Convert RA (dec) from HH:MM:SS.SS format to degrees as a float
#           dec2float
#           radec2J2000   - Convert current values for RA, dec to J2000
#           J20002radec   - Convert RA, dec (J2000) to current values
#           slew_angle    - Calculate the angular distance between two pointings
#           sidereal_time - Returns the local sidereal time for KPNO or the given observer
#           moon_angle    - Returns moon position at KPNO as (ra, dec) in degrees (either current or at a given date)
#           moon_separation Returns angle in degrees between moon and telescope position
#           ctio_observer - Setup pyephem observer for CTIO
#           kpno_observer - Setup pyephem observer for KPNO
#           azel2radec    - Convert azimuth, elevation to ra, dec for a given (pyephem) observer
#           hadec2radec   - Convert hour angle, declination to ra, dec for a given (pyephem) observer
#           str2dict      - convert a list of space separated key=value pairs to a dictionary
#           bytes2human   - convert diskspace information to human readable information
#           dos_parser    - parses an argument list 
#           required_roles  Returns a list of all started roles using the specified product
#           get_option    - Returns the specified option from the .ini file for the selected role
#           desymbolize   - Resolve env. variables in the given text string
#           gethostname   - Checks if socket.gethostname() returns a valid host. If not, calls fcntl
#           getnetworkinfo- Returs a dictionary with network information. Keys are addr, netmask, interface
#           getopenport   - Returns an open (socket) port
#           dict2rec      - Convert a list of dictionaries to a numpy recarray
#           rec2dict      - Convert a numpy recarray to a list of dictionaries
#           encode/decode - simple string encoding and decoding routines
#           simple_log    - Print formatted log message in case a logger is not available
#           raise_error   - Raise a RuntimeError exception
#           obs_day       - Returns observing day as string in YYYYMMDD format (changes at noon)
#           FifoList      - (class) simple fifo implementation
#           make_dir      - Create a directory, if necessary, (multiple tries)
#           caller_info   - Returns IP address and socket of the current (Pyro) client
#           write_to_logbook - Write a dictionary to a form in the electronic logbook
#           doc_inherit   - Inherit doc strings from super class
#           sm2sp         - Convert spectrograph serial number to DOS logical index (needs updating when hardware changes)
#           gfa_detrend   - overscan correct GFA image, return datasec part of array)
#           df2rec        - convert pandas data frame to numpy recarray with text columns converted to S (not unicode) for fitsio
#           checksum      - write a sha256 checksum file
#           find_exposure - query DB to find a given reqid/expid.
#           coords2df     - read coordinates fits file and convert to dataframe
#           fiberassign2df   - read a fiberassign file and convert to dataframe
#           fiberassign2targets = read fiberassign file and return targets bit mask
#           disabled_bitmask - return disabled positioner bitmask for GUI
#############################################################################

import traceback
import math
import datetime
from collections import OrderedDict
import threading
import time
import random
import json
import numpy as np
try:
    import pandas as pd
except:
    pd = None
import glob
try:
    from astropy.stats import sigma_clipped_stats
    sigma_clip_available = True
except:
    sigma_clip_available = False
from functools import wraps
try:
    import ephem
    ephem_available = True
except:
    ephem_available = False 
    # print('pyephem module not loaded. Not all functions from the util package will be available.')
import os
from pyparsing import *
import struct
try:
    import fcntl
except:
    pass
import socket                  
#from netifaces import interfaces, ifaddresses, AF_INET
#import Pyro4
try:
    import ECLAPI
    eclapi_available = True
except Exception:
    eclapi_available = False

# Positioner Flags (see DOS wiki)
POSITIONER_FLAGS = {'POS_MATCHED' : 1,
                    'POS_PINHOLE' : 2,
                    'POS_POS' : 4,
                    'POS_FIF' : 8,
                    'POS_FVCERROR' : 16,
                    'POS_FVCBAD' : 32,
                    'POS_MOTION' : 64,
                    'POS_GIF' : 128,
                    'POS_ETC' : 256,
                    'POS_FITTEDPINHOLE' : 1<<9,
                    'POS_MATCHEDCENTER' : 1<<10,
                    'POS_AMBIGUOUS' : 1<<11,
                    'POS_CONVERGED' : 1<<15,
                    'POS_CTRLDISABLED' : 1<<16,
                    'POS_FIBERBROKEN' : 1<<17,
                    'POS_COMERROR' : 1<<18,
                    'POS_OVERLAP' : 1<<19,
                    'POS_FROZEN' : 1<<20,
                    'POS_UNREACHABLE' : 1<<21,
                    'POS_BOUNDARIES' : 1<<22,
                    'POS_MULTIPLE' : 1<<23,
                    'POS_NONFUNCTIONAL' : 1<<24,
                    'POS_REJECTED' : 1<<25,
                    'POS_EXPERTLIMIT' : 1<<26,
                    'POS_BADNEIGHBOR' : 1<<27,
                    'POS_MISSINGSPOT' : 1<<28,
                    'POS_BADPERFORMANCE' : 1<<29,
                    'POS_DISABLED' : 0xFFFF0000,
                    'POS_CONVERGEDPOS' : 0x8004,
                    'POS_UNMATCHEDPOS' : 0xFFFF0004,
                    'POS_UNMATCHEDFIF' : 0xFFFF0008,
                    'POS_UNMATCHEDGIF' : 0xFFFF0088,
                    'POS_UNMATCHEDETC' : 0xFFFF0104,
                   }

#############################################################################
#
# A simple argument parser derived from the ArgParser in Pyro.util
#
# Options are given in the format -<option>  <value>
# <value> is optional
# Parameter:
#    args       - list of command line arguments (sys.argv[1:])
#
# Methods:
#    parse      - parses the args list
#    hasOption  - returns True is the option is given in args
#    getValue   - returns the value given for an option, raises KeyError is invalid option
#    getListofOptions   - returns a list of options found in args For convenience (ArgParser.options is public)
#
############################################################################

class ArgParser:
    def __init__(self):
        self.options = {}
                
    def parse(self, args):
        self.options = {}
                        
        if type(args) == type(''):
            args = args.split()            # got a string, split it
            
        while args:
            arg = args[0]
            del args[0]
            arg = arg.replace('--','-')
            if arg[0]=='-':
                if args:
                    value = args[0]
                    if value[0]=='-':    # got another option
                        self.options[arg[1:]] = None
                    else:
                        self.options[arg[1:]] = value
                        del args[0]
                else:
                    self.options[arg[1:]] = None
            else:
                self.options['REST'] = arg
                break

    def hasOption(self, option):
        return option in self.options
        
    def getValue(self, option, default=Exception()):
        try:
            return self.options[option]
        except KeyError:
            if not isinstance(default,Exception):
                return default
            raise KeyError('No such option: %s' % option)
    def getListofOptions(self):
        return [opt for opt in self.options]

##############################################################################
def dos_parser(*args, **kwargs):
    """
    parse a PML parameter string
    Parameters of a PML call can be encoded in a string, can be Python objects (args and kwargs) or a json string
    This routine converts the input information into a list (args) and a keyword dictionary (named)
    """
    parseAll = kwargs.pop('parseAll', True)
    iargs = list(args)
    ikwargs = dict(kwargs)
    # regular arguments
    a = []
    # named arguments
    named = {}
    named.update(ikwargs)
    # empty args?
    if len(iargs) == 0:
        return a, named
    # If more than 1 element? Then just copy and return
    if iargs[-1] == {}:
        del iargs[-1]
    if len(iargs) != 1:
        if len(iargs)>1:
            for i in range(len(iargs)):
                a.append(iargs[i])
        return a, named
    # exactly one argument - could be a DOS encoded string or a json string
    try:
        # Check for some common types:
        if type(iargs[0]) in [list, tuple, float, int]:
            a = [iargs[0]]
            return a, named
        elif type(iargs[0]) is dict:
            named.update(iargs[0])
            return a, named
        # json formatted?
        arg = json.loads(iargs[0])
        if type(arg) is list:
            a[0] = list(arg)
        elif type(arg) is dict:
            named.update(arg)
        elif type(arg) is str or type(arg) is str:
            a[0] = str(arg)
        else:
            a[0] = arg
        return a, named
    except:
        # DOS formatted string
        # We allow the following constructs (the command shown in [] is not included in the string):
        # [get] status
        # [get] interlock all
        # [set] vccd=on
        # [set] filter=4
        # [set] filter=[r,g,u]
        # move ra=12:00:00.0, dec = -1:00:00.12, epoch=2000
        # Returns the list of arguments in args and all key=value pairs in the named directory
        # For example, [get] interlock all returns args = ['interlock', 'all'], named = {} or
        # move ra=12:00:00.0... returns a = [], named = {"ra" : "12:00:00.0", "dec" : "-1:00:00.12", "epoch" : 2000}
        pass
    cvtBool = lambda toks: 'T' in str(toks[0]).upper()                                                                                   
    cvtInt = lambda toks: int(toks[0])                                                                                   
    cvtReal = lambda toks: float(toks[0])                                                                                
    cvtTuple = lambda toks : tuple(toks.asList())                                                                        
    cvtList = lambda toks : list(toks.asList())                                                                        
    cvtDict = lambda toks: dict(toks.asList())                                                                           
    conList = lambda con: list(con[0])
    # define punctuation as suppressed literals                                                                          
    lparen,rparen,lbrack,rbrack,lbrace,rbrace,colon = list(map(Suppress,"()[]{}:"))                                                                                          
    integer = Combine(Optional(oneOf("+ -")) + Word(nums)).setName("integer").setParseAction( cvtInt )
    real = Combine(Optional(oneOf("+ -")) + Word(nums) + "." + Optional(Word(nums)) + Optional(oneOf("e E")+Optional(oneOf("+ -")) +Word(nums))).setName("real").setParseAction( cvtReal )
    coord = Combine(Optional(oneOf("+ -")) + Word(nums) + ":" + Word(nums) + ":" + Word(nums) + Optional("." + Word(nums)))
    boolean = oneOf("false False FALSE true True TRUE").setParseAction( cvtBool ) 
    tupleStr = Forward()                                                                                                 
    listStr = Forward()                                                                                                  
    dictStr = Forward()                                                                                                  
    listItem = coord|real|integer|quotedString.setParseAction(removeQuotes)| Group(listStr) | tupleStr | dictStr | boolean |Word( srange("[a-zA-Z_/\$]"), srange("[a-zA-Z0-9_/\$]"))
    tupleStr << ( Suppress("(") + Optional(delimitedList(listItem)) + Optional(Suppress(",")) + Suppress(")") )
    tupleStr.setParseAction( cvtTuple )                                                                                  
    listStr << ( lbrack + Optional(delimitedList(listItem) + Optional(Suppress(","))) + rbrack )
    listStr.setParseAction( cvtList )                           
    dictEntry = Group( listItem + colon + listItem )                                                                     
    dictStr << (lbrace + Optional(delimitedList(dictEntry) + Optional(Suppress(","))) + rbrace)                                                                               
    dictStr.setParseAction( cvtDict )                                                                                    

    key = listItem + Optional(Suppress(","))
    arg = listItem + NotAny("=") + Optional(Suppress(","))
    kv = Word( srange("[a-zA-Z_]"), srange("[a-zA-Z0-9_]") ) + "=" + listItem + Optional(Suppress(","))
    option1 = OneOrMore(kv)
    option2 = OneOrMore(arg) + ZeroOrMore(kv)

    try:                                                                                                             
        result = option1.parseString(iargs[0], parseAll = parseAll)
    except:
        try:
            result = option2.parseString(iargs[0], parseAll = parseAll)
        except:
            raise Exception('Parse error: Invalid string %s' % repr(iargs[0]))

    # now parse results list
    result = result.asList()
    if '=' not in result or len(result)<2:
        a = list(result)
    else:
        # do we have leading key words?
        a = []
        for i in range(len(result)):
            if  '=' != result[i+1]:
                a.append(result[i])
            else:
                break
        if i > len(result)-2:
            raise Exception('Parse error: Invalid parsing results %s' % repr(iargs[0]))
        for i in range(i,len(result),3):
            named[result[i]] = result[i+2]
    return a, named

def float2ra(ra):
    """
    Convert degrees as float into time string, HH:MM:SS.SS
    dec has to be of type float or a string like '34.222'
    360 deg = 24 hrs, 360/24 = 15
    """
    if ra is None:
        return ra
    if type(ra) is str or type(ra) is str:
        if ':' in str(ra):
            # if the string is not properly formatted this will throw an exception
            b=ra2float(ra)
            return ra
        float_ra = float(ra)
    else:
        float_ra = ra
    assert type(float_ra) is float,'Invalid parameter format (incorect data type: %r)' % type(ra)
    if float_ra < 0.0:
        sign = '-'
    else:
        sign = ''
    float_ra = abs(float_ra)
    hrs = float_ra/15. 
    hours = math.trunc(hrs)
    min = abs(hrs - hours) * 60.
    minutes = math.trunc(min) 
    seconds = round((min - minutes) * 60,3)
    if seconds == 60.0:
        seconds = 0.0
        minutes += 1
        if minutes == 60:
            minutes = 0
            hours += 1
            if hours == 24:
                hours = 0
    return sign+'%02i:%02i:%06.3f' % (hours, minutes, seconds)

def ra2float(ra):
    """
    Convert ra to degress (float).
    ra can be given as a time string, HH:MM:SS.SS or as string like '25.6554'
    or (trivially) as a float.
    An exception is thrown if ra is invalid
    360 deg = 24 hrs, 360/24 = 15
    """
    if ra is None:
        return ra
    if type(ra) is float or type(ra) is int:
        return float(ra)
    if (type(ra) is str or type(ra) is str) and ra.find(':') == -1:
        return float(ra)     
    try:
        return float(ra)      # catch numpy types
    except:
        pass
    assert type(ra) is str,'Invalid parameter format (ra2float - data type %r)' % type(ra)
    h,m,s = ra.strip().split(':')
    if h.find('-') != -1:
        h=h.replace('-','')
        sign = -1.0
    else:
        sign = 1.0
    return sign*(float(h)*15.0 + float(m)/4.0 + float(s)/240.0)

def float2dec(dec):
    """
    Convert degrees as float into degree string, DD:MM:SS.SS
    dec has to be of type float or a string like '34.222'
    """
    if dec is None:
        return dec
    if isinstance(dec, str):
        if ':' in str(dec):
            # if the string is not properly formatted this will throw an exception
            b=dec2float(dec)
            return dec
        float_dec = float(dec)
    else:
        float_dec = dec
    assert type(float_dec) is float,'Invalid parameter format (incorrect data type %r)' % type(dec)    
    if float_dec < 0.0:
        sign = '-'
        float_dec = abs(float_dec)
    else:
        sign = ''
    degrees = math.trunc(float_dec)
    min = abs(float_dec - degrees) * 60
    minutes = math.trunc(min)
    seconds = round((min - minutes) * 60,3)
    if seconds == 60.0:
        seconds = 0.0
        minutes += 1
        if minutes == 60:
            minutes = 0
            degrees += 1
    return '%s%02i:%02i:%06.3f' % (sign, degrees, minutes, seconds)

def dec2float(dec):
    """
    Convert dec to degress (float).
    dec can be given as a time string, HH:MM:SS.SS or as string like '25.6554'
    or (trivially) as a float.
    An exception is thrown if dec is invalid
    """
    if dec is None:
        return dec
    if type(dec) is float or type(dec) is int:
        return float(dec)
    if (type(dec) is str or type(dec) is str) and dec.find(':') == -1:
        return float(dec)     
    try:
        return float(dec)      # catch numpy types
    except:
        pass
    assert type(dec) is str,'Invalid parameter format (dec2float - data type %r)' % type(dec)
    d,m,s = dec.strip().split(':')
    if d.find('-') != -1:
        d=d.replace('-','')
        sign = -1.0
    else:
        sign = 1.0
    return sign*(float(d) + float(m)/60.0 + float(s)/3600.0)

def _supplement(fixed, date):
    T = (float(fixed) - 2000.0)/100.0
    t = (float(date) - float(fixed))/100.0
    asec = (2306.218 + 1.397*T)*t +1.095*t*t
    bsec = (2004.311 - 0.853*T)*t -0.427*t*t
    csec = (2306.218 + 1.397*T)*t + 0.302*t*t
    torad = 180.0/math.pi * 3600.0
    m = _makeMatrixSupplement( asec/torad, bsec/torad, csec/torad)
    return m

def _makeMatrixSupplement(a,b,c):
    m = [0.0 for x in range(9)]
    cA = math.cos(a)
    sA = math.sin(a)
    cB = math.cos(b)
    sB = math.sin(b)
    cC = math.cos(c)
    sC = math.sin(c)
    m[0] = cA * cB * cC - sA * sC
    m[3] = -cA * cB *sC - sA * cC
    m[6] = - cA * sB
    m[1] = sA * cB * cC + cA * sC
    m[4] = - sA * cB * sC + cA * cC
    m[7] = -sA * sB
    m[2] = sB * cC
    m[5] = -sB * sC
    m[8] = cB
    return m

def str2dict(dict_string):
    """
    Convert a string of key=value pairs to a dictionary.
    Format is 'KEY=value KEY=other value KEY=and some more values'
    For example: str2dict('key1 = 1 key2 = a b c key3=23.4) returns the dictionary
    {'key1':'1' , 'key2':'a b c', 'key3':'23.4'}
    """
    string_bits = dict_string.split('=')
    keys = [string_bits[0].strip()]
    values = []
    for bits in string_bits[1:-1]:
        pieces = bits.strip().rsplit(' ', 1)
        if len(pieces) == 1:
            key = pieces[0]
            value = 'NONE'
        else:
            key = pieces[1]
            value = pieces[0]
        keys.append(key)
        values.append(value)
    values.append(string_bits[-1])
    return dict(list(zip(keys, values)))

def radec2J2000(RA,DEC):
    toDegrees = 180.0/math.pi
    # to radians
    ra = ra2float(RA)/180.0*math.pi
    dec = dec2float(DEC)/180.0*math.pi
    year = datetime.datetime.now().year
    m = _supplement(2000.0, year)
    results =  _Transform(ra, dec, m)
    # back to degrees
    results[0] = results[0] * 180.0/math.pi
    results[1] = results[1] * 180.0/math.pi
    return results

def J20002radec(RA,DEC):
    toDegrees = 180.0/math.pi
    # to radians
    ra = ra2float(RA)/180.0*math.pi
    dec = dec2float(DEC)/180.0*math.pi
    year = datetime.datetime.now().year
    m = _supplement(year, 2000.0)
    results =  _Transform(ra, dec, m)
    # back to degrees
    results[0] = results[0] * 180.0/math.pi
    results[1] = results[1] * 180.0/math.pi
    return results

def _Transform(ra, dec, m):
    r0 = [math.cos(ra)*math.cos(dec),
          math.sin(ra)*math.cos(dec),
          math.sin(dec)]
    s0 = [r0[0]*m[0] + r0[1]*m[1] + r0[2]*m[2],
          r0[0]*m[3] + r0[1]*m[4] + r0[2]*m[5],
          r0[0]*m[6] + r0[1]*m[7] + r0[2]*m[8]]
    r = math.sqrt(s0[0]*s0[0] + s0[1]*s0[1] + s0[2]*s0[2])
    results = [None, None]
    results[1] = math.asin(s0[2]/r)
    cosaa = ((s0[0]/r)/math.cos(results[1]))
    sinaa = ((s0[1]/r)/math.cos(results[1]))
    results[0] = math.atan2(sinaa, cosaa)
    if results[0] < 0:
        results[0] += 2* math.pi
    return results

def bytes2human(n):
    # http://code.activestate.com/recipes/578019
    # >>> bytes2human(10000)
    # '9.8K'
    # >>> bytes2human(100001221)
    # '95.4M'
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i+1)*10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n

# moon_angle: returns moon position at CTIO as (ra, dec) in degrees (either current or at a given date)
# moon_separation: returns angle in degrees between moon and telescope position
def ctio_observer(date=None, lat = None, lon = None, elevation = None):
        observatory = {'TELESCOP':'CTIO 4.0-m telescope',
                       'OBSERVAT':'CTIO',
                       'OBS-LAT':-30.16606,
                       'OBS-LONG':-70.81489,
                       'OBS-ELEV':2215.0}
        if not ephem_available:
            raise RuntimeError('ctio_observer: ephem module not available.')
        ctio = ephem.Observer()
        if lon == None:
            ctio.lon = observatory['OBS-LONG']*ephem.degree
        else:
            ctio.lon = lon
        if lat == None:
            ctio.lat = observatory['OBS-LAT']*ephem.degree
        else:
            ctio.lat = lat
        if elevation == None:
            ctio.elevation = observatory['OBS-ELEV']*ephem.degree
        else:
            ctio.elevation = elevation*ephem.degree
        if date == None:
            ctio.date = datetime.datetime.utcnow()
        else:
            ctio.date = date
        ctio.epoch=ephem.J2000
        ctio.pressure = 0
        return ctio

def kpno_observer(date=None, lat = None, lon = None, elevation = None):
        observatory = {'TELESCOP':'KPNO 4.0-m telescope',
                       'OBSERVAT':'KPNO',
                       'OBS-LAT':31.9640293,
                       'OBS-LONG':-111.5998917,
                       'OBS-ELEV':2123.0}
        if not ephem_available:
            raise RuntimeError('kpno_observer: ephem module not available.')
        kpno = ephem.Observer()
        if lon == None:
            kpno.lon = observatory['OBS-LONG']*ephem.degree
        else:
            kpno.lon = lon
        if lat == None:
            kpno.lat = observatory['OBS-LAT']*ephem.degree
        else:
            kpno.lat = lat
        if elevation == None:
            kpno.elevation = observatory['OBS-ELEV']
        else:
            kpno.elevation = elevation
        if date == None:
            kpno.date = datetime.datetime.utcnow()
        else:
            kpno.date = date
        kpno.epoch=ephem.J2000
        kpno.pressure = 0
        kpno.horizon = '-1:30'
        return kpno

def sky_calendar(date = None, observer = None):
    """
    Input: 
    date = "2013-09-04 15:00:00" (UTC)
    observer = pyephem observer

    Returns the ephemeris for the selected date for the selected observer. 
    sunset, dawn_civil, dawn_nautical, dawn_astronomical
    sunrise, dusk_civil, dusk_nautical, dusk_astronomical
    moonrise, moonset, moonillumination
    the dates/times are returned as datetime objects
    """
    assert ephem_available,'Ephemeris is not available'
    obs_info = OrderedDict()
    if observer is None:
        if date is None:
            observer = kpno_observer()
        else:
            date = '{}-{}-{} 19:00:00'.format(date[0:4],date[4:6],date[6:])
            observer= kpno_observer(date=date)
    # set date to midnight, local time
    #observer.date = (datetime.datetime.now().date()+datetime.timedelta(days=1)).isoformat()
    sun = ephem.Sun()
    sun.compute(observer.date)
    moon = ephem.Moon()
    moon.compute(observer.date)
    obs_info['sunset'] = observer.next_setting(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M") #Sunset
    obs_info['sunrise'] = observer.next_rising(sun).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M") #Sunrise


    try:
        obs_info['moonrise'] = observer.next_rising(moon).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M") #Moonrise
    except:
        obs_info['moonrise'] = None
    try:
        obs_info['moonset'] = observer.next_setting(moon).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M") #Moonset
    except:
        obs_info['moonset'] = None

    # twilights
    for horizon, name in [('-6','civil'),('-10','ten'),('-12','nautical'),('-18','astronomical')]:
        observer.horizon = horizon
        obs_info[f'dusk_{name}'] = observer.next_setting(sun, use_center=True).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
        obs_info[f'dawn_{name}'] =observer.next_rising(sun, use_center = True).datetime().replace(tzinfo=datetime.timezone.utc).astimezone(tz=None).strftime("%Y%m%dT%H:%M")
    # moon phase at midnight
    try:
        obs_info['illumination'] = round(moon.moon_phase, 3)
    except:
        obs_info['illumination'] = None
    return obs_info

def moon_angle(observer = None):
        if not ephem_available:
            raise RuntimeError('moon_angle: ephem module not available.')
        if observer == None:
            observer = kpno_observer()
        moon = ephem.Moon(observer)
        return moon.ra / ephem.degree, moon.dec / ephem.degree

def moon_separation(ra, dec, observer = None):
        if not ephem_available:
            raise RuntimeError('moon_separation: ephem module not available.')
        ra_rad = ra2float(str(ra))*ephem.degree
        dec_rad = dec2float(str(dec))*ephem.degree
        m_ra, m_dec = moon_angle(observer = observer)
        m_ra = m_ra * ephem.degree
        m_dec = m_dec * ephem.degree
        return float(ephem.separation((ra_rad,dec_rad),(m_ra,m_dec))) / ephem.degree

def azel2radec(az, el, observer = None):
        """ Convert azimuth and elevation (altitude) to ra and dec for a given observer """
        if not ephem_available:
            raise RuntimeError('azel2radec: ephem module not available.')
        if observer == None:
            observer = kpno_observer()
        az_rad = dec2float(str(az))*ephem.degree
        el_rad = dec2float(str(el))*ephem.degree
        ra_rad, dec_rad = observer.radec_of(az_rad, el_rad)
        return ra_rad / ephem.degree, dec_rad / ephem.degree

def sidereal_time(observer = None, as_float=False):
        """ returns the local sidereal time for the observer. CTIO is default """
        if observer == None:
            observer = kpno_observer()
        st = ra2float(str(observer.sidereal_time()))
        if as_float:
            return st
        return float2ra(st)

def hadec2radec(ha, dec, observer = None):
        """ Convert hour angle and declination to ra and dec for a given observer """
        if observer == None:
            observer = kpno_observer()
        h = ra2float(str(ha))
        d = dec2float(str(dec))
        st = ra2float(str(observer.sidereal_time()))
        ra =float2ra(st - h)
        dec = d
        return ra, dec

# Haversine formula example in Python
# Author: Wayne Dyck
# origin, destination = (ra,dec) pairs in either sexigesimal or float notation
# the angle is returned in float degrees
def slew_angle(origin, destination):
    lon1 = ra2float(origin[0])
    lat1 = dec2float(origin[1])
    lon2 = ra2float(destination[0])
    lat2 = dec2float(destination[1])

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return c*180.0/math.pi

"""
Some utility functions to deal with the configuration system

"""
def required_roles(product, config):
    """ Get a list of roles using the specified product from the config dictionary
    """

    # First find all roles that use the product. Then check if these roles are started on any node.
    expected_roles = []
    started_roles = []
    for key, value in config.items():
        if key == 'Roles':
            roles = value
            for role_name, role_args in roles.items():
                if 'product' in role_args:
                    if role_args['product'] == product:
                        expected_roles.append(role_name)
                if 'application_name' in role_args:
                    if role_args['application_name'] == product:
                        expected_roles.append(role_name)
        if key == 'Nodes':
            nodes = value
            for node, node_args in nodes.items():
                if 'roles' in node_args:
                    for role in node_args['roles']:
                        started_roles.append(role)

    list_of_roles = list(expected_roles)
    for role in list_of_roles:
        if not role in started_roles:
            try:
                expected_roles.remove(role)
            except ValueError as message:
                pass
    return expected_roles

def get_option(role, option, config):
    for key, value in config.items():
        if key == 'Roles':
            roles = value
            if role not in list(roles.keys()):
                return None
            if option not in list(roles[role].keys()):
                return None
            return roles[role][option]
    return None

#    A function to resolve environment variables embedded in a string.
#    If, for example, in the process environment DOS_INSTANCE is set to 'dos' the desymbolize function
#    will convert a string like 'Images_${DOS_INSTANCE}_test' to 'Images_dos_test'.

def desymbolize(param):
    """ Resolve env. variables in the given text string"""
    text = param.strip()
    ret = ''
    while text.find('${') != -1 and text.find('}') != -1:
        before = text[0:text.find('$')]
        symbol = text[text.find('$'):text.find('}')].strip('${')
        converted = os.getenv(symbol,'')
        ret = ret + before + converted
        text = text[text.find('}')+1:]
    return ret + text

def getopenport():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr, port = s.getsockname()
    s.close()
    return port

def getnetworkinfo(addr = None):
    if addr == None:
        addr = gethostname()
    # convert to IP address
    ip_addr = socket.gethostbyname(addr)
    for iface in interfaces():
        addresses = ifaddresses(iface)
        for inet_type in addresses:
            for _addr in addresses[inet_type]:
                _address = _addr['addr']
                _netmask = _addr.get('netmask','255.255.255.0')
                if _address == ip_addr:
                    return {'addr' : addr, 'ip_addr' : ip_addr, 'netmask' :_netmask,
                            'interface' : iface}
    msg = "Unable to infer net interface on which '%s' is configured" % (addr)
    raise Exception(msg) 

def gethostname():
    import DOSlib.logger as Log
    for i in range(10):
        sysname = socket.getfqdn()   # returns gethostname() if fqdn is not available
        Log.debug('gethostname: sysname %r' % sysname)
        if not 'localhost' in sysname:
            break
        time.sleep(1)
    s=socket.socket()
    # Deal with NUCs
    count = 5

    if 'SPECTCON' in sysname:
        count = 200
    elif 'localhost' in sysname:
        count = 200
    elif 'SHACK' in sysname:
        count = 200
    elif 'FVC' in sysname or 'fvc' in sysname:
        count = 200
    elif 'COMINST' in sysname:
        count = 200
    elif 'SKYCAM' in sysname:
        count = 200
    s.settimeout(0.25)
    if 'local' in sysname or sysname.startswith('127') or ('SPECTCON' in sysname and '.' not in sysname) or ('desi-fvc' in sysname and '.' not in sysname) or ('desi' in sysname and 'desisp' not in sysname and '.' not in sysname) or ('PC' in sysname and '.' not in sysname) or ('petal' in sysname and '.' not in sysname) or ('beagle' in sysname and '.' not in sysname) or ('CCDS' in sysname and '.' not in sysname) or ('desi-petal' in sysname and '.' not in sysname):
        # get the dot IP address using the netifaces module
        for t in range(count):
            interf = sorted(interfaces())
            for iface in interf:
                address = [i['addr'] for i in ifaddresses(iface).setdefault(AF_INET,[{'addr':'No IP addr'}])]
                Log.debug('gethostname: trying (%d) interface %r, address %r' % (t, iface, address))
                for a in address:
                    if a != 'No IP addr' and not str(a).startswith('127') and not str(a).startswith('192') and not str(a).startswith('10.'):
                        return a
            time.sleep(1)
        interf = sorted(interfaces())
        # Now allow private networks
        for iface in interf:
            address = [i['addr'] for i in ifaddresses(iface).setdefault(AF_INET,[{'addr':'No IP addr'}])]
            for a in address:
                if a != 'No IP addr' and not str(a).startswith('127'):
                    return a
    else:
        try:
            s.connect((sysname,22))
            s.close()
            return sysname
        except:
            pass
    # still nothing - try something else
    s=socket.socket()
    try:    # for rpi's
        sysname = socket.inet_ntoa(fcntl.ioctl(s.fileno(),0x8915, struct.pack('256s','eth0'.encode('utf-8')))[20:24])
        s.close()
        return sysname
    except:
        try:
            s.close()
            return socket.gethostbyname(socket.gethostname())
        except:
            # Connect to google
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0.5)
            s.connect(("gmail.com",80))
            address = s.getsockname()[0]
            s.close()
            return address

# ******************************************************************
#
# dict2rec
#   convert a list of dictionaries to a numpy rec array.
#   names is a list with column names. If not given, the dictionary keys are used
#   formats is a list with data formats in dtype format. If not given, the routine
#   attempts to guess the format based on the first record
#
#   returns a numpy rec.array
#
# ******************************************************************

def dict2rec(d, format = None,  names = None, rename = [], add_if_missing = []):
    import numpy
    assert isinstance(d, list) and len(d) != 0, 'Input parameter must be a list with at least one element which has to be a dictionary'
    assert names is None or isinstance(names,(list, tuple)), 'When given, names must be a list or tuple'
    assert format is None or format == 'DOS', 'When given, format must be one of [DOS]'
    assert isinstance(add_if_missing,(list,tuple)), 'add_if_missing must be a list'
    assert isinstance(rename,(list,tuple)), 'rename must be a list'
    
    # Collect the unique fields
    if names is None:
        fields_raw = []
        for item in d:
            fields_raw += item.keys()
        fields = set(fields_raw)
        n = ','.join(fields).split(',')
    else:
        fields = names
        n = names
    rec_list = []
    # Column data formatnames
    if format =='DOS':
        f = []
        for field in fields:
            v = d[0].get(field, None)
            if isinstance(v, (float)): f.append('f8')
            elif isinstance(v, (int)): f.append('i8')
            elif isinstance(v, (bool)): f.append('b')
            else: f.append('|S%d' % len(str(v)))
                
    else:
        f=None
    
    for item in d:
        values = [item.get(field, None) for field in fields]
        rec_list.append(values)

    # anything to add?
    for item in add_if_missing:
        if item[0] not in n:
            n.append(item[0])
            if f is not None:
                v = item[1]
                if isinstance(v, (float)): f.append('f8')
                elif isinstance(v, (int)): f.append('i8')
                elif isinstance(v, (bool)): f.append('b')
                else: f.append('|S%d' % len(str(v)))

    
            for i in range(len(rec_list)):
                rec_list[i].append(item[1])

    # anything to rename:
    for item in rename:
        if item[0] in n:
            i=n.index(item[0])
            n[i] = item[1]
    

    # Construct the numpy.recarray
    if format is None:
        return numpy.core.records.fromrecords([tuple(x) for x in rec_list], names=n) 
    else:
        return numpy.core.records.fromrecords([tuple(x) for x in rec_list], names=n, formats = f) 

# ******************************************************************
#
# rec2dict
#   convert a numpy rec array to a list of dictionaries
#   names is a list with column names (which will become the keys). If not given, all columns are used
#   returns a list of dictionaries
#
# ******************************************************************

def rec2dict(rr, names = None, rename = [], add_if_missing = [], asString = False):
    import copy
    import numpy
    import numpy.lib.recfunctions as rfn

    assert names is None or isinstance(names,(list, tuple)), 'When given, names must be a list or tuple'
    assert isinstance(rename,(list, tuple)) and isinstance(add_if_missing,(list,tuple)), 'rename and add_if_missing parameters must be lists'
    # Check if we have all the names
    rdt = copy.deepcopy(rr.dtype)
    r = numpy.array(rr,dtype=rdt)
    try:
        # anything to rename?
        columns = list(r.dtype.names)
        for item in rename:
            if item[0] in columns:
                i = columns.index(item[0])
                columns[i] = item[1]
        r.dtype.names = columns
        # anything to add?
        for item in add_if_missing:
            if item[0] not in columns:
                nrows = r.shape[0]
                if isinstance(item[1],bool):
                    dt = [('%s' % item[0],bool)]
                    new = numpy.array([(x,) for x in [item[1]]*nrows], dt)
                elif isinstance(item[1],float):
                    dt = [('%s' % item[0],'f8')]
                    new = numpy.array([(x,) for x in [item[1]]*nrows], dt)
                elif isinstance(item[1],int):
                    dt = [('%s' % item[0],'i4')]
                    new = numpy.array([(x,) for x in [item[1]]*nrows], dt)
                elif isinstance(item[1],str):
                    dt = [('%s' % item[0],'U%d' % len(item[1]))]
                    new = numpy.array([(x,) for  x in [item[1]]*nrows], dt)
                else:
                    continue
                r = rfn.merge_arrays((r, new), asrecarray=True, flatten=True)
        if names == None:
            names = r.dtype.names
        else:
            for name in names:
                if name not in r.dtype.names:
                    raise RuntimeError('rec2dict: no column %s in numpy array' % name)
        if asString == True:
            req = list(zip(*[r[x].tolist() if 'S' not in str(r[x].dtype) else numpy.char.strip(r[x].astype(str)).tolist() for x in names]))
        else:
            req = list(zip(*[r[x].tolist() for x in names]))
        result = [dict(zip(names,x)) for x in req]
        return result
    except Exception as e:
        raise RuntimeError('rec2dict: Exception %s' % str(e))

# ******************************************************************
#
# encode/decode a string with a key for some level of obscurity (NOT security)
#
# ******************************************************************
import base64
def dos_encode(clear):
    pml = 'from PML import dos_connection'
    enc = []
    for i in range(len(clear)):
        key_c = pml[i % len(pml)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc).encode()).decode()

def dos_decode(enc):
    dec = []
    pml = 'from PML import dos_connection'    
    enc = base64.urlsafe_b64decode(enc).decode()
    for i in range(len(enc)):
        key_c = pml[i % len(pml)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)

# ********************************************************************************
#
# simple_log : print formatted text messages in case a logger is not available
#            message:   output string
#            level:     log level  (INFO, WARNING etc)
#            function:  Name of the current function or class
# Example output: _SVE [INFO   ] 2017-04-04 12:58:56 Disconnected from SVE LOGGER
# ********************************************************************************
def simple_log(message, level='INFO', function = '(na)', role = None):
    now = datetime.datetime.utcnow().isoformat().replace('T', ' ')   #.strftime("%Y-%m-%d %H:%M:%S")
    if role:
        if function != '(na)':
            rstring = '%s.%s\t[%-7s] %sZ %s' % (role, function, level, now, str(message))
        else:
            rstring = '%s\t[%-7s] %sZ %s' % (role, level, now, str(message))
    else:
        rstring = '%s\t[%-7s] %sZ %s' % (function, level, now, str(message))
    print(rstring)

# ********************************************************************************
#
# raise_error : raise a RuntimeError exception
#            message:   output string
#            level:     log level  (INFO, WARNING etc)
#            function:  Name of the current function or class
# ********************************************************************************
def decode_dos_msg(msg):
    """
    expand DOS exception text for readability
    """
    msg = str(msg).replace('||r:','Role:').replace('||','').replace('|f','').replace('|l:',' ').replace('|t:','@').replace('|m:',' Message:')
    msg = msg.replace(' )',')')
    return msg

try:
    import DOSlib.logger
except:
    pass
def raise_error(message, level='ERROR', function = '', role = None, local_print = True, purge = True):
    # Assemble message in the ||r:|f:|l:|t:|m:|| format
    r = role if role is not None else ''
    f = function
    l = level if level is not None else 'ERROR'
    m = ''
    t= None    

    # simplify message (to avoid exception pile up)
    if purge:
        # extract role, function, level and message from  a previous exception
        pieces = str(message).split('||')
        if len(pieces)>=3:
            # remove stuff that's not part of the message format
            _ = pieces.pop(0)
            _ = pieces.pop()
        # process pieces
        while len(pieces) != 0:
            piece = pieces.pop(0)
            parts = piece.split('|')
            if len(parts) != 5:
                break
            r += '(%s)' % parts[0].replace('r:','').strip()
            f += '(%s)' % parts[1].replace('f:','').strip()
            l = parts[2].replace('l:','').strip()
            t = parts[3].replace('t:','').strip()
            m = parts[4].replace('m:','').strip()

    if m == '':
        m = str(message).strip()
    if t is None:
        t = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    msg = '||r:%s|f:%s|l:%s|t:%s|m:%s||' % (r, f, l, t,m)

    if local_print:
        try:
            getattr(DOSlib.logger,str(level).lower())(decode_dos_msg(msg))
        except:
            simple_log(message, function = function, level = level)
    raise RuntimeError(msg)

# ********************************************************************************
#
# obs_day : returns a string with the observing day (well night - changes at noon)
#           in YYYYMMDD format
#           default is today but a datetime.datetime can be passed for other dates
# ********************************************************************************
def obs_day(dt = None, as_string = True):
    if dt == None:
        day = datetime.datetime.now()
        yesterday = day - datetime.timedelta(days=1)
    else:
        day = dt
    yesterday = day - datetime.timedelta(days=1)
    if day.hour<12:
        if as_string:
            return '%4d%02d%02d' % (yesterday.year, yesterday.month, yesterday.day)
        else:
            return yesterday.date()
    else:
        if as_string:
            return '%4d%02d%02d' % (day.year, day.month, day.day)
        else:
            return day.date()

# ********************************************************************************
#
# FifoList: this class implements a list based fifo with a maximum lenght.
#           new entries kick out the oldest if max_length is exceeded
#           wait() returns True when data becomes available or False if not or 
#           use timeout=0 or len(FifoList) to just check for data availability
# ********************************************************************************
class FifoList:
    def __init__(self, max_length):
        assert isinstance(max_length, int) and max_length>0, 'Invalid queue length'
        self.max_length = max_length
        self.lock = threading.RLock()
        self._data_available = threading.Event()
        self._data_available.clear()
        self.data = {}
        self.nextin = 0
        self.nextout = 0
    def append(self, data):
        with self.lock:
            self.nextin += 1
            self.data[self.nextin] = data
            if self.nextin - self.nextout > self.max_length:
                self.pop()
            self._data_available.set()
    def wait(self, timeout = None):
        self._data_available.wait(timeout = timeout)
        with self.lock:
            if self._data_available.is_set():
                return True
            else:
                return False
    def pop(self):
        with self.lock:
            self.nextout += 1
            result = self.data[self.nextout]
            del self.data[self.nextout]
            if self.nextout == self.nextin:
                self._data_available.clear()
        return result
    def flush(self):
        with self.lock:
            self._data_available.clear()
            del self.data
            self.data = {}
            self.nextin = 0
            self.nextout = 0
    def __len__(self):
        with self.lock:
            return self.nextin - self.nextout

# *********************************************************************************
#
# make_dir - check if a directory exists, create it if not (and try multiple times)
#
# *********************************************************************************
def make_dir(dir):
    assert isinstance(dir, str), 'Directory must be given as string'
    retcode = None
    for i in range(3):
        if not os.access(dir,os.W_OK):
            try:
                os.makedirs(dir)
                return
            except Exception as e:
                retcode = str(e)
                pass    # try again
        else:
            return
        time.sleep(random.random())
    raise_error('Exposures: %r not found or insufficient permissions to write files (Exception: %s)' % (dir, str(retcode)))

# *********************************************************************************
# 
# caller_info is usable only in the context of DOS applications (Pyro daemons)
# and can be used to return ip and port information from the current caller (client)
#
# *********************************************************************************
def caller_info():
    try:
        return Pyro4.current_context.client_sock_addr
    except Exception as e:
        raise_error('caller_info: information not available: %s' % str(e))
    raise_error('make_dir: failed to create directory: %r. Exception: %r' % (dir, str(e)))

# *********************************************************************************
# 
# convert SPectrograph SM to SP with hardwired mapping (needs maintenance!!!)
#
# *********************************************************************************
def sm2sp2sm(sm=None, sp = None):
    mapping = [(4,0),(10,1),(5,2),(6,3),(1,4),(9,5),(7,6),(8,7),(2,8),(3,9)]
    for pair in mapping:
        if sm == pair[0]:
            return pair[1]
        if sp == pair[1]:
            return pair[0]
    raise_error('sm2sp2sm: Invalid indexes provided')

# *********************************************************************************
# 
# overscan correct GFA image
#
# *********************************************************************************
def gfa_detrend(data, biassec_width = 50, datasec_width = 1024, detrend = True):
    # gfa raw data has the overscans in the middle of the pixel array
    if not sigma_clip_available:
        raise_error('gfa_detrend: sigma_clip_stats is not available')
    # left
    biassec_left = data[:, biassec_width + datasec_width: 2 * biassec_width + datasec_width]
    datasec_left = data[:, biassec_width: biassec_width + datasec_width]
    overscan_mean_left, overscan_median_left, overscan_std_left = sigma_clipped_stats(biassec_left, sigma=3.0, maxiters=3, axis=1)
    overscan_median_left.shape = (overscan_median_left.shape[0], 1)
    oc_data_left = datasec_left - overscan_median_left

    # right
    biassec_right = data[:, 2 * biassec_width + datasec_width: 3 * biassec_width + datasec_width]
    datasec_right = data[:, 3 * biassec_width + datasec_width: 3 * biassec_width + 2 * datasec_width]
    overscan_mean_right, overscan_median_right, overscan_std_right = sigma_clipped_stats(biassec_right, sigma=3.0, maxiters=3, axis=1)
    overscan_median_right.shape = (overscan_median_right.shape[0], 1)
    oc_data_right = datasec_right - overscan_median_right

    # stack
    oc_data = np.hstack([oc_data_left, oc_data_right])
    return oc_data

# *********************************************************************************
# 
# convert dataframe to recarray without unicode strings (for fitsio)
#
# *********************************************************************************
def df2rec(dataframe):
    names = dataframe.columns
    arrays = [dataframe[col].to_numpy() for col in names]
    format = formats = [array.dtype if array.dtype != 'O' else 
                        array.astype(str).dtype.str.replace('<U','S') for array in arrays]
    return np.rec.fromarrays(arrays, dtype={'names':names, 'formats':formats})

# *********************************************************************************
# 
# find an exposure directory given expid/reqid (required exposure DB access)
#
# *********************************************************************************
def find_exposure(expid):
    import psycopg2
    assert isinstance(expid, int),'Exposure ID must be an integer'
    # DB connection
    conn = psycopg2.connect(dbname=os.environ['DOS_DB_NAME'], host=os.environ['DOS_DB_HOST'], 
                            port=os.environ['DOS_DB_PORT'], user=os.environ['DOS_DB_READER'], 
                            password=os.environ['DOS_DB_READER_PASSWORD'])
    cur = conn.cursor()

    cur.execute('SELECT night, sequence FROM exposure.exposure where id = %d limit 1' % expid)
    f = cur.fetchall()
    # raises an exception if not found
    p = None
    for record in f:
        try:
            o = record[0]
            s = record[1]
            p = os.path.join('/exposures/desi', str(o), '%08d' % expid)
        except:
            continue
        else:
            break
    cur.close()
    conn.close()
    if not p is None:
        return p
    else:
        raise RuntimeError('Exposure %d not found' % expid)

def find_night(expid):
    import psycopg2
    assert isinstance(expid, int),'Exposure ID must be an integer'
    # DB connection
    conn = psycopg2.connect(dbname=os.environ['DOS_DB_NAME'], host=os.environ['DOS_DB_HOST'], 
                            port=os.environ['DOS_DB_PORT'], user=os.environ['DOS_DB_READER'], 
                            password=os.environ['DOS_DB_READER_PASSWORD'])
    cur = conn.cursor()

    cur.execute('SELECT night, sequence FROM exposure.exposure where id = %d limit 1' % expid)
    f = cur.fetchall()
    # raises an exception if not found
    o = None
    for record in f:
        try:
            o = record[0]
        except:
            continue
        else:
            break
    cur.close()
    conn.close()
    if not o is None:
        return o
    else:
        raise RuntimeError('Exposure %d not found' % expid)
    
# *********************************************************************************
# 
# read a coordinate file and convert into a Pandas DataFrame
#      param can be an exposure number of a filename (full path) or the path to an 
#      exposure directory
#
# *********************************************************************************
def coords2df(param):
    from fitsio import FITS
    if isinstance(param, int):
        p = find_exposure(param)
        filename = os.path.join(p, 'coordinates-%08d.fits' % param)
    elif os.path.isdir(p):
        expid = int(os.path.basename(param))
        filename = os.path.join(p, 'coordinates-%08d.fits' % expid)
    elif os.path.isfile(p):
        filename = p
    f = FITS(filename, 'r')
    coords = pd.DataFrame(f['DATA'].read().byteswap().newbyteorder())
    coords.set_index(['PETAL_LOC', 'DEVICE_LOC'], inplace=True)
    return coords

# *********************************************************************************
# 
# read a fiberassign file coordinate file and convert FIBERASSIGN extension into a Pandas DataFrame
#      param can be an exposure number of a filename (full path) or the path to an 
#      exposure directory
#      Note that fiberassign files have the tileid, not the expid in the file name
# *********************************************************************************
def fiberassign2df(param):
    from fitsio import FITS
    if isinstance(param, int):
        p = find_exposure(param)
        fn = glob.glob(os.path.join(p, "fiberassign*.fits"))[0]
        filename = os.path.join(p, fn)
    elif os.path.isdir(p):
        fn = glob.glob("fiberassign*.fits")[0]
        filename = os.path.join(p, fn)
    elif os.path.isfile(p):
        filename = p
    tileid = os.path.basename(filename).split
    f = FITS(filename, 'r')
    fa = pd.DataFrame(f['FIBERASSIGN'].read().byteswap().newbyteorder())
    return fa

def fiberassign2tileid(param):
    if isinstance(param, int):
        p = find_exposure(param)
        fn = glob.glob(os.path.join(p, "fiberassign*.fits"))[0]
        filename = os.path.join(p, fn)
    elif os.path.isdir(p):
        fn = glob.glob("fiberassign*.fits")[0]
        filename = os.path.join(p, fn)
    elif os.path.isfile(p):
        filename = p
    fn = os.path.basename(filename)
    return int(fn.replace('.fits','').split('-')[-1])

def fiberassign2targets(param):
    fa = fiberassign2df(param)
    def mask(row):
        target_mask = 0
        if row.get('BGS_TARGET', None):
            target_mask |= 1
        if row.get('MWS_TARGET', None):
            target_mask |= 2
        if row.get('CMX_TARGET', None):
            target_mask |= 4
        if row.get('DESI_TARGET', None):
            target_mask |= 32
            if row['DESI_TARGET'] & 0x10000000:
                target_mask |= 64
        if target_mask == 0:
            target_mask = 128
        return target_mask
    fa['TARGETS'] = fa.apply(lambda row: mask(row), axis = 1)
    targets_df = fa[['PETAL_LOC','DEVICE_LOC','TARGETS']].copy()
    return targets_df

# *********************************************************************************
# 
# set disabled bitmask for positioner GUI
# enabled is a bitmask to enable/disable GUI bit
# 
# *********************************************************************************
def disabled_bitmask(flags, enabled = 255):
    # used by focal plane GUI
    def bitmask(f):
        bits = 0
        if not f&1: bits |= 0x80
        if f&0x10000: bits |= 1
        if f&0x20000: bits |= 2
        if f&0x1000000: bits |= 4
        if f&0x4680000: bits |= 8
        if f&0x204000: bits |= 0x10
        if f&0x8000000: bits |= 0x40
        return bits
    if isinstance(flags, int):
        return bitmask(flags)
    return flags.apply(lambda flag: bitmask(int(flag)) & enabled)

def unmatched_positioner_count(flags):
    return len(flags[(flags&5) == 0])

def disabled_positioner_count(flags):
    p = flags[(flags&0xFFFF0000)!=0]
    return len(p[p&4 !=0])

def select_positioners(flags, mask, match = None):
    """
    Example: Find all unmatched positioners that are not disabled
        select_positioners(select_positioners(c['FLAGS_COR_1'],positioner_mask(['DISABLED','POS']),
               match=4), positioner_mask(['MATCHED']),match=0)
    """
    if match == None:
        if isinstance(flags, int):
            return (flags&mask) != 0
        return flags[(flags&mask) != 0]
    else:
        assert isinstance(match, int),'match must be integer when given'
        if isinstance(flags, int):
            return (flags&mask) == match
        return flags[(flags&mask) == match]

def positioner_mask(bits):
    assert isinstance(bits, list),'A list of positioner flags must be provided'
    mask = 0
    for flag in bits:
        if not flag.startswith('POS_'):
            flag = 'POS_' + flag
        mask += POSITIONER_FLAGS.get(flag,0)
    return mask

# *********************************************************************************
# 
# write a sha256 checksum file
#
# *********************************************************************************
def checksum(reqid, directory = None, extensions = None):
    """
    Write sha256 checksum on the exposure directory
    extensions is a list like ['*.fits', '*.fits.fz', '*.done']
    if none, all files in the directory will be used for the checksum
    directory is the directory to be used. If none, obsday and reqid are used to form the standard
    exposure directory.
    """
    import DOSlib.logger as Log
    sha256_file = 'checksum-%08d.sha256sum' % reqid
    if directory is None:
        file_path = os.path.join("/exposures/desi",obs_day(),'%08d' % self.expid)

    else:
        file_path = directory
    # cleanup
    try:
        os.remove(os.path.join(file_path, sha256_file))
    except:
        pass

    if isinstance(extensions, list):
        files_to_include = ' '.join(str(x) for x in extensions)
    else:
        # include all files
        files_to_include = '*'

    cmd = "cd %s;sha256sum %s > %s; cd -" % (file_path, files_to_include, sha256_file)

    try:
        Log.info('checksum: cmd = %s' % cmd)
        os.system(cmd)
        Log.info('checksum: Done')
    except Exception as e:
        raise_error('Exception writing checksum: %s' % str(e))

# *********************************************************************************
# 
# write a dictionary to a form in the electronic logbook (e.g. exposure)
#
# *********************************************************************************
def write_to_logbook(field_dictionary, category = 'Exposures', form = 'exposure', subject = None, url=None, user=None, password=None):
    """Create a form entry in the logbook based on dictionary (mostly used for the exposure form)
   
    Return response from server. Raises an exception if posting failed.

    The keys of the field dictionary have to match the form definition on the eLog side
    url, user and password are set by the Site package

    If subject is None a default subject will be included
    """

    # Connection info
    if not url:
        url = os.getenv('DOS_LOGBOOK_URL')
    if not user:
        user = os.getenv('DOS_LOGBOOK_USER')
    if not password:
        password = os.getenv('DOS_LOGBOOK_PASSWORD')
    assert url and user and password, "Incomplete connection information. Logbook unavailable."

    # Change to the Test Entries for, well, testing.
    if eclapi_available:
        e = ECLAPI.ECLEntry(category, formname = form)
    else:
        return 'Logbook (ELCAPI) not available'

    # Fill out the form
    e.setAuthor('DOS auto logger')
    for key, value in field_dictionary.items():
        e.setValue(key, str(value))
    
    if isinstance(subject, str):
        e.addSubject(subject)
    else:        
        id = field_dictionary.get('id', 'na')
        seq = field_dictionary.get('sequence', 'na')
        type = field_dictionary.get('type', 'na')
        p = field_dictionary.get('program', 'na')
        t = field_dictionary.get('tileid', 'na')
        s = 'Exp: %r, Seq: %r, Type: %r, Tile: %r, Prog: %r' % (id, seq, type, t, p)
        e.addSubject(s)

    # Put it in the database
    elconn = ECLAPI.ECLConnection(url, user, password)
    response = elconn.post(e)
    elconn.close()
    if response[0] != 200:
        raise Exception(response)
    return response

# ******************************************************************
# Decorator to inherit doc strings from super class
# ******************************************************************

class DocInherit(object):
    """
    Docstring inheriting method descriptor

    The class itself is also used as a decorator
    """

    def __init__(self, mthd):
        self.mthd = mthd
        self.name = mthd.__name__

    def __get__(self, obj, cls):
        if obj:
            return self.get_with_inst(obj, cls)
        else:
            return self.get_no_inst(cls)

    def get_with_inst(self, obj, cls):

        overridden = getattr(super(cls, obj), self.name, None)

        @wraps(self.mthd, assigned=('__name__','__module__'))
        def f(*args, **kwargs):
            return self.mthd(obj, *args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def get_no_inst(self, cls):

        for parent in cls.__mro__[1:]:
            overridden = getattr(parent, self.name, None)
            if overridden: break

        @wraps(self.mthd, assigned=('__name__','__module__'))
        def f(*args, **kwargs):
            return self.mthd(*args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def use_parent_doc(self, func, source):
        if source is None:
            raise NameError("Can't find '%s' in parents"%self.name)
        func.__doc__ = self.mthd.__doc__ + '\nParent Class:\n' +source.__doc__
        return func


doc_inherit = DocInherit 
