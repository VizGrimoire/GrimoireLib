#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# This file is a part of the vizGrimoire.R package
#
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>

# Misc utils to be distributed in specific modules

import calendar
from ConfigParser import SafeConfigParser
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser
import logging
import json
import math
from optparse import OptionParser
import rpy2.rinterface as rinterface
from rpy2.robjects.vectors import StrVector
import os,sys
from numpy import average, median

def read_options():
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 0.1")
    parser.add_option("-d", "--database",
                      action="store",
                      dest="dbname",
                      help="Database where information is stored")
    parser.add_option("-u","--dbuser",
                      action="store",
                      dest="dbuser",
                      default="root",
                      help="Database user")
    parser.add_option("-p","--dbpassword",
                      action="store",
                      dest="dbpassword",
                      default="",
                      help="Database password")
    parser.add_option("-g", "--granularity",
                      action="store",
                      dest="granularity",
                      default="months",
                      help="year,months,weeks granularity")
    parser.add_option("-o", "--destination",
                      action="store",
                      dest="destdir",
                      default="data/json",
                      help="Destination directory for JSON files")
    parser.add_option("-r", "--reports",
                      action="store",
                      dest="reports",
                      default="",
                      help="Reports to be generated (repositories, companies, countries, people)")
    parser.add_option("-s", "--start",
                      action="store",
                      dest="startdate",
                      default="1900-01-01",
                      help="Start date for the report")
    parser.add_option("-e", "--end",
                      action="store",
                      dest="enddate",
                      default="2100-01-01",
                      help="End date for the report")
    parser.add_option("-i", "--identities",
                      action="store",
                      dest="identities_db",
                      help="Database with unique identities and affiliations")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="backend",
                      default="bugzilla",
                      help="Type of backend: bugzilla, allura, jira, github")
    parser.add_option("--npeople",
                      action="store",
                      dest="npeople",
                      default="10",
                      help="Limit for people analysis")
    parser.add_option("-c", "--config-file",
                      action="store",
                      dest="config_file",
                      help="Automator config file path")
    parser.add_option("--data-source",
                      action="store",
                      dest="data_source",
                      help="data source to be generated")
    parser.add_option("--filter",
                      action="store",
                      dest="filter",
                      help="filter to be generated")


    (opts, args) = parser.parse_args()

    if len(args) != 0:
        parser.error("Wrong number of arguments")

    if opts.config_file is None :
        if not(opts.dbname and opts.dbuser and opts.identities_db):
            parser.error("--database --db-user and --identities are needed")
    return opts

def valRtoPython(val):
    if val is rinterface.NA_Character: val = None
    # Check for .0 and convert to int
    elif isinstance(val, float):
        if (val % 1 == 0): val = int(val)
    elif (type(val) == StrVector):
        val2 = []
        for item in val: val2.append(item)
        val = val2
    return val


def createTimeSeries(ts_data):
    new_ts_data = {}

    # TODO: old format from R JSON. To be simplified
    new_ts_data['unixtime'] = []
    new_ts_data['date'] = []
    new_ts_data['id'] = []
    data_vars = ts_data.keys()
    for key in (data_vars): new_ts_data[key] = []

    return new_ts_data

# Check that all list entries are arrays
def checkListArray(data):
    data_vars = data.keys()
    for key in (data_vars):
        if not isinstance(data[key], (list)):
            data[key] = [data[key]]

# NaN converted to 0
def cleanNaN(ts_data):
    data_vars = ts_data.keys()
    for key in (data_vars):
        for i in range(0,len(ts_data[key])):
            val = ts_data[key][i]
            if (isinstance(val, float) and math.isnan(val)): ts_data[key][i] = 0
    return ts_data


def completePeriodIdsYears(ts_data, start, end):
    data_vars = ts_data.keys()
    new_ts_data =  createTimeSeries(ts_data)
    checkListArray(ts_data)

    start_year = start.year * 12
    years = end.year - start.year


    for i in range(0, years+1):
        if (start_year+(i*12) in ts_data['year']) is False:
            # Add new time point with all vars to zero
            for key in (data_vars):
                new_ts_data[key].append(0)
            new_ts_data['year'].pop()
            new_ts_data['year'].append(start_year+(i*12))
        else:
            # Add already existing data for the time point
            index = ts_data['year'].index(start_year+(i*12))
            for key in (data_vars):
                new_ts_data[key].append(ts_data[key][index])

        current =  start + relativedelta(years=i)
        timestamp = calendar.timegm(current.timetuple())
        new_ts_data['unixtime'].append(unicode(timestamp))
        new_ts_data['id'].append(i)
        new_ts_data['date'].append(datetime.strftime(current, "%b %Y"))

    return new_ts_data


def completePeriodIdsMonths(ts_data, start, end):
    data_vars = ts_data.keys()
    new_ts_data =  createTimeSeries(ts_data)
    checkListArray(ts_data)

    start_month = start.year*12 + start.month
    end_month = end.year*12 + end.month
    months = end_month - start_month

    # All data is from the complete month
    start = start - timedelta(days=(start.day-1))

    for i in range(0, months+1):
        if (start_month+i in ts_data['month']) is False:
            # Add new time point with all vars to zero
            for key in (data_vars):
                new_ts_data[key].append(0)
            new_ts_data['month'].pop()
            new_ts_data['month'].append(start_month+i)
        else:
            # Add already existing data for the time point
            index = ts_data['month'].index(start_month+i)
            for key in (data_vars):
                new_ts_data[key].append(ts_data[key][index])

        current =  start + relativedelta(months=i)
        timestamp = calendar.timegm(current.timetuple())
        new_ts_data['unixtime'].append(unicode(timestamp))
        new_ts_data['id'].append(i)
        new_ts_data['date'].append(datetime.strftime(current, "%b %Y"))

    return new_ts_data

def date2Week(date):
    # isocalendar: year weeknumber weekday
    week   = str(date.isocalendar()[0])
    week  += "%02d" % date.isocalendar()[1]
    return week

def completePeriodIdsWeeks(ts_data, start, end):
    data_vars = ts_data.keys()
    new_ts_data =  createTimeSeries(ts_data)
    checkListArray(ts_data)

    # Start of the week
    dayweek = start.isocalendar()[2]
    new_week = start - relativedelta(days=dayweek-1)
    i = 1 # for ids in time series

    while (new_week <= end):
        new_week_txt = date2Week(new_week)
        if (int(new_week_txt) in ts_data['week']) is False:
            # Add new time point with all vars to zero
            for key in (data_vars):
                new_ts_data[key].append(0)
            new_ts_data['week'].pop()
            new_ts_data['week'].append(new_week_txt)
        else:
            # Add already existing data for the time point
            index = ts_data['week'].index(int(new_week_txt))
            for key in (data_vars):
                new_ts_data[key].append(ts_data[key][index])
            new_ts_data['week'].pop()
            new_ts_data['week'].append(str(ts_data['week'][index]))

        timestamp = calendar.timegm(new_week.timetuple())
        new_ts_data['unixtime'].append(unicode(timestamp))
        new_ts_data['id'].append(i)
        i += 1
        new_ts_data['date'].append(datetime.strftime(new_week, "%b %Y"))
        new_week = new_week + relativedelta(weeks=1)

    return new_ts_data

def completePeriodIds(ts_data, period, startdate, enddate):
    # If already complete, return
    if "id" in ts_data: return ts_data

    if len(ts_data.keys()) == 0: return ts_data
    new_ts_data = ts_data
    startdate = startdate.replace("'", "")
    enddate = enddate.replace("'", "")
    start = datetime.strptime(startdate, "%Y-%m-%d")
    end = datetime.strptime(enddate, "%Y-%m-%d")

    if period == "week":
        new_ts_data = completePeriodIdsWeeks(ts_data, start, end)
    elif period == "month":
        new_ts_data = completePeriodIdsMonths(ts_data, start, end)
    elif period == "year":
        new_ts_data = completePeriodIdsYears(ts_data, start, end)

    return cleanNaN(new_ts_data)

# Convert a R data frame to a python dictionary
def dataFrame2Dict(data):
    dict_ = {}

    # R start from 1 in data frames
    for i in range(1,len(data)+1):
        # Get the columns data frame
        col = data.rx(i)
        colname = col.names[0]
        colvalues = col[0]
        dict_[colname] = [];

        if (len(colvalues) == 1):
            dict_[colname] = valRtoPython(colvalues[0])
        else:
            for j in colvalues: 
                dict_[colname].append(valRtoPython(j))
    return dict_

def getPeriod(granularity, number = None):
    period = None
    nperiod = None
    if (granularity == 'years'):
        period = 'year'
        nperiod = 365
    elif (granularity == 'months'): 
        period = 'month'
        nperiod = 31
    elif (granularity == 'weeks'): 
        period = 'week'
        nperiod = 7
    elif (granularity == 'days'): 
        period = 'day'
        nperiod = 1
    else: 
        logging.error("Incorrect period:",granularity)
        sys.exit(1)
    if (number): return nperiod
    return period


def removeDecimals(data):
    from decimal import Decimal
    if (isinstance(data, dict)):
        for key in data:
            if (isinstance(data[key], Decimal)):
                data[key] = float(data[key])
            elif (isinstance(data[key], list)):
                data[key] = convertDecimals(data[key])
            elif (isinstance(data[key], dict)):
                data[key] = convertDecimals(data[key])
    if (isinstance(data, list)):
        for i in range(0,len(data)):
            if (isinstance(data[i], Decimal)):
                data[i] = float(data[i])
    return data

def convertDatetime(data):
    if (isinstance(data, dict)):
        for key in data:
            if (isinstance(data[key], datetime)):
                data[key] = str(data[key])
            elif (isinstance(data[key], list)):
                data[key] = convertDatetime(data[key])
            elif (isinstance(data[key], dict)):
                data[key] = convertDatetime(data[key])
    if (isinstance(data, list)):
        for i in range(0,len(data)):
            if (isinstance(data[i], datetime)):
                data[i] = str(data[i])
    return data

# Until we use VizPy we will create JSON python files with _py
def createJSON(data, filepath, check=False, skip_fields = []):
    check = False # for production mode
    filepath_tokens = filepath.split(".json")
    filepath_py = filepath_tokens[0]+"_py.json"
    filepath_r = filepath_tokens[0]+"_r.json"
    json_data = json.dumps(removeDecimals(data), sort_keys=True)
    json_data = json_data.replace('NaN','"NA"')
    if check == False: #forget about R JSON checking
        jsonfile = open(filepath, 'w')
        jsonfile.write(json_data)
        jsonfile.close()
        return

    # NA as value is not decoded with Python JSON
    # JSON R has "NA" and not NaN
    # JSON R has "NA" and not null
    json_data = json_data.replace('NA','"NA"').replace('NaN','"NA"').replace('null',' "NA" ')
    jsonfile = open(filepath_py, 'w')
    jsonfile.write(json_data)
    jsonfile.close()

    if (check == True and compareJSON(filepath, filepath_py, skip_fields) is False):
        logging.error("Wrong data generated from Python "+ filepath_py)
        sys.exit(1)
    else:
        # Rename the files so Python JSON is used
        os.rename(filepath, filepath_r)
        os.rename(filepath_py, filepath)

def compareJSON(orig_file, new_file, skip_fields = []):
    f1 = open(orig_file)
    f2 = open(new_file)
    data1 = json.load(f1)
    data2 = json.load(f2)
    f1.close()
    f2.close()

    return compare_json_data(data1, data2, orig_file, new_file, skip_fields)

def compare_json_data(data1, data2, orig_file = "", new_file = "", skip_fields = []):
    """ Compare two JS objects read from a JSON file or created from code """

    if (orig_file == ""): orig_file = "orig data"
    if (new_file == ""): orig_file = "new data"

    check = True
    if len(data1) > len(data2):
        logging.warn("More data in " + orig_file + " than in "+
                     new_file +": " + str(len(data1)) + " " + str(len (data2)))
        check = False

    elif isinstance(data1, list):
        for i in range(0, len(data1)):
            if isinstance(data1[i], float): data1[i] = round(data1[i],6)
            if isinstance(data2[i], float): data2[i] = round(data2[i],6)
            if (data1[i] != data2[i]):
                if (data1[i] == "NA" and data2[i] == 0): continue
                logging.warn(data1[i])
                logging.warn("is not")
                logging.warn(data2[i])
                check = False
                break

    elif isinstance(data1, dict):
        for name in data1:
            if data2.has_key(name) is False:
                logging.warn (name + " does not exists in " + new_file)
                check = False
            if isinstance(data1[name], float): data1[name] = round(data1[name],6)
            if isinstance(data2[name], float): data2[name] = round(data2[name],6)
            if isinstance(data1[name], list) and isinstance(data2[name], list): 
                for i in range(0, len(data1[name])): 
                    if isinstance(data1[name][i], float): data1[name][i] = round(data1[name][i],6)
                    if isinstance(data2[name][i], float): data2[name][i] = round(data2[name][i],6)
            elif data1[name] != data2[name]:
                if (data1[name] == "NA" and data2[name] == 0): continue
                elif name in skip_fields:
                    logging.warn ("'"+name + "' (skipped) different in dicts\n" + str(data1[name]) + "\n" + str(data2[name])) 
                    continue
                logging.warn ("'"+name + "' different in dicts\n" + str(data1[name]) + "\n" + str(data2[name]))
                check = False
        for name in data2:
            if data1.has_key(name) is False:
                logging.warn (name + " does not exists in " + orig_file)

    return check

def GetDates (last_date, days):
    enddate = last_date.replace("'","")

    enddate = parser.parse(enddate)
    startdate = enddate - timedelta(days=days)
    prevdate = startdate - timedelta(days=days)

    chardates = ["'"+enddate.strftime('%Y-%m-%d')+"'"]
    chardates.append("'"+startdate.strftime('%Y-%m-%d')+"'")
    chardates.append("'"+prevdate.strftime('%Y-%m-%d')+"'")

    return (chardates)

def GetPercentageDiff (value1, value2):
    # This function returns the % diff between value 1 and value 2.
    # The difference could be positive or negative, but the returned value
    # is always > 0

    percentage = 0

    if (value1 == 0 or value1 is None  or value2 is None): return (0)
    value1 = float(value1)
    value2 = float(value2)

    if (value1 < value2):
        diff = float(value2 - value1)
        percentage = int((diff/abs(value1)) * 100)
    if (value1 > value2):
        percentage = int((1-(value2/value1)) * 100)

    return(percentage)

def checkFloatArray(data):
    if not isinstance(data, (list)):
        data = [data]
    for i in range(0,len(data)):
        val = data[i]
        data[i] = float(val)
        if (val == 0): data[i] = 0
    return data

def read_main_conf(config_file):
    options = {}
    parser = SafeConfigParser()
    fd = open(config_file, 'r')
    parser.readfp(fd)
    fd.close()

    sec = parser.sections()
    # we'll read "generic" for db information and "r" for start_date and "bicho" for backend
    for s in sec:
        if not((s == "generic") or (s == "r") or (s == "bicho")):
            continue
        options[s] = {}
        opti = parser.options(s)
        for o in opti:
            options[s][o] = parser.get(s, o)
    return options

def get_subprojects(project, identities_db):
    """ Return all subprojects ids for a project in a string join by comma """

    from GrimoireSQL import ExecuteQuery

    q = "SELECT project_id from %s.projects WHERE id='%s'" % (identities_db, project)
    project_id = ExecuteQuery(q)['project_id']

    q = """
        SELECT subproject_id from %s.project_children pc where pc.project_id = '%s'
    """ % (identities_db, project_id)

    subprojects = ExecuteQuery(q)

    if not isinstance(subprojects['subproject_id'], list):
        subprojects['subproject_id'] = [subprojects['subproject_id']]

    project_with_children = subprojects['subproject_id'] + [project_id]
    project_with_children_str = ','.join(str(x) for x in project_with_children)

    return  project_with_children_str

def completeTops(tops, details, backend_name='bugzilla'):
    completed_tops = dict(tops.items())
    completed_tops['summary'] = []
    completed_tops['url'] = []

    for issue_id in tops['issue_id']:
        info = details[issue_id]
        completed_tops['summary'].append(info[0])
        completed_tops['url'].append(buildIssueURL(info[1], issue_id, backend_name))
    return completed_tops

def buildIssueURL(tracker_url, issue_id, backend='bugzilla'):
    if backend is not 'bugzilla':
        return ""
    offset = tracker_url.find('/buglist.cgi')
    url = tracker_url[:offset] + '/show_bug.cgi?id=' + issue_id
    return url


def get_median(period_values):
    # No data for this period
    if not period_values:
        return float('nan')
    if not isinstance(period_values, list):
        return period_values
    return median(convertDecimals(period_values))