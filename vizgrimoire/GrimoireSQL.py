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
# Authors:
#     Alvaro del Castillo <acs@bitergia.com>

# SQL utilities

import MySQLdb
import logging
import re, sys
from vizgrimoire.metrics.query_builder import DSQuery


# global vars to be moved to specific classes
cursor = None
# one connection per database
dbpool = {}

##
## METAQUERIES
##

# TODO: regexpr not adapted yet from R to Python


def GetSQLGlobal(date, fields, tables, filters, start, end,
                 type_analysis = None):
    all_items = DSQuery.get_all_items(type_analysis)
    return DSQuery.GetSQLGlobal(date, fields, tables, filters, start, end, all_items)
    return(sql)

def GetSQLPeriod(period, date, fields, tables, filters, start, end,
                 type_analysis = None):
    all_items = DSQuery.get_all_items(type_analysis)
    return DSQuery.GetSQLPeriod(period, date, fields, tables, filters, start, end, all_items)

############
#Generic functions to check evolutionary or static info and for the execution of the final query
###########

def BuildQuery (period, startdate, enddate, date_field, fields, tables, filters, evolutionary):
    # Select the way to evolutionary or aggregated dataset
    q = ""

    if (evolutionary):
        q = GetSQLPeriod(period, date_field, fields, tables, filters,
                          startdate, enddate)
    else:
        q = GetSQLGlobal(date_field, fields, tables, filters,
                          startdate, enddate)

    return(q)

def SetDBChannel (user=None, password=None, database=None,
                  host="127.0.0.1", port=3306, group=None):
    global cursor
    global dbpool

    db = None

    if database in dbpool:
        db = dbpool[database]
    else:
        if (group == None):
            db = MySQLdb.connect(user=user, passwd=password,
                                 db=database, host=host, port=port)
        else:
            db = MySQLdb.connect(read_default_group=group, db=database)
        dbpool[database] = db

    cursor = db.cursor()
    cursor.execute("SET NAMES 'utf8'")

def ExecuteQuery (sql):
    result = {}
    cursor.execute(sql)
    rows = cursor.rowcount
    columns = cursor.description

    if columns is None: return result

    for column in columns:
        result[column[0]] = []
    if rows > 1:
        for value in cursor.fetchall():
            for (index,column) in enumerate(value):
                result[columns[index][0]].append(column)
    elif rows == 1:
        value = cursor.fetchone()
        for i in range (0, len(columns)):
            result[columns[i][0]] = value[i]
    return result 
