#!/usr/bin/env python
# -*- coding: utf-8 -*-

## Copyright (C) 2014 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details. 
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.


from vizgrimoire.analysis.timezone import Timezone
from vizgrimoire.SCM import SCM
from vizgrimoire.MLS import MLS
from vizgrimoire.metrics.query_builder import SCMQuery
from vizgrimoire.metrics.query_builder import MLSQuery
from vizgrimoire.metrics.metrics_filter import MetricFilters

dbuser = "root"
dbpassword = ""
dbcvsanaly = "eclipse_source_code"
dbmlstats = "eclipse_mailing_list"
dbidentities = "eclipse_source_code"
period = "month"
startdate = "'2013-10-16'"
enddate = "'2014-10-17'"

scm_dbcon = SCMQuery(dbuser, dbpassword, dbcvsanaly, dbidentities)
mls_dbcon = MLSQuery(dbuser, dbpassword, dbmlstats, dbidentities)

filters = MetricFilters(period, startdate, enddate, [], 10, "", "")

tz = Timezone(scm_dbcon, filters)
dataset = tz.result(SCM)
print dataset

tz = Timezone(mls_dbcon, filters)
dataset = tz.result(MLS)
print dataset

