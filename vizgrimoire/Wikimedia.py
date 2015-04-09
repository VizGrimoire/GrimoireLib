## Copyright (C) 2013 Bitergia
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
##
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## People.R
##
## Queries for source code review data analysis
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

from GrimoireSQL import ExecuteQuery

# _filter_submitter_id as a static global var to avoid SQL re-execute
def _init_filter_submitter_id():
    people_userid = 'l10n-bot'
    q = "SELECT id FROM people WHERE user_id = '%s'" % (people_userid)
    globals()['_filter_submitter_id'] = ExecuteQuery(q)['id']

# To be used for issues table
def GetIssuesFiltered():
    if ('_filter_submitter_id' not in globals()): _init_filter_submitter_id()
    filters = " submitted_by <> %s" % (globals()['_filter_submitter_id'])
    return filters

# To be used for changes table
def GetChangesFiltered():
    if ('_filter_submitter_id' not in globals()): _init_filter_submitter_id()
    filters = " changed_by <> %s" % (globals()['_filter_submitter_id'])
    return filters
