## Copyright (C) 2012, 2013 Bitergia
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
##
## Queries for SCM data analysis
##
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>

from vizgrimoire.data_source import DataSource
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.GrimoireUtils import completePeriodIds

def GetCommitsSummaryCompanies (period, startdate, enddate, identities_db, num_organizations):
    # This function returns the following dataframe structrure
    # unixtime, date, week/month/..., company1, company2, ... company[num_organizations -1], others
    # The 3 first fields are used for data and ordering purposes
    # The "companyX" fields are those that provide info about that company
    # The "Others" field is the aggregated value of the rest of the organizations
    # Companies above num_organizations will be aggregated in Others

    from vizgrimoire.SCM import SCM

    metric = DataSource.get_metrics("organizations", SCM)
    organizations = metric.get_list()
    organizations = organizations['name']

    first_organizations = {}
    count = 1
    for company in organizations:
        company_name = "'"+company+"'"
        type_analysis = ['company', company_name]
        mcommits = DataSource.get_metrics("commits", SCM)
        mfilter = MetricFilters(period, startdate, enddate, type_analysis)
        mfilter_orig = mcommits.filters
        mcommits.filters = mfilter
        commits = mcommits.get_ts()
        mcommits.filters = mfilter_orig
        # commits = EvolCommits(period, startdate, enddate, identities_db, ["company", company_name])
        # commits = completePeriodIds(commits, period, startdate, enddate)
        # Rename field commits to company name
        commits[company] = commits["commits"]
        del commits['commits']

        if (count <= num_organizations):
            #Case of organizations with entity in the dataset
            first_organizations = dict(first_organizations.items() + commits.items())
        else :
            #Case of organizations that are aggregated in the field Others
            if 'Others' not in first_organizations:
                first_organizations['Others'] = commits[company]
            else:
                first_organizations['Others'] = [a+b for a, b in zip(first_organizations['Others'],commits[company])]
        count = count + 1

    #TODO: remove global variables...
    first_organizations = completePeriodIds(first_organizations, period, startdate, enddate)
    return(first_organizations)


def GetClosedSummaryCompanies (period, startdate, enddate, identities_db, closed_condition, num_organizations):

    from vizgrimoire.ITS import ITS

    count = 1
    first_organizations = {}

    metric = DataSource.get_metrics("organizations", ITS)
    organizations = metric.get_list()
    organizations = organizations['name']

    for company in organizations:
        type_analysis = ["company", "'"+company+"'"]
        filter_com = MetricFilters(period, startdate, enddate, type_analysis)
        mclosed = ITS.get_metrics("closed", ITS)
        mclosed.filters = filter_com
        closed = mclosed.get_ts()
        # Rename field closed to company name
        closed[company] = closed["closed"]
        del closed['closed']

        if (count <= num_organizations):
            #Case of organizations with entity in the dataset
            first_organizations = dict(first_organizations.items() + closed.items())
        else :
            #Case of organizations that are aggregated in the field Others
            if 'Others' not in first_organizations:
                first_organizations['Others'] = closed[company]
            else:
                first_organizations['Others'] = [a+b for a, b in zip(first_organizations['Others'],closed[company])]
        count = count + 1
    first_organizations = completePeriodIds(first_organizations, period, startdate, enddate)

    return(first_organizations)


