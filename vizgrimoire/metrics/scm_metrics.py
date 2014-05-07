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
##
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

""" Source Code Management core metrics """

import logging

from GrimoireUtils import getPeriod
from metric import Metric
from report import Report
from SCM import SCM

class Commits(Metric):

    def __init__(self):
        self.id = "commits"
        self.name = "Commits"
        self.desc = "Number of commits (changes to source code), aggregating all branches"
        self.data_source = SCM
        self.data_source.add_metric(self)
        # extra
        self.envision = {
            "y_labels" : "true",
            "show_markers" : "true"
        }
        self.startdate = "'"+Report.get_config()['r']['start_date']+"'"
        self.enddate = "'"+Report.get_config()['r']['end_date']+"'"
        self.i_db = Report.get_config()['generic']['db_identities']
        if 'period' not in Report.get_config()['r']:
            self.period = getPeriod("months")
        else:
            self.period = getPeriod(Report.get_config()['r']['period'])


    def get_aggregate(self, filter_ = None):
        from SCM import StaticNumCommits
        Report.connect_ds(self.data_source)
        return StaticNumCommits(self.period, self.startdate, self.enddate, self.i_db, None)

    def get_evolutionary(self, filter_ = None):
        from SCM import GetSCMEvolutionaryData
        Report.connect_ds(self.data_source)
        return GetSCMEvolutionaryData(self.period, self.startdate, self.enddate, self.i_db, None)


class Committers(Metric):

    def __init__(self):
        self.id = "committers"
        self.name = "Committers"
        self.desc = "Number of persons committing (merging changes to source code)"
        self.data_source = SCM
        self.data_source.add_metric(self)
        # extra
        self.action =  "commits"
        self.envision = {"gtype" : "whiskers"}

class Authors(Metric):

    def __init__(self):
        self.id = "authors"
        self.name = "Authors"
        self.desc = "Number of persons authoring commits (changes to source code)"
        self.data_source = SCM
        self.data_source.add_metric(self)
        # extra
        self.action = "commits"
        self.envision = {"gtype" : "whiskers"}

class Branches(Metric):

    def __init__(self):
        self.id = "branches"
        self.name = "Branches"
        self.desc = "Number of active branches"
        self.data_source = SCM
        self.data_source.add_metric(self)

class Files(Metric):

    def __init__(self):
        self.id = "files"
        self.name = "Files"
        self.desc = "Number of files modified by at least one commit"
        self.data_source = SCM
        self.data_source.add_metric(self)

class AddedLines(Metric):

    def __init__(self):
        self.id = "added_lines"
        self.name = "Added lines"
        self.desc = "Addition of lines added in all commits"
        self.data_source = SCM
        self.data_source.add_metric(self)

class RemovedLines(Metric):

    def __init__(self):
        self.id = "removed_lines"
        self.name = "Removed Lines"
        self.desc = "Addition of lines removed in all commits"
        self.data_source = SCM
        self.data_source.add_metric(self)

class Repositories(Metric):

    def __init__(self):
        self.id = "repositories"
        self.name = "Repositories"
        self.desc = "Evolution of the number of source code repositories"
        self.data_source = SCM
        self.data_source.add_metric(self)
        self.envision = {"gtype" : "whiskers"}

class Companies(Metric):

    def __init__(self):
        self.id = "companies"
        self.name = "Affiliations"
        self.desc = "Number of affiliations (companies, etc.) with persons active in changing code"
        self.data_source = SCM
        self.data_source.add_metric(self)

class Countries(Metric):

    def __init__(self):
        self.id = "countries"
        self.name = "Countries"
        self.desc = "Number of countries with persons active in changing code"
        self.data_source = SCM
        self.data_source.add_metric(self)

class Domains(Metric):

    def __init__(self):
        self.id = "domains"
        self.name = "Domains"
        self.desc = "Number of distinct domains with persons active in changing code"
        self.data_source = SCM
        self.data_source.add_metric(self)

class People(Metric):

    def __init__(self):
        self.id = "people"
        self.name = "People"
        self.desc = "Number of people active in changing code (authors, committers)"
        self.data_source = SCM
        self.data_source.add_metric(self)