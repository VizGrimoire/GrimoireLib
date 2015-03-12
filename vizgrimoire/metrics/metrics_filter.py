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
##   Daniel Izquierdo-Cortazar <dizquierdo@bitergia.com>


class MetricFilters(object):

    """ Specific filters for each analysis """

    # Generic supported periods

    """Allows to have per year time series analysis"""
    PERIOD_YEAR = "year"

    """Allows to have per month time series analysis"""
    PERIOD_MONTH = "month"

    """Allows to have per week time series analysis"""
    PERIOD_WEEK = "week"

    """Allows to have per day time series analysis"""
    PERIOD_DAY = "day"


    # Generic type of analysis to any data source

    """Allows per repository type of analysis.
       A repository is a specific data source such as
       a git repository, tracker in Bugzilla or an IRC channel.
       This type of analysis helps to produce metrics for a specific
       repository."""
    REPOSITORY = "repository"

    """Allows per company type of analysis.
       People of the community may have an affiliation. That affiliation
       can be registered. This type of analysis helps to produce 
       metrics for a specific company."""
    COMPANY = "company"

    """Allows per domain type of analysis.
       People of the community are related to an email address. That email address
       has a domain, that can be registered. This type of analysis helps to
       produce metrics for a specific domain."""
    DOMAIN = "domain"

    """Allows per project type of analysis.
       A project consists of a list of repositories. This type of analysis helps
       to aggregate all of the information for a given project."""
    PROJECT = "project"

    """Allows per person type of analysis.
       This type of analysis helps to produce metrics for a specific member of 
       the community."""
    PEOPLE = "people"

    # Specific source code systems type of analysis 

    """Allows per branch type of analysis in source code systems.
       Branches are typically used in software development. This type of analysis
       helps to produce metrics for a specific branch."""
    SCM_BRANCH = "branch"

    """Allows per module type of analysis in source code systems.
       Modules are also directories in the project tree. This type of analysis
       helps to produce metrics for a specific module."""
    SCM_MODULE = "module"

    """Allows per type of file analysis in source code systems.
       The type of a file is determined by the extension of such file and 
       this depends on the results provided by the CVSAnalY tool.
       The supported type of files are: 'unknown', 'devel-doc', 'build',
       'code', 'i18n', 'documentation', 'image', 'ui' and 'package'.
       This type of analysis helps to produce metrics for a specific type
       of file."""
    SCM_FILETYPE = "filetype"

    """Allows log word filtering analysis in source code systems.
       A change in the source code goes always with a log message left by
       a developer. This type of analysis helps to produce metrics filtered
       by a specific log message."""
    SCM_LOGMESSAGE = "logmessage"

    # Specific ticketing type of analysis

    """Allows per ticket type analysis in ticketing systems.
       Issues go through states. This type of analysis helps to produce metrics
       filtered by a specific status of a ticket."""
    ITS_TICKET_TYPE = "ticket_type"

    """ Delimiter to be used in type_analysis for multivalues filters """
    DELIMITER = ",,"

    def __init__(self, period, startdate, enddate, type_analysis=None, npeople=10,
                 people_out = None, organizations_out = None, global_filter = None):
        self.period = period
        self.startdate = startdate
        self.enddate = enddate
        self.type_analysis = type_analysis
        self.npeople = npeople
        self.people_out = people_out
        self.organizations_out = organizations_out
        self.global_filter = global_filter
        self.closed_condition = None

    def add_filter(self, typeof_analysis, value):
        """This function adds a new type of analysis to the type_analysis
           variable"""

        # TODO: fix type of analysis hack. We need to remove the list of 
        # strings and go for a smarter way of doing this.
        # TODO: need to check all constants to check that user is
        # selecting a supported type of analysis.
        
        if not isinstance(value, str):
            raise TypeError("String expected")

        if value[0] <> "'" and value[len(value) - 1] <> "'":
            value = "'" + value + "'"

        if self.type_analysis is None or self.type_analysis == []:
            self.type_analysis = [typeof_analysis, value]
        else:
            self.type_analysis[0] = self.type_analysis[0] + MetricFilters.DELIMITER + typeof_analysis
            self.type_analysis[1] = self.type_analysis[1] + MetricFilters.DELIMITER + value


    def add_period(self, typeof_period):
        """This function set the period type of analysis."""

        if (typeof_period <> MetricFilters.PERIOD_YEAR and
           typeof_period <> MetricFilters.PERIOD_MONTH and
           typeof_period <> MetricFilters.PERIOD_WEEK and
           typeof_period <> MetricFilters.PERIOD_DAY):
            raise NameError("Period not supported")

        self.period = typeof_period


    def set_global_filter(self, value): self.global_filter = value

    def set_closed_condition(self, value): self.closed_condition = value

    def copy(self):
        newcopy = MetricFilters(self.period,
                                self.startdate,
                                self.enddate,
                                self.type_analysis,
                                self.npeople,
                                self.people_out,
                                self.companies_out,
                                self.global_filter)
        return newcopy

