#! /usr/bin/python
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
##
## Reporting of some metrics for a proposed maturity model for projects.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

if __name__ == "__main__":

    from grimoirelib_alch.query.scm import DB as SCMDatabase
    from grimoirelib_alch.family.scm import SCM, NomergesCondition
    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from grimoirelib_alch.aux.reports import create_report

    stdout_utf8()

    prefix = "maturity-"
    database = SCMDatabase (url = "mysql://jgb:XXX@localhost/",
                   schema = "vizgrimoire_cvsanaly",
                   schema_id = "vizgrimoire_cvsanaly")

    #---------------------------------
    print_banner ("Number of commits (timeseries, total)")
    nomerges = NomergesCondition()
    data = SCM (datasource = database,
                name = "ncommits",
                conditions = (nomerges,))
    print data.timeseries()
    print data.total()

    report = {
        prefix + 'timeseries.json': data.timeseries(),
        prefix + 'total.json': data.total()
        }

    create_report (report_files = report, destdir = '/tmp/')
