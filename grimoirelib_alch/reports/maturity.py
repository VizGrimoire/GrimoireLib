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

from datetime import datetime, timedelta

analysis_date = datetime(2014,2,1)

scm_database = {
    "url": "mysql://jgb:XXX@localhost/",
    "schema": "cp_cvsanaly_Eclipse_3328",
    "schema_id": "cp_cvsanaly_Eclipse_3328"
    }
scm_repos_name = ['org.eclipse.ptp.git',] 
scm_branches_name = ['master']

mls_database = {
    "url": "mysql://jgb:XXX@localhost/",
    "schema": "cp_mlstats_Eclipse_3623",
    "schema_id": "cp_cvsanaly_Eclipse_3328"
    }
mls_devels_name = ['ptp-dev.mbox',] 
mls_users_name = ['ptp-dev.mbox',]

# Dictionary to store values for maturity metrics 
values = {}
# Dictionary to store time series for maturity metrics
timeseries = {}

if __name__ == "__main__":

    from grimoirelib_alch.query.scm import DB as SCMDatabase
    from grimoirelib_alch.query.mls import DB as MLSDatabase
    from grimoirelib_alch.family.scm import (
        SCM, NomergesCondition, PeriodCondition, BranchesCondition
        )
    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner
    from grimoirelib_alch.aux.reports import create_report

    from sqlalchemy import func, and_
    from sqlalchemy.sql import label

    stdout_utf8()

    prefix = "maturity-"
    month_end = analysis_date
    month_start = analysis_date - timedelta(days=30)

    #
    # SCM
    #

    database = SCMDatabase (url = scm_database["url"],
                   schema = scm_database["schema"],
                   schema_id = scm_database["schema_id"])
    session = database.build_session()

    # Get SCM repository ids
    query = session.query(
        label("id", SCMDatabase.Repositories.id)
        ) \
        .filter (SCMDatabase.Repositories.name.in_ (scm_repos_name))
    scm_repos = [row.id for row in query.all()]
    # Get SCM branche ids
    query = session.query(
        label("id", SCMDatabase.Branches.id)
        ) \
        .filter (SCMDatabase.Branches.name.in_ (scm_branches_name))
    scm_branches = [row.id for row in query.all()]
    print scm_branches

    #---------------------------------
    print_banner ("SCM_COMMITS_1M: Number of commits during last month")
    # nomerges = NomergesCondition()
    # last_month = PeriodCondition (start = month_start,
    #                               end = month_end,
    #                               date = "author"
    #                               )
    # master = BranchesCondition (branches = ("master",))
    # ncommits = SCM (datasource = session,
    #                 name = "ncommits",
    #                 conditions = (nomerges, last_month, master))
    # values ["scm_commits_1m"] = ncommits.total()
    # ncommits = SCM (datasource = session,
    #             name = "ncommits",
    #             conditions = (nomerges, master))
    # timeseries ["scm_commits_1m"] = ncommits.timeseries()

    query = session.query(
        label(
            "commits",
            func.count (func.distinct(SCMDatabase.SCMLog.rev))
            )
        ) \
        .join (SCMDatabase.Actions) \
        .filter(
            SCMDatabase.Actions.branch_id.in_ (scm_branches),
            SCMDatabase.SCMLog.repository_id.in_ (scm_repos),
            SCMDatabase.SCMLog.author_date > month_start,
            SCMDatabase.SCMLog.author_date <= month_end
            )
    values ["scm_commits_1m"] = query.one().commits

    #---------------------------------
    print_banner ("SCM_COMMITTED_FILES_1M: Number of files during last month")

    query = session.query(
        label(
            "files",
            func.count (func.distinct(SCMDatabase.Actions.file_id))
            )
        ) \
        .join (SCMDatabase.SCMLog) \
        .filter(
            SCMDatabase.Actions.branch_id.in_ (scm_branches),
            SCMDatabase.SCMLog.repository_id.in_ (scm_repos),
            SCMDatabase.SCMLog.author_date > month_start,
            SCMDatabase.SCMLog.author_date <= month_end
            )
    values ["scm_committed_files_1m"] = query.one().files

    #---------------------------------
    print_banner ("SCM_COMMITTERS_1M: Number of committers during last month")
    query = session.query(
        label(
            "authors",
            func.count (func.distinct(SCMDatabase.PeopleUPeople.upeople_id))
            )
        ) \
        .join (
            SCMDatabase.SCMLog,
            SCMDatabase.PeopleUPeople.people_id == SCMDatabase.SCMLog.author_id
            ) \
        .join (SCMDatabase.Actions) \
        .filter(SCMDatabase.Actions.branch_id.in_ (scm_branches),
                SCMDatabase.SCMLog.repository_id.in_ (scm_repos),
                SCMDatabase.SCMLog.author_date > month_start,
                SCMDatabase.SCMLog.author_date <= month_end
                )
    values ["scm_committers_1m"] = query.one().authors


    #
    # MLS
    #

    database = MLSDatabase (url = mls_database["url"],
                   schema = mls_database["schema"],
                   schema_id = mls_database["schema_id"])
    session = database.build_session()

    # Get MLS repository ids (urls)
    query = session.query(
        label("id", MLSDatabase.MailingLists.mailing_list_url)
        ) \
        .filter (MLSDatabase.MailingLists.mailing_list_name.in_ (
                mls_devels_name)
                 )
    mls_devels = [row.id for row in query.all()]
    query = session.query(
        label("id", MLSDatabase.MailingLists.mailing_list_url)
        ) \
        .filter (MLSDatabase.MailingLists.mailing_list_name.in_ (
                mls_users_name)
                 )
    mls_users = [row.id for row in query.all()]

    #---------------------------------
    print_banner ("MLS_DEV_VOL_1M: Number of posts in the developer mailing list during last month")
    query = session.query(
        label(
            "posts",
            func.count (MLSDatabase.Messages.message_ID)
            )
        ) \
        .join (MLSDatabase.MailingLists) \
        .filter (
            MLSDatabase.MailingLists.mailing_list_url.in_ (
                mls_devels
                ),
            MLSDatabase.Messages.arrival_date > month_start,
            MLSDatabase.Messages.arrival_date <= month_end
            )
    values ["mls_dev_vol_1m"] = query.one().posts

    #---------------------------------
    print_banner ("MLS_DEV_AUTH_1M: Number of distinct authors in developer mailing lists during last month")
    query = session.query(
        label(
            "authors",
            func.count (func.distinct(MLSDatabase.PeopleUPeople.upeople_id))
            )
        ) \
        .join (
            MLSDatabase.MessagesPeople,
            MLSDatabase.PeopleUPeople.people_id == \
                MLSDatabase.MessagesPeople.email_address
            ) \
        .join (MLSDatabase.Messages) \
        .join (MLSDatabase.MailingLists) \
        .filter (
            MLSDatabase.MessagesPeople.type_of_recipient == "From",
            MLSDatabase.MailingLists.mailing_list_url.in_ (
                mls_devels
                ),
            MLSDatabase.Messages.arrival_date > month_start,
            MLSDatabase.Messages.arrival_date <= month_end
            )
    values ["mls_dev_auth_1m"] = query.one().authors

    #---------------------------------
    print_banner ("MLS_USR_AUTH_1M: Number of distinct authors in user mailing lists during last month")
    query = session.query(
        label(
            "authors",
            func.count (func.distinct(MLSDatabase.PeopleUPeople.upeople_id))
            )
        ) \
        .join (
            MLSDatabase.MessagesPeople,
            MLSDatabase.PeopleUPeople.people_id == \
                MLSDatabase.MessagesPeople.email_address
            ) \
        .join (MLSDatabase.Messages) \
        .join (MLSDatabase.MailingLists) \
        .filter (
            MLSDatabase.MessagesPeople.type_of_recipient == "From",
            MLSDatabase.MailingLists.mailing_list_url.in_ (
                mls_users
                ),
            MLSDatabase.Messages.arrival_date > month_start,
            MLSDatabase.Messages.arrival_date <= month_end
            )
    values ["mls_usr_auth_1m"] = query.one().authors


    #---------------------------------
    print_banner ("Generate JSON report")
    report = {
        prefix + 'values.json': values,
        prefix + 'timeseries.json': timeseries,
        }

    create_report (report_files = report, destdir = '/tmp/')
