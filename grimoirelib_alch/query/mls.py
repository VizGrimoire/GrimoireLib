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
## Package to deal with queries for MLS data from *Grimoire
##  (MLStats databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.type.timeseries import TimeSeries
from grimoirelib_alch.type.activity import ActivityList
from grimoirelib_alch.query.common import GrimoireDatabase, GrimoireQuery

from sqlalchemy import func, Column, String, Integer, ForeignKey, and_
from sqlalchemy.sql import label
from datetime import datetime


class DB (GrimoireDatabase):
    """Class for dealing with MLS (MLStats) databases.

    """

    def _query_cls(self):
        """Return que defauld Query class for this database

        Returns
        -------

        GrimoireQuery: default Query class.

        """

        return Query

    def _create_tables(self, tables = None, tables_id = None):
        """Create all SQLAlchemy tables.

        Builds a SQLAlchemy class per SQL table, by using _table().
        It assumes self.Base, self.schema and self.schema_id are already
        set (see super.__init__() code).

        """

        DB.Messages = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Messages',
            tablename = 'messages',
            schemaname = self.schema,
            columns = dict (
                mailing_list_url = Column(
                    String,
                    ForeignKey(self.schema + '.' + \
                                   'mailing_lists.mailing_list_url'))
                ))

        DB.MessagesPeople = GrimoireDatabase._table (
            bases = (self.Base,), name = 'MessagesPeople',
            tablename = 'messages_people',
            schemaname = self.schema,
            columns = dict (
                mailing_list_url = Column(
                    String,
                    ForeignKey(self.schema + '.' + \
                                   'mailing_lists.mailing_list_url')),
                message_id = Column(
                    String,
                    ForeignKey(self.schema + '.' + \
                                   'messages.message_ID')),
                email_address = Column(
                    String,
                    ForeignKey(self.schema + '.' + \
                                   'people.email_address'))     
                ))

        DB.People = GrimoireDatabase._table (
            bases = (self.Base,), name = 'People',
            tablename = 'people',
            schemaname = self.schema)

        DB.PeopleUIdentities = GrimoireDatabase._table (
            bases = (self.Base,), name = 'PeopleUIdentities',
            tablename = 'people_uidentities',
            schemaname = self.schema,
            columns = dict (
                uuid = Column(
                    Integer,
                    ForeignKey(self.schema_id + '.' + 'uidentities.uuid'),
                    primary_key = True
                    ),
                people_id = Column(
                    Integer,
                    ForeignKey(self.schema + '.' + 'people.email_address'),
                    primary_key = True
                    ),
                )
            )

        DB.MailingLists = GrimoireDatabase._table (
            bases = (self.Base,), name = 'MailingLists',
            tablename = 'mailing_lists',
            schemaname = self.schema)

        DB.UIdentities = GrimoireDatabase._table (
            bases = (self.Base,), name = 'UIdentities',
            tablename = 'uidentities',
            schemaname = self.schema_id)

        if "organizations" in tables_id:
            DB.Organizations = GrimoireDatabase._table (
                bases = (self.Base,), name = 'Organizations',
                tablename = 'organizations',
                schemaname = self.schema_id,
                columns = dict (
                    id = Column(Integer, primary_key = True)
                    )
                )

        if "uidentities_organizations" in tables_id:
            DB.UIdentitiesOrganizations = GrimoireDatabase._table (
                bases = (self.Base,), name = 'UIdentitiesOrganizations',
                tablename = 'uidentities_organizations',
                schemaname = self.schema_id,
                )


class Query (GrimoireQuery):
    """Class for dealing with MLS queries"""


    def select_personsdata(self, kind):
        """Adds columns with persons data to select clause.

        Adds person_id, name, email, to the select clause of query.
        Joins MessagesPeople if not already joined

        Parameters
        ----------

        kind: {"senders", "starters", "followers"}
           Kind of person to select

        Returns
        -------

        MLSQuery: Result query, with new fields: person_id, name, email        

        """

        query = self.add_columns (label('person_id', DB.People.email_address),
                                  label("name", DB.People.name),
                                  label('email', DB.People.email_address))
        if kind == "senders":
            if DB.MessagesPeople not in self.joined:
                query = query.join (DB.MessagesPeople)
                self.joined.append (DB.MessagesPeople)
            query = query.filter (
                and_(DB.MessagesPeople.type_of_recipient == "From"))
        elif kind == "starters":
            raise Exception ("select_personsdata: Not yet implemented")
        elif kind == "followers":
            raise Exception ("select_personsdata: Not yet implemented")
        else:
            raise Exception ("select_personsdata: Unknown kind %s." \
                             % kind)
        return query


    def select_personsdata_uid(self, kind):
        """Adds columns with persons data to select clause (uid version).

        Adds person_id, name, to the select clause of query,
        having unique identities into account.
        Joins with PeopleUIdentities, UIdentities, and MessagesPeople if they
        are not already joined.
        Relationships: UIdentities.uuid == PeopleUIdentities.uuid,
          PeopleUIdentities.people_id == MessagesPeople.email_address
          MessagesPeople.type_of_recipient == "From"

        Parameters
        ----------

        kind: {"senders", "starters", "followers"}
           Kind of person to select

        Returns
        -------

        MLSQuery: Result query, with new fields: person_id, name, email        

        """

        query = self.add_columns (label("person_id", DB.UIdentities.uuid),
                                  label("name", DB.UIdentities.uuid))
        self.joined.append (DB.UIdentities)
        if kind == "senders":
            if DB.PeopleUIdentities not in self.joined:
                query = query.join (
                    DB.PeopleUIdentities,
                    DB.UIdentities.uuid == DB.PeopleUIdentities.uuid)
                self.joined.append (DB.PeopleUIdentities)
            if DB.MessagesPeople not in self.joined:
                query = query.join (
                    DB.MessagesPeople,
                    DB.MessagesPeople.email_address == \
                        DB.PeopleUIdentities.people_id)
                self.joined.append (DB.MessagesPeople)
            query = query.filter (DB.MessagesPeople.type_of_recipient == "From")
        elif kind == "starters":
            raise Exception ("select_personsdata: Not yet implemented")
        elif kind == "followers":
            raise Exception ("select_personsdata: Not yet implemented")
        else:
            raise Exception ("select_personsdata: Unknown kind %s." \
                             % kind)
        return query


    def select_activeperiod(self, date = "arrival"):
        """Add to select the activity period of messages sent.

        Adds min(messages.*_date) and max(messages.*_date)
        for selected messages.

        Parameters
        ----------

        date: {"arrival"|"first"}
           consider either arrival date or first date (default: arrival)

        Returns
        -------

        MLSQuery: Result query, with two new fields: firstdate, lastdate

        """

        if date == "arrival":
            date_field = DB.Messages.arrival_date
        elif date == "first":
            date_field = DB.Messages.first_date
        else:
            raise Exception ("select_activeperiod: Unknown kind of date: %s." \
                                 % date)
        query = self.add_columns (label('firstdate',
                                        func.min(date_field)),
                                  label('lastdate',
                                        func.max(date_field)))
        query = query.filter (
            DB.Messages.message_ID == DB.MessagesPeople.message_id)
        return query


    def select_orgs (self):
        """Select organizations data.

        Include id and name, as they appear in the organizations table.
        Warning: doesn't join other tables. For now, only works alone.

        Returns
        -------

        Query
            Including new columns in SELECT
        
        """

        query = self.add_columns (label("org_id", DB.Organizations.id),
                                  label("org_name", DB.Organizations.name))
        return query


    def filter_period(self, start = None, end = None, date = "arrival"):
        """Filter for a period, according to message dates

        Filter query, considering only messages in the corresponding period.
        Messages considered are between starting date and end date
        (exactly: start <= date < end)

        Parameters
        ----------

        start: datetime
           starting date
        end: datetime
           end date
        date: {"arrival"|"first"}
           consider either arrival date or first date (default: arrival)

        Returns
        -------

        MLSQuery: Resulting query

        """

        if (start is None) and (end is None):
            # No period, do nothing
            return self
        query = self
        if DB.Messages not in self.joined:
            query = query.join(DB.Messages)
            self.joined.append (DB.Messages)
        if date == "arrival":
            date_field = DB.Messages.arrival_date
        elif date == "first":
            date_field = DB.Messages.first_date
        else:
            raise Exception ("filter_period: Unknown kind of date: %s." \
                                 % date)
        if start is not None:
            self.start = start
            query = query.filter(date_field >= start.isoformat())
        if end is not None:
            self.end = end
            query = query.filter(date_field < end.isoformat())
        return query


    def filter_orgs (self, orgs):
        """Filter organizations matching a list of names

        Fiters query by a list of organization names, checking for
        them in the organizations table.

        Parameters
        ----------
        
        orgs: list of str
            List of organizations

        """

        query = self
        query = query.filter(DB.Organizations.name.in_(orgs))
        return query


    def filter_org_ids (self, list, kind = "senders", date = "arrival"):
        """Filter organizations matching a list of organization ids

        Fiters query by a list of organization ids.

        Parameters
        ----------
        
        list: list of int
            List of organization ids
        kind: {"senders", "starters", "followers"}
           Kind of person to select
        date: {"arrival"|"first"}
           consider either arrival date or first date (default: arrival)

        """

        query = self
        if date == "arrival":
            date_field = DB.Messages.arrival_date
        elif date == "first":
            date_field = DB.Messages.first_date
        else:
            raise Exception ("select_activeperiod: Unknown kind of date: %s." \
                                 % date)
        if kind == "senders":
            query = query.filter (DB.MessagesPeople.type_of_recipient == \
                                      "From")
        elif kind in ("starters", "followers"):
            raise Exception ("filter_org_ids: " \
                                 "Kind not yet implemented %s." \
                                 % kind)
        else:
            raise Exception ("filter_org_ids: Unknown kind %s." \
                                 % kind)
        query = query \
            .filter (DB.MessagesPeople.email_address == \
                        DB.PeopleUIdentities.people_id) \
            .join (DB.UIdentitiesOrganizations,
                   DB.PeopleUIdentities.uuid == \
                       DB.UIdentitiesOrganizations.uuid) \
            .filter (date_field.between (
                           DB.UIdentitiesOrganizations.start,
                           DB.UIdentitiesOrganizations.end
                           )) \
            .filter (DB.UIdentitiesOrganizations.organization_id.in_(list))
        return query


    def group_by_person (self):
        """Group by person

        Uses person_id field in the query to do the grouping.
        That field should be added by some other method.

        Parameters
        ----------

        None

        Returns
        -------

        SCMQuery object, with a new field (person_id)
        and a "group by" clause for grouping the results.

        """

        return self.group_by("person_id")

    def null_arrival (self):
        """Get all records in table Message with arrival_date being Null.

        """

        query = self.add_columns (label('arrival', DB.Messages.arrival_date)) \
            .filter (DB.Messages.arrival_date == None)
        return query
        
    def activity (self):
        """Return an ActivityList object.

        The query has to produce rows with the following fields:
        id (string),  name (string), start (datetime), end (datetime)

        """

        list = self.all()
        return ActivityList(list)

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()

    database = DB (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'oscon_openstack_mls',
                   schema_id = 'oscon_openstack_scm')
    session = database.build_session(Query, echo = False)

    #---------------------------------
    print_banner ("Number of messages which don't have arrival_date")
    res = session.query().null_arrival().count()
    print res

    #---------------------------------
    print_banner ("List of senders")
    res = session.query() \
        .select_personsdata("senders")
    for row in res.limit(10).all():
        print row.name, row.email

    #---------------------------------
    print_banner ("List of senders for a period")
    res = session.query() \
        .select_personsdata("senders") \
        .filter_period(start=datetime(2013,9,1),
                       end=datetime(2014,1,1))
    for row in res.limit(10).all():
        print row.name, row.email

    #---------------------------------
    print_banner ("List of senders for a period (uid)")
    res = session.query() \
        .select_personsdata_uid("senders") \
        .filter_period(start=datetime(2013,9,1),
                       end=datetime(2014,1,1))
    for row in res.limit(10).all():
        print row.name

    #---------------------------------
    print_banner ("List of (grouped) senders for a period")
    res = session.query() \
        .select_personsdata("senders") \
        .filter_period(start=datetime(2013,12,15),
                       end=datetime(2014,1,1)) \
        .group_by_person()
    for row in res.limit(10).all():
        print row.name, row.email
    print res.count()

    #---------------------------------
    print_banner ("List of (grouped) senders for a period (uid)")
    res = session.query() \
        .select_personsdata_uid("senders") \
        .filter_period(start=datetime(2013,12,15),
                       end=datetime(2014,1,1)) \
        .group_by_person()
    for row in res.limit(10).all():
        print row.name
    print res.count()

    #---------------------------------
    print_banner ("Activity period for senders")
    res = session.query() \
        .select_personsdata("senders") \
        .select_activeperiod() \
        .group_by_person()
    for row in res.limit(10).all():
        print row.person_id, row.name, row.email, row.firstdate, row.lastdate

    #---------------------------------
    print_banner ("Activity period for senders (uid)")
    res = session.query() \
        .select_personsdata_uid("senders") \
        .select_activeperiod() \
        .group_by_person()
    for row in res.limit(10).all():
        print row.person_id, row.name, row.firstdate, row.lastdate
