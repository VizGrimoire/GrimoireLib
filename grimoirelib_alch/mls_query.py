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

from sqlalchemy import func, Column, String, Integer, ForeignKey, and_
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.sql import label
from datetime import datetime
from timeseries import TimeSeries
from activity import ActivityList
from common_query import table_factory, GrimoireDatabase, GrimoireQuery


class MLSDatabase(GrimoireDatabase):
    """Class for dealing with MLS (MLStats) databases.

    """

    def __init__(self, database, schema, schema_id):
        """Instatiation.

        Parameters
        ----------

        database: string        # ;:
           SQLAlchemy url for the database to be used, such as
           mysql://user:passwd@host:port/
        schema: string
           Schema name for the ITS data
        schema_id: string
           Schema name for the unique ids data
        
        """

        global Messages, MessagesPeople, People, PeopleUPeople, MailingLists
        global UPeople
        self.database = database
        Base = declarative_base(cls=DeferredReflection)
        self.Base = Base
        self.query_cls = MLSQuery
        Messages = table_factory (
            bases = (Base,), name = 'Messages',
            tablename = 'messages',
            schemaname = schema,
            columns = dict (
                mailing_list_url = Column(
                    String,
                    ForeignKey(schema + '.' + 'mailing_lists.mailing_list_url'))
                ))
        MessagesPeople = table_factory (
            bases = (Base,), name = 'MessagesPeople',
            tablename = 'messages_people',
            schemaname = schema,
            columns = dict (
                mailing_list_url = Column(
                    String,
                    ForeignKey(schema + '.' + 'mailing_lists.mailing_list_url')),
                message_id = Column(
                    String,
                    ForeignKey(schema + '.' + 'messages.message_ID'))     
                ))
        People = table_factory (bases = (Base,), name = 'People',
                                tablename = 'people',
                                schemaname = schema)
        PeopleUPeople = table_factory (bases = (Base,), name = 'PeopleUPeople',
                                tablename = 'people_upeople',
                                schemaname = schema,
                                columns = dict (
                upeople_id = Column(Integer,
                                    ForeignKey(schema + '.' + 'upeople.id'))
                ))
        MailingLists = table_factory (bases = (Base,), name = 'MailingLists',
                                tablename = 'mailing_lists',
                                schemaname = schema)
        UPeople = table_factory (bases = (Base,), name = 'UPeople',
                                tablename = 'upeople',
                                schemaname = schema_id)


class MLSQuery (GrimoireQuery):
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

        query = self.add_columns (label('person_id', People.email_address),
                                  label("name", People.name),
                                  label('email', People.email_address))
        if kind == "senders":
            if MessagesPeople not in self.joined:
                query = query.join (MessagesPeople)
                self.joined.append (MessagesPeople)
            query = query.filter (
                and_(MessagesPeople.type_of_recipient == "From"))
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
        Joins with PeopleUPeople, UPeople, and MessagesPeople if they
        are not already joined.
        Relationships: UPeople.id == PeopleUPeople.upeople_id,
          PeopleUPeople.people_id == MessagesPeople.email_address
          MessagesPeople.type_of_recipient == "From"

        Parameters
        ----------

        kind: {"senders", "starters", "followers"}
           Kind of person to select

        Returns
        -------

        MLSQuery: Result query, with new fields: person_id, name, email        

        """

        query = self.add_columns (label("person_id", UPeople.id),
                                  label("name", UPeople.identifier))
        self.joined.append (UPeople)
        if kind == "senders":
            if PeopleUPeople not in self.joined:
                query = query.join (
                    PeopleUPeople,
                    UPeople.id == PeopleUPeople.upeople_id)
                self.joined.append (PeopleUPeople)
            if MessagesPeople not in self.joined:
                query = query.join (
                    MessagesPeople,
                    MessagesPeople.email_address == PeopleUPeople.people_id)
                self.joined.append (MessagesPeople)
            query = query.filter (MessagesPeople.type_of_recipient == "From")
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
            date_field = Messages.arrival_date
        elif date == "first":
            date_field = Messages.first_date
        else:
            raise Exception ("filter_period: Unknown kind of date: %s." \
                                 % date)
        query = self.add_columns (label('firstdate',
                                        func.min(date_field)),
                                  label('lastdate',
                                        func.max(date_field)))
        query = query.filter (Messages.message_ID == MessagesPeople.message_id)
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
        if Messages not in self.joined:
            query = query.join(Messages)
            self.joined.append (Messages)
        if date == "arrival":
            date_field = Messages.arrival_date
        elif date == "first":
            date_field = Messages.first_date
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


    def activity (self):
        """Return an ActivityList object.

        The query has to produce rows with the following fields:
        id (string),  name (string), start (datetime), end (datetime)

        """

        list = self.all()
        return ActivityList(list)

if __name__ == "__main__":

    import sys
    import codecs
    from standalone import print_banner

    # Trick to make the script work when using pipes
    # (pipes confuse the interpreter, which sets codec to None)
    # http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)

    MLSDB = MLSDatabase(database = 'mysql://jgb:XXX@localhost/',
                        schema = 'oscon_openstack_mls',
                        schema_id = 'oscon_openstack_scm')
    session = MLSDB.build_session(MLSQuery, echo = False)

    #---------------------------------
    print_banner ("List of senders")
    res = session.query() \
        .select_personsdata("senders")
    print res
    for row in res.limit(10).all():
        print row.name, row.email

    #---------------------------------
    print_banner ("List of senders for a period")
    res = session.query() \
        .select_personsdata("senders") \
        .filter_period(start=datetime(2013,9,1),
                       end=datetime(2014,1,1))
    print res
    for row in res.limit(10).all():
        print row.name, row.email

    #---------------------------------
    print_banner ("List of senders for a period (uid)")
    res = session.query() \
        .select_personsdata_uid("senders") \
        .filter_period(start=datetime(2013,9,1),
                       end=datetime(2014,1,1))
    print res
    for row in res.limit(10).all():
        print row.name

    #---------------------------------
    print_banner ("List of (grouped) senders for a period")
    res = session.query() \
        .select_personsdata("senders") \
        .filter_period(start=datetime(2013,12,15),
                       end=datetime(2014,1,1)) \
        .group_by_person()
    print res
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
    print res
    for row in res.limit(10).all():
        print row.name
    print res.count()

    #---------------------------------
    print_banner ("Activity period for senders")
    res = session.query() \
        .select_personsdata("senders") \
        .select_activeperiod() \
        .group_by_person()
    print res
    for row in res.limit(10).all():
        print row.person_id, row.name, row.email, row.firstdate, row.lastdate

    #---------------------------------
    print_banner ("Activity period for senders (uid)")
    res = session.query() \
        .select_personsdata_uid("senders") \
        .select_activeperiod() \
        .group_by_person()
    print res
    for row in res.limit(10).all():
        print row.person_id, row.name, row.firstdate, row.lastdate
