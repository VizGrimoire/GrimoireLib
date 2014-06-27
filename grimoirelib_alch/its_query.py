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
## Package to deal with queries for ITS data from *Grimoire
##  (Bicho databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy import create_engine, func, Column, Integer, ForeignKey, or_
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.sql import label
from datetime import datetime
from timeseries import TimeSeries
from activity import ActivityList

Base = declarative_base(cls=DeferredReflection)
BaseId = declarative_base(cls=DeferredReflection)

def buildSession(database, id_database = None, echo = False):
    """Create a session with the database
        
    - database: string, url of the database, in the format
       mysql://user:passwd@host:port/database
    - echo: boolean, output SQL to stdout or not
        
    Instantiatates an engine and a session to work with it
    """

    # To set Unicode interaction with MySQL
    # http://docs.sqlalchemy.org/en/rel_0_9/dialects/mysql.html#unicode
    trailer = "?charset=utf8&use_unicode=0"
    if id_database is None:
        id_database = database
    database = database + trailer
    id_database = id_database + trailer
    engine = create_engine(database,
                           convert_unicode=True, encoding='utf8',
                           echo=echo)
    id_engine = create_engine(id_database,
                              convert_unicode=True, encoding='utf8',
                              echo=echo)
    Base.prepare(engine)
    BaseId.prepare(id_engine)
    bindings = {Changes: engine,
                Issues: engine,
                People: engine,
                PeopleUPeople: engine,
                Trackers: engine,
                UPeople: id_engine}
    Session = sessionmaker(binds=bindings, query_cls=ITSQuery)
    session = Session()
    return (session)


class Changes(Base):
    """changes table"""

    __tablename__ = 'changes'
    issue_id = Column(Integer, ForeignKey('issues.id'))

class Issues (Base):
    """issues table"""

    __tablename__ = 'issues'
    changed_by = Column(Integer, ForeignKey('people.id'))

class People(Base):
    """upeople table"""

    __tablename__ = 'people'

class PeopleUPeople(Base):
    """people_upeople table"""

    __tablename__ = 'people_upeople'
    upeople_id = Column(Integer, ForeignKey('upeople.id'))

class Trackers(Base):
    """repositories table"""

    __tablename__ = 'trackers'

class UPeople(BaseId):
    """upeople table"""

    __tablename__ = 'upeople'

class ITSQuery (Query):
    """Class for dealing with ITS queries"""

    def __init__ (self, entities, session):
        """Create an ITSQuery.

        Parameters
        ----------

        entities: list of SQLAlchemy entities
           Entities (tables) to include in the query
        session: SQLAlchemy session
           SQLAlchemy session to use to connect to the database

        Attributes
        ----------

        self.start: datetime.datetime
           Start of the period to consider for commits. Default: None
           (start from the first commit)
        self.end: datetime.datetime
           End of the period to consider for commits. Default: None
           (end in the last commit)

        """

        self.start = None
        self.end = None
        # Keep an accounting of which tables have been joined, to avoid
        # undesired repeated joins
        self.joined = []
        Query.__init__(self, entities, session)


    def __repr__ (self):

        if self.start is not None:
            start = self.start.isoformat()
        else:
            start = "ever"
        if self.end is not None:
            end = self.end.isoformat()
        else:
            end = "ever"
        repr = "ITSQuery from %s to %s\n" % (start, end)
        repr = "  Joined: %s\n" % str(self.joined)
        repr += Query.__str__(self)
        return repr

    def __str__ (self):

        return self.__repr__()


    def select_personsdata(self, kind):
        """Adds columns with persons data to select clause.

        Adds people.user, people.email to the select clause of query.
        Does not join new tables.

        Parameters
        ----------

        kind: {"openers", "closers", "changers"}
           Kind of person to select

        Returns
        -------

        SCMObject: Result query, with new fields: id, name, email        

        """

        query = self.add_columns (label("person_id", People.id),
                                  label("name", People.user_id),
                                  label('email', People.email))
        if kind == "openers":
            person = Issues.submitted_by
            if Issues in self.joined:
                query = query.filter (People.id == person)
            else:
                self.joined.append (Issues)
                query = query.join (Issues, People.id == person)
        elif kind == "changers":
            person = Changes.changed_by
            if Changes in self.joined:
                query = query.filter (People.id == person)
            else:
                self.joined.append (Changes)
                query = query.join (Changes, People.id == person)
        elif kind == "closers":
            raise Exception ("select_personsdata: Not yet implemented")
        else:
            raise Exception ("select_personsdata: Unknown kind %s." \
                             % kind)
        return query


    def select_changesperiod(self):
        """Add to select the period of the changed tickets.

        Adds min(changes.changed_on) and max(changes.changed_on)
        for selected commits.
        
        Returns
        -------

        SCMObject: Result query, with two new fields: firstdate, lastdate

        """

        query = self.add_columns (label('firstdate',
                                        func.min(Changes.changed_on)),
                                  label('lastdate',
                                        func.max(Changes.changed_on)))
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

    session = buildSession(
        database='mysql://jgb:XXX@localhost/vizgrimoire_bicho',
        id_database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo=False)

    #---------------------------------
    print_banner ("List of openers")
    res = session.query() \
        .select_personsdata("openers") \
        .group_by_person()
    print res
    for row in res.limit(10).all():
        print row.person_id, row.name, row.email

    #---------------------------------
    print_banner ("Activity period for changers")
    res = session.query() \
        .select_personsdata("changers") \
        .select_changesperiod() \
        .group_by_person()
    print res
    for row in res.limit(10).all():
        print row.person_id, row.name, row.email, row.firstdate, row.lastdate
