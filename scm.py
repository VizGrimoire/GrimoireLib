#! /usr/bin/python

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
## Some tests using SQLAlchemy to calculate some variables on SCM data
##  (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy import create_engine, func, Column, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.sql import label

Base = declarative_base(cls=DeferredReflection)

class SCMLog(Base):
    """scmlog table"""
    __tablename__ = 'scmlog'

class Actions(Base):
    """actions table"""
    __tablename__ = 'actions'
    commit_id = Column(Integer, ForeignKey('scmlog.id'))


class SCMQuery (Query):

    def filter_period(self, start = None, end = None):
        """Filter commits for a period

        - start: string, starting date, such as "2013-06-01"
        - end: string, end date, such as "2014-01-01"

        Commits considered are between starting date and end date
        (exactly: start <= date < end)
        """

        query = self
        if start is not None:
            query = query.filter(SCMLog.date >= start)
        if end is not None:
            query = query.filter(SCMLog.date < end)
        return query

    def select_ncommits(self):
        """Select number (count) of commits"""

        return self.add_columns (
            label("ncommits", func.count(func.distinct(SCMLog.id)))
            ) \
            .join(Actions)

    def timeseries(self):
        """Select timeseries (per month)"""

        return self \
            .add_columns (label("month", func.month(SCMLog.date)),
                          label("year", func.year(SCMLog.date))) \
            .group_by("month", "year").order_by("year", "month")

    def list(self):
        """Select a list of commits"""
        
        return self \
            .add_columns (label("id", func.distinct(SCMLog.id)),
                          label("date", SCMLog.date))

def buildSession(database, echo):
    """Create a session with the database
    
    - database: string, url of the database, in the format
        mysql://user:passwd@host:port/database
    - echo: boolean, output SQL to stdout or not

    Instantiatates an engine and a session to work with it
    """

    engine = create_engine(database, encoding='utf8', echo=echo)
    Base.prepare(engine)
    # Create a session linked to the SCMQuery class
    Session = sessionmaker(bind=engine, query_cls=SCMQuery)
    session = Session()
    return (session)


if __name__ == "__main__":
    session = buildSession(
        database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo=False)

    # Number of commits
    res = session.query().select_ncommits() \
        .filter_period(start="2012-09-01", end="2014-01-01")
    print res.scalar()
    res = session.query().select_ncommits() \
        .filter_period(end="2014-01-01")
    print res.scalar()

    # Time series of commits
    res = session.query().select_ncommits() \
        .timeseries() \
        .filter_period(end="2014-01-01")
    for row in res.all():
        print str(row.year) + ", " + str(row.month) + ": " + str(row.ncommits)

    # List of commits
    res = session.query() \
        .list() \
        .filter_period(start="2012-09-01", end="2014-01-01")
    for row in res.limit(10).all():
        print row.id, row.date
