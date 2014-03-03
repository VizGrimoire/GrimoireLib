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

def buildSession(database, echo):
    """Create a session with the database
    
    - database: string, url of the database, in the format
        mysql://user:passwd@host:port/database
    - echo: boolean, output SQL to stdout or not

    Instantiatates an engine and a session to work with it
    """

    engine = create_engine(database, encoding='utf8', echo=echo)
    Base.prepare(engine)    
    Session = sessionmaker(bind=engine)
    session = Session()
    return (session)

class CommitsQuery:
    """Base class for queries asking for commits

    Constructor:
    - session: session object, usually produced by
         Session = sessionmaker(bind=engine)
         session = Session()
    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"

    Commits considered are between starting date and end date
      (exactly: start <= date < end)
    """

    def filter_period(self, start = None, end = None):
        """Filter commits for a period"""

        if start is not None:
            self.q = self.q.filter(SCMLog.date >= start)
        if end is not None:
            self.q = self.q.filter(SCMLog.date < end)

    def selectors(self):
        """Selectors: what to extract from selected rows"""

        self.q = self.session.query(func.count(func.distinct(SCMLog.id)))

    def result(self):
        """Evaluate query"""

        return self.q.scalar()

    def query(self):
        """Return query"""

        return self.q

    def __init__ (self, session, start = None, end = None):
        self.session = session
        self.selectors()
        self.q = self.q.join(Actions)
        self.filter_period(start, end)

class NumCommitsQuery (CommitsQuery):
    """Get number of commits

    Returns: integer (number of commits)
    
    This is exactly like the root class"""

    pass
    
class TSCommitsQuery (CommitsQuery):
    """
    Get time series of commits (by month)

    Returns: list of tuples, each tuple is (ncommits, month)
    """

    def selectors(self):
        """Selectors: what to extract from selected rows"""

        self.q = self.session.query(
            label("ncommits", func.count(func.distinct(SCMLog.id))),
            label("month", func.month(SCMLog.date)),
            label("year", func.year(SCMLog.date))) \
            .group_by("month", "year").order_by("year", "month")

    def result(self):
        """Evaluate query"""

        return self.q.all()

class ListCommitsQuery (CommitsQuery):
    """
    Get commits

    Returns: list of tuples, each tuple is (id, date)
    """

    def selectors(self):
        """Selectors: what to extract from selected rows"""

        self.q = self.session.query(
            label("id", func.distinct(SCMLog.id)),
            label("date", SCMLog.date))

    def result(self):
        """Evaluate query"""

        return self.q.all()


if __name__ == "__main__":
    session = buildSession(
        database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo=False)

    # Number of commits
    res = NumCommitsQuery (session=session,
                           start="2012-09-01", end="2014-01-01")
    print res.result()
    res = NumCommitsQuery (session=session, end="2014-01-01")
    print res.result()

    # Time series of commits
    res = TSCommitsQuery (session=session,
                          start="2012-09-01", end="2014-01-01")
    for row in res.result():
        print str(row.year) + ", " + str(row.month) + ": " + str(row.ncommits)

    # List of commits
    res = ListCommitsQuery(session=session,
                           start="2012-09-01", end="2014-01-01")
    for row in res.query().limit(10).all():
        print row.id, row.date
