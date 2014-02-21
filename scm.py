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
from sqlalchemy.schema import ForeignKeyConstraint

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

def getNCommitsQuery (start, end):
    """
    Get number of commits

    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"

    Get number of commits between starting date and end date
      (exactly: start <= date < end)
    """

    res = session.query(func.count(func.distinct(SCMLog.id))) \
        .join(Actions) \
        .filter(SCMLog.date >= start) \
        .filter(SCMLog.date < end)
    return res

def getTSCommitsQuery (start, end):
    """
    Get time series of commits (by month)

    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"

    Returns: list of tuples, each tuple is (ncommits, month)

    Get time series of commits between starting date and end date
      (exactly: start <= date < end)
    """

    res = session.query(func.count(func.distinct(SCMLog.id)).label("ncommits"),
                        func.month(SCMLog.date).label("month"),
                        func.year(SCMLog.date).label("year")) \
        .join(Actions) \
        .filter(SCMLog.date >= start) \
        .filter(SCMLog.date < end) \
        .group_by("month", "year").order_by("year", "month")
    return res

def getCommitsQuery (start, end):
    """
    Get commits

    - start: string, starting date, such as "2013-06-01"
    - end: string, end date, such as "2014-01-01"

    Returns: list of tuples, each tuple is (id, date)

    Get all commits between starting date and end date
      (exactly: start <= date < end)
    """

    res = session.query(func.distinct(SCMLog.id).label("id"),
                        SCMLog.date.label("date")).\
        join(Actions) \
        .filter(SCMLog.date >= start) \
        .filter(SCMLog.date < end)
    return res


if __name__ == "__main__":
    session = buildSession(
        database='mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
        echo=False)

    # Number of commits
    res = getNCommitsQuery(start="2012-09-01", end="2014-01-01")
    print res.scalar()
    # Time series of commits
    res = getTSCommitsQuery(start="2012-09-01", end="2014-01-01")
    for row in res.all():
        print row.ncommits, row.year, row.month
    # List of commits
    res = getCommitsQuery(start="2012-09-01", end="2014-01-01")
    for row in res.limit(10).all():
        print row.id, row.date

