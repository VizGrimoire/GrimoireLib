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
from datetime import datetime
from dateutil.relativedelta import relativedelta

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

        - start: datetime, starting date
        - end: datetime, end date

        Commits considered are between starting date and end date
        (exactly: start <= date < end)
        """

        query = self
        if start is not None:
            self.start = start
            query = query.filter(SCMLog.date >= start.isoformat())
        if end is not None:
            self.end = end
            query = query.filter(SCMLog.date < end.isoformat())
        return query

    def select_ncommits(self):
        """Select number (count) of commits"""

        return self.add_columns (
            label("ncommits", func.count(func.distinct(SCMLog.id)))
            ) \
            .join(Actions)

    def group_by_period (self):
        """Group by time period (per month)"""

        return self \
            .add_columns (label("month", func.month(SCMLog.date)),
                          label("year", func.year(SCMLog.date))) \
            .group_by("month", "year").order_by("year", "month")

    def timeseries (self):
        """Return a TimeSeries object.

        The query has to include a group_by_period filter
        """

        data = []
        for row in self.all():
            data.append ((datetime (row.year, row.month, 1),
                         (row.ncommits,)))
        return TimeSeries (period = "months",
                           start = self.start, end = self.end,
                           data = data)

    def list(self):
        """Select a list of commits"""
        
        return self \
            .add_columns (label("id", func.distinct(SCMLog.id)),
                          label("date", SCMLog.date))

    def __repr__ (self):

        if self.start is not None:
            start = self.start.isoformat()
        else:
            start = "ever"
        if self.end is not None:
            end = self.end.isoformat()
        else:
            end = "ever"
        repr = "SCMQuery from %s to %s\n" % (start, end)
        repr += Query.__str__(self)
        return repr

    def __str__ (self):

        return self.__repr__()

    def __init__ (self, entities, session):
        """Initialize the object
        
        self.start and self.end will be used in case there are temporal
        limits for the query (useful to produce TimeSeries objects,
        which needs those.
        """

        self.start = None
        self.end = None
        Query.__init__(self, entities, session)

class TimeSeries:
    """Abstract data type for time series.

    Internally, a time series is a data structure with:

    - start: starting date for the time series (datetime)
    - end: end date for the time series (datetime)
    - period: sampling period (string)
    - data: list of tuples, each tuple being:
       - time for the beginning of the period (datetime)
       - string for representing the name of the period (datetime)
       - tuple with the values for that period
       The index in data is the period number, starting with 0.
    """

    def _min_date (self, data):
        """Calculate min date for all items in data

        - data list of tuples, each tuple of the form
            (date, values).
        """

        min = data[0][0]
        for (date, value) in data:
            if date < min:
                min = date
        return min

    def _max_date (self, data):
        """Calculate mxn date for all items in data

        - data list of tuples, each tuple of the form
            (date, values).
        """

        max = data[0][0]
        for (date, value) in data:
            if date > max:
                max = date
        return max

    def _period (self, date):
        """Get period id (from self.start) corresponding to a date

        period ids are integers, starting at 0
        """

        return date.year * 12 + date.month - \
            self.start.year * 12 - self.start.month

    def _normalize (self, data, novalue = None):
        """Normalize data intended to store as data in the class.

        - data: list of tuples, each tuple of the form
            (date, values), being values also a tuple of values
        - novalue: value to use for tuples with no value

        Produces data suitable for self.data, with
        tuples for those periods with no tuples,
        and sorts the result in ascending time order.
        """

        periods = self._period(self.end) + 1
        # Fill in result to return with dates and novalue
        result = [(self.start + relativedelta(months=period), novalue)
                  for period in xrange(periods)]
        # Fill in the values for the dates received in data
        for (date, value) in data:
            period = self._period(date)
            if result [period] == novalue:
                raise Exception("Dup period")
            else:
                result [period] = (result[period][0], value)
        return result

    def __repr__ (self):

        repr = "TimeSeries object (%s) " % (self.period)
        repr += "from %s to %s\n" % (self.start, self.end)
        repr += "data:\n"
        for item in self.data:
            repr += " %s: %s\n" % item
        return repr

    def __init__ (self, period, start, end, data, zerovalue = 0L):
        """Intialize a TimeSeries object
        
        - period: period for the time series (for now, "months")
        - start: starting time for the time series
        - end: ending time for the time series
        - data: list of tuples, each tuple of the form
            (date, values), being values also a tuple of values
        - zerovalue: value to use as "zero" for novalue tuples
            (those correspnding to periods without a value)

        start and/or end could be None
        """

        self.period = period
        if start is None:
            self.start = self._min_date(data)
        else:
            self.start = start
        if end is None:
            self.end = self._max_date(data)
        else:
            self.end = end
        # Use tuple of 0s for no values, same length as tuples in data
        novalue = (zerovalue,) * len (data[0][1])
        self.data = self._normalize(data, novalue)

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
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    print res.scalar()
    res = session.query().select_ncommits() \
        .filter_period(end=datetime(2014,1,1))
    print res.scalar()

    # Time series of commits
    res = session.query().select_ncommits() \
        .group_by_period() \
        .filter_period(end=datetime(2014,1,1))
    for row in res.all():
        print str(row.year) + ", " + str(row.month) + ": " + str(row.ncommits)
    ts = res.timeseries ()
    print (ts)

    # List of commits
    res = session.query() \
        .list() \
        .filter_period(start=datetime(2012,9,1),
                       end=datetime(2014,1,1))
    for row in res.limit(10).all():
        print row.id, row.date
