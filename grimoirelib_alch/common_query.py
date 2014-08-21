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
## Package to deal with common functionality for queries to *Grimoire
##  databases
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection


class GrimoireDatabase:
    """Class for dealing with Grimoire databases.

    """

    def __init__(self, url, schema, schema_id):
        """Instatiation.

        Parameters
        ----------

        url: string
           SQLAlchemy url for the database to be used, such as
           mysql://user:passwd@host:port/
        schema: string
           Schema name for the ITS data
        schema_id: string
           Schema name for the unique ids data
        
        """

        self.url = url
        self.schema = schema
        self.schema_id = schema_id
        self.query_cls = self._query_cls()
        self.Base = declarative_base(cls=DeferredReflection)
        self._create_tables()

    def _query_cls(self):
        """Return the default Query class for this database

        Returns
        -------

        GrimoireQuery: default Query class.

        """

        raise Exception ("_query_cls should be provided by child class")

    def _create_tables(self):
        """Create all SQLAlchemy tables.

        Builds a SQLAlchemy class per SQL table, by using _table().
        It assumes self.Base, self.schema and self.schema_id are already
        set (see __init__() code).

        """

        raise Exception ("_create_tables should be provided by child class")

    @staticmethod
    def _table (bases, name, tablename, schemaname, columns = {}):
        """Factory for building SQAlchemy table classes.

        This function allows for the creation of classes for each of the
        tables in the database. This way, table classes can be created
        when the code is running, and some parameters such as the schema
        name are available (as opposed to using the Python class constructor,
        which requieres that all the parameters are ready at declaration time.
        
        Parameters
        ----------

        base: list of classes to inherit from
           Base classes, of which the resulting clase will inherit
        name: string
           Name of the class to be built
        tablename: string
           Name of the database table to be interfaced by the class
        schemaname: string
           Name of the schema to which the table belongs

        Returns
        -------

        class in the bases hierarchies
        
        """

        attr = dict (
            __tablename__ = tablename,
            __table_args__ = {'schema': schemaname}
            )
        for key in columns:
            attr[key] = columns[key]
        table_class = type(name, bases, attr)
        return table_class

    def build_session(self, query_cls = None, echo = False):
        """Create a session with the database

        Instantiatates an engine and a session to work with it.

        Parameters
        ----------
        
        query_cls: Class in GrimoireQuery hierarchy
           Query class (Default: None)
           None means the "canonical query class", defined by the subclass,
           such as ITSQuery for ITSDatabase.
        echo: boolean
           Output SQL to stdout or not.
        
        """
        
        # To set Unicode interaction with MySQL
        # http://docs.sqlalchemy.org/en/rel_0_9/dialects/mysql.html#unicode
        trailer = "?charset=utf8&use_unicode=0"
        database = self.url + trailer
        engine = create_engine(database,
                               convert_unicode=True, encoding='utf8',
                               echo=echo)
        self.Base.prepare(engine)
        if query_cls is None:
            query_cls = self.query_cls
        Session = sessionmaker(bind=engine, query_cls=query_cls)
        session = Session()
        return (session)


class GrimoireQuery (Query):
    """Class for dealing with Grimiore queries

    In fact, this is the root of the hierarchy of classes, not
    intended for direct use.

    """

    def __init__ (self, entities, session):
        """Create a GrimoreQuery.

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
        repr = "Query from %s to %s\n" % (start, end)
        repr = "  Joined: %s\n" % str(self.joined)
        repr += Query.__str__(self)
        return repr

    def __str__ (self):

        return self.__repr__()

