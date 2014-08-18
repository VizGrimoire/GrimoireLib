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
## Common functionality for GrimoireLib
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from sqlalchemy.orm import Session

class DatabaseDefinition:
    """Class for defining a Grimoire database.

    For defining a "Grimoire Database" usually a SQLAlchemy database
    name and a set of schemas are needed.

    Attributes
    ----------

    url: string
       SQLAlchemy url for the database to be used, such as
       mysql://user:passwd@host:port/
    schema: string
       Schema name for the main data (SCM, ITS, MLS, etc.)
    schema_id: string
       Schema name for the unique ids data.
    
    """

    def __init__ (self, url, schema, schema_id):
        """Create a database definition.

        Parameters
        ----------

        url: string
           SQLAlchemy url for the database to be used, such as
           mysql://user:passwd@host:port/
        schema: string
           Schema name for the main data (SCM, ITS, MLS, etc.)
        schema_id: string
           Schema name for the unique ids data.
        echo: Boolean
           Write SQL queries to output stream or not (default: False).

        """

        self.url = url
        self.schema = schema
        self.schema_id = schema_id

    def create_session (self, echo = False):
        """Creates a session for the defined database.

        Uses _datasource_cls() to determine which kind of Grimoire database
        is being used (SCM, ITS, MLS, etc.). It should be provided by
        children classes.

        echo: Boolean
           Write debugging information to output stream or not.

        Returns
        -------

        session: sqalchemy.orm.Session suitable for querying.

        """

        database_cls, query_cls = self._datasource_cls()
        DB = database_cls(database = self.url,
                         schema = self.schema,
                         schema_id = self.schema_id)
        return DB.build_session(query_cls, echo = echo)

    def _datasource_cls(self):
        """Return classes related to datasource.

        Returns:
        --------

        common_query.GrimoireDatabase: subclass for Grimoire database to use
        common_query.GrimoireQuery: subclass for Grimoire Query to use

        """

        raise Exception ("_datasource_cls should be provided by child class")

    def __repr__ (self):

        repr = "Database url: " + self.url + "\n"
        repr = repr + " Main database schema: " + self.schema + "\n"
        repr = repr + " Unique id database schema: " + self.schema_id + "\n"
        return repr

    def __str__ (self):

        return self.__repr__()

class Family:
    """Root of hierarchy of families of entities.

    An entity is an abstract item, with a unique name, representing
    some property or measurement of the analyzed project, with some
    specific semantics. An entity can be an specific measurement
    (such as number of commits), but also other kinds of data
    (such as developer name) or a structured combination of specific
    measurements or other kinds of data (such as a time series of the
    number of commits per month, or a list of developers).

    A family is a collection of entities that have some semantic
    relationship, usually coming from the same or similar tables in
    the database. For example, entities directly related to the source
    code management system (SCM family), to the issue tracking system
    (ITS family), or maybe to the aging studies (Ages family).
    
    A family of entities is modeled as a class, which when instantiated
    as an object corresponds to a given entity in the context of
    zero or more conditions.

    When a family class is instantiated into an entity object, a session
    is created, which can be reused for further entities of the same class,
    thus reusing the underlying SQLAlchemy session.

    """

    def __init__ (self, datasource, name, conditions = (), echo = False):
        """Instantiation of an entity of the family.

        This method should be provided by each child in the hierarchy.
        name are conditions are always present, datasource will be
        dependent on how the value for the entity is computed
        (eg, from a database, from an object, etc.)

        Parameters
        ----------
        
        datasource: database, object, etc.
           Definition of the data source used to compute values for
           the entity.
        name: string
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        echo: Boolean
           Write debugging information to output stream or not.

        """

        raise Exception ("__init__ should be provided by child class")


class DBFamily:
    """Root of hierarchy of families using a Grimoire database as data source.

    Families of entities that produce data by querying a Grimoire database.

    Method _produce_query() should be provided when defining
    a child in this hierarchy.

    """

    def __init__ (self, datasource, name, conditions = (),
                  echo = False):
        """Instantiation of an entity of the family.

        Instantiation can be specified with a DatabaseDefinition
        or with an SQLAlchemy session. The resulting object can be
        used to obtain data about an entity, or just to get
        a session suitable to be used in the creation of further objects
        of this class.

        Parameters
        ----------

        datasource: {sqlalchemy.orm.Session | Common.DatabaseDefinition}
           If Session, active session for working with an SQLAlchemy database.
           If DatabaseDefinition, the names defining a Grimoire database.
        name: string
           Entity name. Each child class will define valid strings for
           the entities that can be instantiated for the corresponding
           family (_init() method).
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        echo: Boolean
           Write SQL queries and other debugging infor to output stream or not.

        """

        self.echo = echo
        if isinstance(datasource, Session):
            self.session = datasource
        elif isinstance(datasource, DatabaseDefinition):
            # Create a session using info in database
            self.session = datasource.create_session(echo = echo)
        else:
            raise Exception ("DBFamily: datasource must be of " + \
                                 "sqlalchemy.orm.Session or " + \
                                 "DatabaseDefinition class.")
        self._init (name, conditions)

    def _init (self, name, conditions):
        """Initialize everything, once a session is ready.

        Uses _produce_query(), which should be provided by child classes.
 
        Parameters
        ----------
        
        name: {"list_authors" | "list_committers" |
           "list_uauthors" | "list_ucommitters"}
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
        """

        self.query = self.session.query()
        self._produce_query(name)
        for condition in conditions:
            self.query = condition.filter(self.query)

    def _produce_query (self, name):
        """Produce the base query to obtain activity per person.

        This function assumes that self.query was already initialized.
        The produced query replaces self.query.
        Conditions will be applied (as filters) later to the query
        produced by this function.

        Parameters
        ----------
        
        name: string
           Entity name.

        """

        raise Exception ("_produce_query should be provided by child class")


    def get_session (self):
        """Obtain the session being used.

        This session can be reused when instantiating other objects
        of this class.

        Returns
        -------

        SQLAlchemy session: session.

        """

        return self.session


class Condition ():
    """Root of all conditions

    Provides a method to check if the object to which to apply is of the
    right class.

    """

    @staticmethod
    def _check_class (object):
        """Check that object is an instance of Family.

        Raise exception if it is not.

        """

        if not isinstance (object, Family):
            raise Exception ("Condition: " + \
                                 "This condition can only be applied to " + \
                                 "Family entities.")


class DBCondition (Condition):
    """Root of all database-related conditions

    Provides a filter method which will be called when applying the condition.
    """

    def filter (query):
        """Filter to apply for this condition

        Parameters
        ----------

        query: slalchemy.query
           Query to which the filter will be applied

        """

        return query
