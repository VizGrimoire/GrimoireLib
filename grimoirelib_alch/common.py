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
       Schema name for the unique ids data
    
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
           Schema name for the unique ids data

        """

        self.url = url
        self.schema = schema
        self.schema_id = schema_id

    def __repr__ (self):

        repr = "Database url: " + self.url + "\n"
        repr = repr + "Main database schema: " + self.schema + "\n"
        repr = repr + "Unique id database schema: " + self.schema_id + "\n"
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

    def __init__ (self, name, conditions = (), datasource = None):
        """Instantiation of an entity of the family.

        This method should be provided by each child in the hierarchy.
        name are conditions are always present, datasource will be
        dependent on how the value for the entity is computed
        (eg, from a database, from an object, etc.)

        Parameters
        ----------
        
        name: string
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        datasource: database, object, etc.
           Definition of the data source used to compute values for
           the entity.

        """

        raise Exception ("__init__ should be provided by child classes")

class DBFamily:
    """Root of hierarchy of families using a database as data source.

    Families of entities that produce data by querying a Grimoire database.

    Methods _create_session and _init should be provided when defining
    a child in this hierarchy.

    """

    def __init__ (self, name = None, conditions = (),
                  session = None,
                  database = None,
                  echo = False):
        """Instantiation of an entity of the family.

        Instantiation can be specified with a DatabaseDefinition
        or with an SQLAlchemy session. The resulting object can be
        used to obtain data about an entity, or just to get
        a session suitable to be used in the creation of further objects
        of this class.

        Parameters
        ----------

        name: string
           Entity name (default: None)
           If None, no real initialization of the entity is done,
           only a session is produced (useful for obtaining a session
           to reuse later).
           Otherwise, each child class will define valid strings for
           the entities that can be instantiated for the corresponding
           family.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.
           (default: empty list).
        session: sqlalchemy.orm.session
           Session for working with an SQLAlchemy database.
        database: Common.DatabaseDefinition
           Names defining the database. If session is not None,
           database is silently ignored, and session is used instead.
        echo: Boolean
           Write SQL queries to output stream or not.

        """

        if session is not None:
            self.session = session
        elif database is not None:
            # Create a session using info in database
            self.session = self._create_session(database = database,
                                                echo = echo)
        else:
            raise Exception ("Family: Either a session or a " + \
                                 "database must be specified")
        if name is not None:
            self._init (name, conditions)

    def get_session (self):
        """Obtain the session being used.

        This session can be reused when instantiating other objects
        of this class.

        Returns
        -------

        SQLAlchemy session: session.

        """

        return self.session

    def _init (self, name, conditions):
        """Initialize the entity, once a session is ready.

        It can assume self.session is already set up. This method
        is intended to be provided by children of this root class,
        initializing what is needed for each specific entity.

        
        Parameters
        ----------

        name: string
           Entity name.
        conditions: list of Condition objects
           Conditions to be applied to provide context to the entity.

        """

        raise Exception ("_init should be provided by child classes")
        
    def _create_session (self, database, echo):
        """Creates a session given a database definition.

        Parameters
        ----------

        database: Common.DatabaseDefinition
           Names defining the database.
        echo: Boolean
           Write SQL queries to output stream or not.

        Returns
        -------

        session: SQLAlchemy session suitable for querying.

        """

        raise Exception ("_create_session should be provided by child classes")
