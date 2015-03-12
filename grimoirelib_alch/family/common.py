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

from grimoirelib_alch.query.common import GrimoireDatabase

from sqlalchemy.orm import Session

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

        datasource: {sqlalchemy.orm.Session | GrimoireDatabase}
           If Session, active session for working with an SQLAlchemy database.
           If GrimoireDatabase, a Grimoire database object.
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
        elif isinstance(datasource, GrimoireDatabase):
            # Create a session using info in database
            query_cls = self._query_cls ()
            self.session = datasource.build_session(query_cls = query_cls,
                                                    echo = echo)
        else:
            raise Exception ("DBFamily: datasource must be of " + \
                                 "sqlalchemy.orm.Session or " + \
                                 "GrimoreDatabase class.")
        self._init (name, conditions)

    def _query_cls(self):
        """Return the default Query class.

        If None, the underlaying Query class used by default by the
        datasource will be used. This function returns None, child
        classes should override that if needed (usually, when a
        Query class which is not the default one for the datasource
        is needed).

        Returns
        -------

        GrimoireQuery: Query class.

        """

        return None

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


class Entities(type):
    """Watcher for a metaclass, maintaining a dictionary with its subclasses

    This watcher class maintains a dictionary with all the subclasses of
    the metaclass that uses it.

    Attributes
    ----------

    subclasses : dictionary
        Subclasses of metaclass, keyed by name attribute of each subclass
        (values are the subclasses themselves)

    """

    subclasses = {}

    def __init__(cls, name, bases, clsdict):
        """Add subclass cls to subclasses, keyed by its name attribute.

        """

        if clsdict['name'] is not None:
            Entities.subclasses[clsdict['name']] = cls
    

class Entity ():
    """Metaclass, root of all entities.

    This is the root of the hierarchy of entities.
    It uses a watcher for keeping a dictionary with all its subclasses.
    Defines attributes and methods to be used in all the hierarchy.

    Attributes
    ----------

    name: str
        Entity name used for the entity represented by the class
    desc: str
        Description of the entity (one line)
    longdesc: str
        Description of the entity (long)

    Methods
    -------

    query (q)
        Returns the query to produce the entity

    """

    __metaclass__ = Entities

    name = None
    desc = None
    longdesc = None

    @staticmethod
    def query (q):
        """Return the query to produce this entity.

        Parameters
        ----------

        q: query.scm.Query
            Base query

        Returns
        -------

        query.scm.Query
            Query with the needed filters to produce the entity.

        """

        raise Exception ("query(): real entities should provide real code")
