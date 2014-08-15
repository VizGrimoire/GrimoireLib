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

class Variable:
    """Root of hierarchy of variables.

    When defining a child in this hierarchy, methods _create_session and
    _init should be provided. 

    """

    def __init__ (self, var = None, conditions = (),
                  session = None,
                  database = None,
                  echo = False):
        """Instantiation of the object.

        Instantiation can be specified with a DatabaseDefinition
        or with an SQLAlchemy session. The resulting object can be
        used to obtain databa about a variable, or just to get
        a session suitable to be used in the creation of further objects
        of this class.
        Uses _produce_query(), which should be provided by child classes.

        Parameters
        ----------

        var: string
           Variable (default: None)
           If None, no real initialization of the variable is done,
           only a session is produced (useful for obtaining a session
           to reuse later).
           Otherwise, each child class will define valid strings.
        conditions: list of Condition objects
           Conditions to be applied to provide context to variable
           (default: empty list).
        session: SQLAlchemy session
           Session for working with an SQLAlchemy database.
        database: Common.DatabaseDefinition
           Names defining the database. If session is not None,
           database is silently ignored, and session is used instead.
        echo: Boolean
           Write SQL queries to output stream or not.

        """

        if session is None:
            # Create a session using info in database
            self.session = self._create_session(database = database,
                                                echo = echo)
        else:
            self.session = session
        if var is not None:
            self._init (var, conditions)

    def get_session (self):
        """Obtain the session being used.

        This session could be reused in further queries for variables.

        Returns
        -------

        SQLAlchemy session: session.

        """

        return self.session

    def _init (self, var, conditions):
        """Initialize everything, once a session is ready.

        It can assume self.session is already set up. This method
        is intended to be provided by children of this root class,
        initializing what is needed for each specific kind of variable.

        
        Parameters
        ----------

        var: string
           Variable
        conditions: list of Condition objects
           Conditions to be applied to provide context to variable

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
