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

