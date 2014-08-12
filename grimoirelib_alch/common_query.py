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

def table_factory (bases, name, tablename, schemaname, columns = {}):
    """Factory for building table classes.

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

    class in the Base hierarchy

    """

    attr = dict (
        __tablename__ = tablename,
        __table_args__ = {'schema': schemaname}
        )
    for key in columns:
        attr[key] = columns[key]
    table_class = type(name, bases, attr)
    return table_class
