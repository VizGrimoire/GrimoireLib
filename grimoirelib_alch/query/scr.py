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
## Package to deal with queries for SCR (source code review) data
##  from *Grimoire (Bicho databases with Gerrit backend)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.query.its import (
    DB as ITSDB, Query as ITSQuery
    )

from grimoirelib_alch.query.common import GrimoireDatabase

from sqlalchemy import func, Column, Integer, ForeignKey
from sqlalchemy.sql import label


class DB (ITSDB):
    """Class for dealing with SCR (Bicho-Gerrit) databases.

    Inherits from the class to deal with ITS databases, because it
    is basically the same (factoring out tables produced by the 
    Gerrit backend).

    """
 
    def _query_cls(self):
        """Return que defauld Query class for this database

        Returns
        -------

        GrimoireQuery: default Query class.

        """

        return Query

    def _create_tables(self):
        """Create all SQLAlchemy tables.

        Builds a SQLAlchemy class per SQL table, by using _table().
        It assumes self.Base, self.schema and self.schema_id are already
        set (see super.__init__() code).

        Uses super._create_tables() to create ITS basic tables, which
        are shared by SCR databases.

        """

        ITSDB._create_tables(self)
        DB.IssuesExtGerrit = GrimoireDatabase._table (
            bases = (self.Base,), name = 'IssuesExtGerrit',
            tablename = 'issues_ext_gerrit',
            schemaname = self.schema
                )


class Query (ITSQuery):
    """Class for dealing with SCR queries.

    Inherits all methods for dealing with ITS queries

    """


if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()

    database = DB (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'reviews_wikimedia_2014_09_11',
                   schema_id = 'vizgrimoire_cvsanaly')
    session = database.build_session(Query, echo = False)

    #---------------------------------
    print_banner ("List of openers")
    res = session.query() \
        .select_personsdata("openers") \
        .group_by_person()
    print res
    for row in res.limit(10).all():
        print row.person_id, row.name, row.email

