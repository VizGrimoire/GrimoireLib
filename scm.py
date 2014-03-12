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
## Package to deal with SCM data from *Grimoire (CVSAnalY databases)
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from scm_query import buildSession, SCMQuery

class SCM:
    
    def timeseries (self):

        return self.query.group_by_period().timeseries()

    # def _buildSession(self, database, echo):
    #     """Create a session with the database
        
    #     - database: string, url of the database, in the format
    #         mysql://user:passwd@host:port/database
    #     - echo: boolean, output SQL to stdout or not

    #     Instantiatates an engine and a session to work with it
    #     """

    #     engine = create_engine(database, encoding='utf8', echo=echo)
    #     Base = declarative_base(cls=DeferredReflection)
    #     Base.prepare(engine)
    #     # Create a session linked to the SCMQuery class
    #     Session = sessionmaker(bind=engine, query_cls=SCMQuery)
    #     session = Session()
    #     return (session)

    def __init__ (self, database, var, dates = (None, None), echo = False):

        self.session = buildSession(
            database=database,
            echo=echo)
        if var == "ncommits":
            self.query = self.session.query().select_ncommits()
        elif var == "listcommits":
            self.query = self.session.query().select_listcommits()
        self.query = self.query.filter_period(start = dates[0],
                                              end = dates[1])

if __name__ == "__main__":

    datos = SCM (database = 'mysql://jgb:XXX@localhost/vizgrimoire_cvsanaly',
                 var = "ncommits", dates = (None, None))
    print datos.timeseries()
