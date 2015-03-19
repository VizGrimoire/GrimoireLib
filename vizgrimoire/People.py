## Copyright (C) 2013 Bitergia
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
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

from vizgrimoire.GrimoireSQL import ExecuteQuery

def GetPersonIdentifiers (identities_db, upeople_id):
    q = """
        SELECT i.name, email, username, cou.name as country, org.name as affiliation, up.identifier
        FROM %s.uidentities up, %s.identities i,
             %s.organizations org, %s.enrollments enr,
             %s.countries cou, %s.nationalities nat
        WHERE up.uuid ='%s' AND
            up.uuid = i.uuid AND
            enr.uuid= up.uuid AND
            org.id = enr.organization_id AND
            nat.uuid= up.uuid AND
            cou.id = nat.country_id
        """ % (identities_db, identities_db, identities_db,
               identities_db, identities_db, identities_db, 
               upeople_id)
    return (ExecuteQuery(q))
