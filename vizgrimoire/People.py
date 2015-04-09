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
    """ Get people, company and country information """
    res = None
    q = """
        SELECT pro.uuid, pro.name, pro.email, cou.name as country,
               org.name as affiliation
        FROM %s.profiles pro
        JOIN %s.enrollments enr ON enr.uuid= pro.uuid
        JOIN %s.organizations org ON org.id = enr.organization_id
        LEFT JOIN %s.countries cou ON cou.code = pro.country_code
        WHERE pro.uuid ='%s'
        """ % (identities_db, identities_db, identities_db, identities_db,
               upeople_id)
    try:
        res = ExecuteQuery(q)
    except:
        # No organizations. Just people data and country data.
        q = """
            SELECT pro.uuid, pro.name, pro.email, cou.name as country
            FROM %s.profiles pro
            LEFT JOIN %s.countries cou ON cou.code = pro.country_code
            WHERE pro.uuid ='%s'
            """ % (identities_db, identities_db, upeople_id)
        res = ExecuteQuery(q)
    return res