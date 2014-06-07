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
## Miscellanous support for testing.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from json import loads

def equalJSON_file_persons (json, file_name):
    """Compare a JSON string with the content of a file (only persons).

    Returns a boolean with the result of the comparison. The comparison
    consists in comparing the persons.id and persons.age components of
    the dictionary (the rest is irrelevant)

    Parameters
    ----------

    json: string
       JSON string to compare
    file_name: string
       Name of file to compare

    Returns
    -------

    Boolean

    """

    with open (file_name, "r") as file:
        file_content = file.read()
    json_dict = loads(json)
    file_dict = loads(file_content)
    if json_dict["persons"]["id"] == file_dict["persons"]["id"]:
        print "persons.id is equal"
    if len(json_dict["persons"]["id"]) == len(file_dict["persons"]["id"]):
        print "len(persons.id) is equal"
    # Decrease age in one, because age of eg. 5 hours is 0 days
    file_dict["persons"]["age"] = [age - 1 
                                   for age in file_dict["persons"]["age"]]
    if json_dict["persons"]["age"] == file_dict["persons"]["age"]:
        print "persons.age is equal"
    else:
        for i in range(len(json_dict["persons"]["age"])):
            if json_dict["persons"]["age"][i] != \
                    file_dict["persons"]["age"][i]:
                print json_dict["persons"]["id"][i], \
                    json_dict["persons"]["name"][i], \
                    json_dict["persons"]["age"][i], \
                    file_dict["persons"]["age"][i]
    return loads(json) == loads(file_content)

