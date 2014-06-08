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

basic_types = (str, unicode, int, long, float, bool)
sequence_types = (list, tuple)

def compare_items (a, b, level = 0):
    """Compare two items, of any type.
 
    Returns True if both are equal, False if they are different.
    If they are different, tries to find out where they differ.

    Parameters
    ----------

    a: any
       First item to compare
    b: any
       Second item to compare
    level: int
       Level of data (for indentation). Default: 0

    Returns
    -------

    Boolean: True if items are equal

    """

    if a != b:
        equal = False
        if isinstance(a, basic_types):
            str_a = str(a)
        else:
            str_a = False
        if isinstance(b, basic_types):
            str_b = str(b)
        else:
            str_b = False
        if str_a and str_b:
            strings = " " * level + "Basic types differ: " + \
                str_a + ", " + str_b + "\n"
        elif (isinstance(a, sequence_types) and
              isinstance(b, sequence_types)):
            (equal, strings) = compare_seqs (a, b, level + 1)
            strings = " " * level + "Sequences differ:\n" + strings
        elif (isinstance(a, dict) and
              isinstance(b, dict)):
            (equal, strings) = compare_dicts (a, b, level + 1)
            strings = " " * level + "Dictionaries differ:\n" + strings
        else:
            strings = " " * level + "Other types differ: " + \
                type(a) + ", " + type(b)
    else:
        equal = True
        strings = ""
    return (equal, strings)


def compare_seqs (a, b, level = 0):
    """Compare two sequences.
 
    Returns True if both are equal, False if they are different.
    If they are different, tries to find out where they differ.

    Parameters
    ----------

    a: sequence
       First data structure to compare
    b: sequence
       Second data structure to compare
    level: int
       Level of data (for indentation). Default: 0

    Returns
    -------

    Boolean: True if sequences are equal

    """

    equal = True
    strings = ""
    len_a = len(a)
    len_b = len(b)
    length = len_b
    if len_a != len_b:
        strings = " " * level + \
            "Lengths are different: %d, %d" % (len_a, len_b)
        equal = False
        if len_a < len_b:
            length = len_a
    for i in range (length):
        if a[i] != b[i]:
            (equal_items, strings_items) = compare_items (a[i], b[i],
                                                          level + 1)
            if not equal_items:
                equal = False
                strings = " " * level + \
                    "Item %d is different:\n" % (i,) + strings_items
    return (equal, strings)

def compare_dicts (a, b, level = 0):
    """Compare two dictionaries.
 
    Returns True if both are equal, False if they are different.
    If they are different, tries to find out where they differ.

    Parameters
    ----------

    a: dictionary
       First data structure to compare
    b: dictionary
       Second data structure to compare
    level: int
       Level of data (for indentation). Default: 0

    Returns
    -------

    Boolean: True if dictionaries are equal

    """

    equal = True
    keys_a = sorted(a.keys())
    keys_b = sorted(b.keys())
    if keys_a != keys_b:
        equal = False
        (equal_seq, strings_seq) = compare_seqs(keys_a, keys_b, level + 1)
        strings = " " * level + \
            "Keys are different:\n" + strings_seq
    else:
        strings = ""
    for key in keys_a:
        if a[key] != b[key]:
            equal = False
            (equal_seq, strings_seq) = compare_items (a[key], b[key],
                                                      level + 1)
            strings = strings + " " * level + \
                "Value for %s is different:\n" % (str(key),) + strings_seq
    return (equal, strings)
        

def print_comparison (a, b, func):
    """Compare two variables using func.

    func has a signaturee (a, b, level), and returns (equal, strings).
    It is assumed that the appripriate func for the types of a and b
    is used.

    Parameters
    ----------

    a: any
       First variable to compare
    b: any
       Second variable to compare
    func: {compare_items | comparte_seqs | compare_dicts}
       Function used for the comparison

    """

    (equal, strings) = func (a, b)
    print "Comparing:"
    print a
    print b
    print strings


def equalJSON (jsonA, jsonB, details = False):
    """Compare two json strings
    
    Returns a boolean with the result of the comparison.

    It can also provide some details on the differences.

    Parameters
    ----------

    jsonA: string
       First string to compare
    jsonB: string
       Second string to compare
    details: boolean (Default: False)
       Provide details about the comparison

    Returns
    -------

    Boolean

    """

    a = loads(jsonA)
    b = loads(jsonB)
    if a != b:
        if details:
            print_comparison (a, b, compare_items)
        return False
    else:
        return True


def equalJSON_file (json, file_name, details = False):
    """Compare a JSON string with the content of a file.

    Returns a boolean with the result of the comparison.
    It can also provide some details on the differences.

    Parameters
    ----------

    json: string
       JSON string to compare
    file_name: string
       Name of file to compare
    details: boolean (Default: False)
       Provide details about the comparison

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


if __name__ == "__main__":

    list_a = ["hola", "adios", 4, 5]
    list_b = ["hola", "adios", 4]
    list_c = ["hola", "adis", 4, 5]
    list_d = [3, 4, 6]
    list_e = [3, 4, 7]
    list_f = ["hola", "adios", list_d, 5]
    list_g = ["hola", "adios", list_d, 4]
    list_h = ["hola", "adios", list_e, 5]
    dict_a = {"first": "Primero",
              "second": "Segundo"}
    dict_b = {"first": "Primero",
              "second": "Segundón"}
    dict_c = {"first": "Primero",
              "second": "Segundón",
              3: "Third"}
    print_comparison (list_a, list_b, compare_seqs)
    print_comparison (list_c, list_b, compare_seqs)
    print_comparison (list_f, list_g, compare_seqs)
    print_comparison (list_f, list_h, compare_seqs)
    print_comparison (list_f, list_h, compare_items)
    print_comparison (dict_a, dict_b, compare_dicts)
    print_comparison (dict_a, dict_c, compare_items)
    equalJSON (dict_a, dict_c, details = True)
    
