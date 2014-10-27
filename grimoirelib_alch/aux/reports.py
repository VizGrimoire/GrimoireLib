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
## Common code to easy the production of reports.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from os.path import join
from jsonpickle import encode, set_encoder_options
import codecs

def produce_json (filename, data, compact = True):
    """Produce JSON content (data) into a file (filename).

    Parameters
    ----------

    filename: string
       Name of file to write the content.
    data: any
       Content to write in JSON format. It has to be ready to pack using
       jsonpickle.encode.

    """

    if compact:
        # Produce compact JSON output
        set_encoder_options('json', separators=(',', ': '),
                            ensure_ascii=False,
                            encoding="utf8")
    else:
        # Produce pretty JSON output
        set_encoder_options('json', sort_keys=True, indent=4,
                            separators=(',', ': '),
                            ensure_ascii=False,
                            encoding="utf8")
    data_json = encode(data, unpicklable=False)
    with codecs.open(filename, "w", "utf-8") as file:
        file.write(data_json)


def create_report (report_files, destdir):
    """Create report, by producing a collection of JSON files

    Parameters
    ----------

    report_files: dictionary
       Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.
    destdir: str
       Name of the destination directory to write all JSON files

    """

    for file in report_files:
        print "Producing file: ", join (destdir, file)
        produce_json (join (destdir, file), report_files[file])

def add_report (report, to_add):
    """Add new files (with their data) to report.

    Adds new_files (which is a dictorionay in report format)
    to report, and returns the result.

    Parameters
    ----------

    report: dictionary
       Base report Keys are the names of JSON files to produce, values are the
       data to include in those JSON files.
    to_add: dictionary
       Report to add. Same format as report. Keys in to_add will
       replace the same keys in report, or just be added to it.
    """

    for file in to_add:
        report[file] = to_add[file]
    return report
