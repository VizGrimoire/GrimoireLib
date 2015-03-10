## Copyright (C) 2012, 2013 Bitergia
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
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

import logging, re

class Filter(object):

    _filters_data = [
                     ["repository","rep","repos"], ["company","com","companies"],
                     ["country","cou","countries"], ["domain","dom","domains"],
                     ["project","prj","projects"], ["tag", "tag", "tags"],
                     ["people","people","people"],
                     ["people2","people2","people2"],
                     ["company+country","com+cou","companies+countries"]
                    ]

    def __init__(self, name, item = None):
        self.name = name
        for filter_data in Filter._filters_data:
            if name in filter_data:
                self.name_short = filter_data[1]
                self.name_plural = filter_data[2]
        self.item = item 

    @staticmethod
    def get_filter_from_plural(plural):
        for data in Filter._filters_data:
            if (data[2] == plural):
                return Filter(data[0])

    def get_name(self):
        return self.name

    def get_name_short(self):
        return self.name_short

    def get_name_plural(self):
        return self.name_plural

    def get_item(self):
        return self.item

    def get_filename (self, ds):
        return ds.get_name()+"-"+self.get_name_plural()+".json"

    def get_evolutionary_filename_all (self, ds):
        return ds.get_name()+"-"+self.get_name_short()+"-all-evolutionary.json"

    def get_evolutionary_filename (self, ds):
        name  = None

        if (self.get_item() is None):
            logging.warn("No item defined in get_evolutionary_filename")
        else:
            selfname_short = self.get_name_short()
            item = self.get_item().replace("/","_").replace("<","__").replace(">","___")
            if re.compile("^\..*").match(item) is not None: item = "_"+item
            name = item+"-"+ds.get_name()+"-"+selfname_short+"-evolutionary.json"

        return name

    def get_static_filename_all (self, ds):
        return ds.get_name()+"-"+self.get_name_short()  +"-all-static.json"

    def get_static_filename (self, ds):
        name  = None

        if (self.get_item() is None):
            logging.warn("No item defined in get_static_filename")
        else:
            selfname_short = self.get_name_short()
            item = self.get_item().replace("/","_").replace("<","__").replace(">","___")
            if re.compile("^\..*").match(item) is not None: item = "_"+item
            name = item+"-"+ds.get_name()+"-"+selfname_short+"-static.json"

        return name

    def get_top_filename (self, ds):
        name  = None

        if (self.get_item() is None):
            logging.warning("No filename for filter top " + self.get_name())
        else:
            item = self.get_item().replace("/","_").replace("<","__").replace(">","___")

            name = item+"-"+ds.get_name()+"-"+self.get_name_short()+"-top-"
            if (ds.get_name() == "scm"):
                name += "authors.json"
            elif (ds.get_name() == "its" or ds.get_name() == "its_1"):
                name += "closers.json"
            elif (ds.get_name() == "mls"): 
                name += "senders.json"
            else:
                logging.warning("No filename for filter top %s %s"
                                % (self.get_name(), self.get_item()))

        return name

    def get_summary_filename(self, ds):
        name = ds.get_name()+"-"

        if (ds.get_name() == "scm"):
            if (self.get_name() == "company"):
                name += "companies-commits-summary.json"
        elif (ds.get_name() == "its" or ds.get_name() == "its_1"):
            if (self.get_name() == "company"):
                name += "closed-companies-summary.json"
        elif (ds.get_name() == "mls"):
            if (self.get_name() == "company"):
                name += "sent-companies-summary.json"
        else:
            logging.warning("No filename for filter summary %s " % (ds.get_name()))
        return name

    def get_type_analysis(self):
        """Old format for filtering"""
        name = self.get_name()
        item = self.get_item()
        if item is not None: item =  "'"+self.get_item()+"'"

        type_analysis = [name, item]
        return type_analysis