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
## AuxiliarySCM.R
##
## Queries for SCM data analysis
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>


from GrimoireSQL import SetDBChannel
from GrimoireUtils import read_options, read_main_conf
import SCM, ITS, MLS, SCR, Mediawiki, IRC

class Report(object):

    @staticmethod
    def _get_config():
        opts = read_options()

        opts.config_file = "../../../conf/main.conf"
        automator = read_main_conf(opts.config_file)

        return automator

    @staticmethod
    def connect_ds(ds):
        opts = read_options()
        automator = Report._get_config()
        db = automator['generic'][ds.get_db_name()]
        SetDBChannel (database=db, user=opts.dbuser, password=opts.dbpassword)

    @staticmethod
    def get_data_sources():
        automator = Report._get_config()
        all_data_sources = [SCM.SCM, ITS.ITS, MLS.MLS, SCR.SCR, Mediawiki.Mediawiki, IRC.IRC]
        data_sources= []

        for ds in all_data_sources:
            if not ds.get_db_name() in automator['generic']: continue
            else: data_sources.append(ds)
        return data_sources