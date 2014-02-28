#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# This file is a part of the vizGrimoire package
#
## Authors:
## Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>

import logging
import sys
import GrimoireUtils, GrimoireSQL
from GrimoireUtils import dataFrame2Dict, createJSON, completePeriodIds
from optparse import OptionParser
from threads import Threads

def read_options():
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 0.1")
    parser.add_option("-u","--dbuser",
                      action="store",
                      dest="dbuser",
                      default="root",
                      help="Database user")
    parser.add_option("-p","--dbpassword",
                      action="store",
                      dest="dbpassword",
                      default="",
                      help="Database password")
    parser.add_option("-m", "--mls",
                      action="store",
                      dest="dbmls",
                      help="MLS database")
    parser.add_option("-t", "--identities",
                      action="store",
                      dest="dbidentities",
                      help="Identities database")
    parser.add_option("-d", "--initdate",
                      action="store",
                      dest="initdate",
                      help="Identities database")
    parser.add_option("-e", "--enddate",
                      action="store",
                      dest="enddate",
                      help="Identities database")
    (opts, args) = parser.parse_args()

    if len(args) != 0:
        parser.error("Wrong number of arguments")

    if not(opts.dbidentities and opts.dbuser):
        parser.error("--dbuser and --dbidentities are needed")
    return opts


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s')
    logging.info("Starting Studies analysis")
    opts = read_options()
   
    GrimoireSQL.SetDBChannel (database=opts.dbmls, user=opts.dbuser, password=opts.dbpassword)
    main_topics = Threads(opts.initdate, opts.enddate)
    email = main_topics.verboseThread()
    print "The most verbose thread: "
    print """
              message_id: %s
              subject: %s
              date: %s
              body: %s
              """ % (email.message_id, email.subject, email.date, email.body)
    email = main_topics.longestThread()
    print "The longest list: " 
    print """
              message_id: %s
              subject: %s
              date: %s
              body: %s
              """ % (email.message_id, email.subject, email.date, email.body)

    print "Number of threads: "  + str(main_topics.numThreads())
    
    longest_threads = main_topics.topLongestThread(10)
    print "Top longest threads: " 
    for email in longest_threads:
        print """
              message_id: %s
              lenght: %s
              subject: %s
              date: %s
              body: %s
              """ % (email.message_id, main_topics.lenThread(email.message_id), email.subject, email.date, email.body)

