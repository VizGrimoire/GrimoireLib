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
## Miscellanous support for running modules as standalone scripts
## and for testing.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

import sys
from codecs import getwriter

def print_banner (banner):
    """Print a simple banner for a kind of result
   
    Parameters
    ----------

    banner: string
       Text of the banner to print

    """

    print
    print "===================================="
    print banner
    print


def stdout_utf8 ():
    """Set utf8 codec for stdout.

    This is in fact a trick to make the script work with unicode content
    when using pipes (pipes confuse the interpreter, which sets codec to None)
    More info:
    http://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python

    """

    sys.stdout = getwriter('utf8')(sys.stdout)
        


if __name__ == "__main__":

    print_banner("Testing banner")
    stdout_utf8()
    print_banner(u"Apañando el uso de utf8 (test by piping to less)")
