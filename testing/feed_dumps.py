## Python script to creat the databases from dumps

## Tests to be done to speed data restore:
## (echo "SET AUTOCOMMIT=0;";     echo "SET UNIQUE_CHECKS=0;";     echo "SET FOREIGN_KEY_CHECKS=0;"; cat irc.mysql; echo "SET FOREIGN_KEY_CHECKS=1;";     echo "SET UNIQUE_CHECKS=1;";     echo "SET AUTOCOMMIT=1;";     echo "COMMIT;"; ) | mysql -u jgb -pXXX msr_openstack_irc
## show variables like 'bulk%';
## SET GLOBAL bulk_insert_buffer_size = 1024 * 1024 * 512;

import os
import subprocess
import argparse

description = """
Simple script to deploy databases for testing GrimoireLib

Creates MySQL schemas for all databases, and feed them with data from
dumps in the db directory, after uncompressing them.
"""

dumps = {
    "db": ["downloads", "irc", "mailing_lists", "mediawiki",
           "pullpo", "releases", "reviews", "sibyl", "sortinghat",
           "source_code", "tickets",
           ],
    }

def parse_args ():
    """
    Parse command line arguments, returns arguments for MySQL

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("--user", help = "MySQL user")
    parser.add_argument("--passwd", help = "MySQL passwd")
    parser.add_argument("--host", help = "MySQL host")
    parser.add_argument("--port",
                        help = "MySQL port number")
    args = parser.parse_args()
    mysql_args = []
    if args.user:
        mysql_args.extend(["-u", args.user])
    if args.passwd:
        mysql_args.extend(["-p" + args.passwd])
    if args.host:
        mysql_args.extend(["-h", args.host])
    if args.port:
        mysql_args.extend(["-P", args.port, "--protocol=tcp"])
    return mysql_args

for dir in dumps:
    print dir
    mysql_args = parse_args()
    mysql_cmd = ["mysql"] + mysql_args
    for dump in dumps[dir]:
        file = os.path.join (dir, dump + ".mysql.7z")
        database = "cp_" + dump + "_GrimoireLibTests"
        print "Processing:", database, file
        sql_str = "CREATE DATABASE IF NOT EXISTS " + database
        create_cmd = mysql_cmd + ["-e", sql_str]
        print "*** Create:", " ".join(create_cmd)
        subprocess.call(create_cmd)
        uncompress_cmd = ["7z", "x", file, "-so"]
        dump_cmd = mysql_cmd + [database]
        dump_cmd_str = " ".join(uncompress_cmd) + " | " \
            + " ".join(dump_cmd)
        print "*** Uncompress / recover dump:", dump_cmd_str
        subprocess.call (dump_cmd_str, shell=True)
