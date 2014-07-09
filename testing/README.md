# GrimoireLib Testing

In order to execute the tests you need to create the test dbs in MySQL.

    mysqladmin -u root create  cp_cvsanaly_GrimoireLibTests
    mysqladmin -u root create  cp_bicho_GrimoireLibTests
    mysqladmin -u root create  cp_gerrit_GrimoireLibTests
    mysqladmin -u root create  cp_mlstats_GrimoireLibTests
    mysqladmin -u root create  cp_irc_GrimoireLibTests
    mysqladmin -u root create  cp_mediawiki_GrimoireLibTests
    mysqladmin -u root create  cp_downloads_GrimoireLibTests
    mysqladmin -u root create  cp_releases_GrimoireLibTests
    mysqladmin -u root create  cp_qaforums_GrimoireLibTests

In testing/db execute:

    7zr x irc.mysql.7z
    7zr x mailing_lists.mysql.7z
    7zr x mediawiki.mysql.7z
    7zr x reviews.mysql.7z
    7zr x source_code.mysql.7z
    7zr x tickets.mysql.7z
    7zr x downloads.mysql.7z
    7zr x releases.mysql.7z
    7zr x qaforums.mysql.7z

    mysql -u root cp_cvsanaly_GrimoireLibTests < source_code.mysql
    mysql -u root cp_bicho_GrimoireLibTests < tickets.mysql
    mysql -u root cp_gerrit_GrimoireLibTests < reviews.mysql
    mysql -u root cp_mlstats_GrimoireLibTests < mailing_lists.mysql
    mysql -u root cp_irc_GrimoireLibTests < irc.mysql
    mysql -u root cp_mediawiki_GrimoireLibTests < mediawiki.mysql
    mysql -u root cp_downloads_GrimoireLibTests < downloads.mysql
    mysql -u root cp_releases_GrimoireLibTests < releases.mysql
    mysql -u root cp_qaforums_GrimoireLibTests < qaforums.mysql

Install R environment in root dir:

    mkdir r-lib
    R CMD INSTALL -l r-lib vizgrimoire

Then execute in testing dir:
    ./test_data_source_api.py

If you want to clean all dbs first:

    echo "drop database cp_cvsanaly_GrimoireLibTests" | mysql -u root
    echo "drop database cp_bicho_GrimoireLibTests" | mysql -u root
    echo "drop database cp_gerrit_GrimoireLibTests" | mysql -u root
    echo "drop database cp_mlstats_GrimoireLibTests" | mysql -u root
    echo "drop database cp_irc_GrimoireLibTests" | mysql -u root
    echo "drop database cp_mediawiki_GrimoireLibTests" | mysql -u root
    echo "drop database cp_downloads_GrimoireLibTests" | mysql -u root
    echo "drop database cp_releases_GrimoireLibTests" | mysql -u root
    echo "drop database cp_qaforums_GrimoireLibTests" | mysql -u root
