# Recreate all testing dbs from scratch

cd db

echo Removing current testing dbs
echo "drop database cp_cvsanaly_GrimoireLibTests" | mysql -u root
echo "drop database cp_bicho_GrimoireLibTests" | mysql -u root
echo "drop database cp_gerrit_GrimoireLibTests" | mysql -u root
echo "drop database cp_mlstats_GrimoireLibTests" | mysql -u root
echo "drop database cp_irc_GrimoireLibTests" | mysql -u root
echo "drop database cp_mediawiki_GrimoireLibTests" | mysql -u root
echo "drop database cp_downloads_GrimoireLibTests" | mysql -u root
echo "drop database cp_releases_GrimoireLibTests" | mysql -u root
echo "drop database cp_sibyl_GrimoireLibTests" | mysql -u root
echo "drop database cp_pullpo_GrimoireLibTests" | mysql -u root
echo "drop database cp_sortinghat_GrimoireLibTests" | mysql -u root


echo Creating testing dbs
mysqladmin -u root create  cp_cvsanaly_GrimoireLibTests
mysqladmin -u root create  cp_bicho_GrimoireLibTests
mysqladmin -u root create  cp_gerrit_GrimoireLibTests
mysqladmin -u root create  cp_mlstats_GrimoireLibTests
mysqladmin -u root create  cp_irc_GrimoireLibTests
mysqladmin -u root create  cp_mediawiki_GrimoireLibTests
mysqladmin -u root create  cp_downloads_GrimoireLibTests
mysqladmin -u root create  cp_releases_GrimoireLibTests
mysqladmin -u root create  cp_sibyl_GrimoireLibTests
mysqladmin -u root create  cp_pullpo_GrimoireLibTests
mysqladmin -u root create  cp_sortinghat_GrimoireLibTests

echo Uncompressing testing dbs dumps
7zr -y x irc.mysql.7z
7zr -y x mailing_lists.mysql.7z
7zr -y x mediawiki.mysql.7z
7zr -y x reviews.mysql.7z
7zr -y x source_code.mysql.7z
7zr -y x tickets.mysql.7z
7zr -y x downloads.mysql.7z
7zr -y x releases.mysql.7z
7zr -y x sibyl.mysql.7z
7zr -y x pullpo.mysql.7z
7zr -y x sortinghat.mysql.7z

echo Loading testing dbs dumps
mysql -u root cp_cvsanaly_GrimoireLibTests < source_code.mysql
mysql -u root cp_bicho_GrimoireLibTests < tickets.mysql
mysql -u root cp_gerrit_GrimoireLibTests < reviews.mysql
mysql -u root cp_mlstats_GrimoireLibTests < mailing_lists.mysql
mysql -u root cp_irc_GrimoireLibTests < irc.mysql
mysql -u root cp_mediawiki_GrimoireLibTests < mediawiki.mysql
mysql -u root cp_downloads_GrimoireLibTests < downloads.mysql
mysql -u root cp_releases_GrimoireLibTests < releases.mysql
mysql -u root cp_sibyl_GrimoireLibTests < sibyl.mysql
mysql -u root cp_pullpo_GrimoireLibTests < pullpo.mysql
mysql -u root cp_sortinghat_GrimoireLibTests < sortinghat.mysql

cd ..
