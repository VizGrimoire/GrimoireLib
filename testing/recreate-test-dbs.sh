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
echo "drop database cp_projects_GrimoireLibTests" | mysql -u root


echo Creating testing dbs
echo "CREATE DATABASE cp_cvsanaly_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_bicho_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_gerrit_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_mlstats_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_irc_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_mediawiki_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_downloads_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_releases_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_sibyl_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_pullpo_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_sortinghat_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root
echo "CREATE DATABASE cp_projects_GrimoireLibTests CHARACTER SET utf8" |  mysql -u root

echo Uncompressing testing dbs dumps
7zr -y x irc.mysql.7z
7zr -y x mlstats.mysql.7z
7zr -y x mediawiki.mysql.7z
7zr -y x gerrit.mysql.7z
7zr -y x cvsanaly.mysql.7z
7zr -y x bicho.mysql.7z
7zr -y x downloads.mysql.7z
7zr -y x releases.mysql.7z
7zr -y x sibyl.mysql.7z
7zr -y x pullpo.mysql.7z
7zr -y x sortinghat.mysql.7z
7zr -y x projects.mysql.7z

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
mysql -u root cp_projects_GrimoireLibTests < projects.mysql

cd ..
