# GrimoireLib Testing

For running the GrimoireLib tests, some databases have to be deployed in a MySQL instance. The dumps corresponding to these databases are in the db directory. To deploy them, you can follow a manual procedure, run a shell script or run a (slightly more flexible) Python script. See below for details.

Once the databases are deployed, install R vizgrimoire library in the directory. For that, VizGrimoireUtils should be cloned from GitHub:

    git clone https://github.com/VizGrimoire/VizGrimoireUtils.git

Assuming VizGrimoireUtils was cloned in the parent directory, for installing the R vizgrimoire library (in the GrimoireLib base directory), a directory for storing it and its dependencies has to be created. Then, dependencies are installed from CRAN, using the R shell. Finally, grimoirelibR is installed from source:

    mkdir r-lib
    R
    [in the R shell]
    install.packages("RMySQL", lib="r-lib")
    install.packages("rjson", lib="r-lib")
    install.packages("RColorBrewer", lib="r-lib")
    install.packages("ggplot2", lib="r-lib")
    install.packages("optparse", lib="r-lib")
    install.packages("ISOweek", lib="r-lib")
    install.packages("rgl", lib="r-lib")
    install.packages("zoo", lib="r-lib")
    quit()
    [Back in the regular shell]
    R CMD INSTALL -l r-lib ../VizGrimoireUtils/grimoirelibR

It is also important to set up a Python path with the directories where rpy2 is installed (if it is not installed in the standard directories), where GrimoireLib is installed (".." from the testing directory), and where vizgrimoire is installed ("../vizgrimoire" from the testing directory):

    export PYTHONPATH=/path/to/dir-with-rpy2/:..:../vizgrimoire:$PYTHONPATH

Then execute in testing directory:

    ./test_data_source_api.py

## Shell script for creating testing databases

In the testing directory, run

    recreate-test-dbs.sh

The script assumes user "root" has access to the database without password.

## Python script for creating databases

In the testing directory, run

    python feed_dumps.py --user user --passwd XXX

Other options are available, use "--help" for more information.

## Cleaning up the testing databases

If you want to clean all dbs (assuming mysql user is "root", without a password):

    echo "drop database cp_bicho_GrimoireLibTests" | mysql -u root
    echo "drop database cp_cvsanaly_GrimoireLibTests" | mysql -u root
    echo "drop database cp_downloads_GrimoireLibTests" | mysql -u root
    echo "drop database cp_gerrit_GrimoireLibTests" | mysql -u root
    echo "drop database cp_irc_GrimoireLibTests" | mysql -u root
    echo "drop database cp_mediawiki_GrimoireLibTests" | mysql -u root
    echo "drop database cp_mlstats_GrimoireLibTests" | mysql -u root
    echo "drop database cp_pullpo_GrimoireLibTests" | mysql -u root
    echo "drop database cp_releases_GrimoireLibTests" | mysql -u root
    echo "drop database cp_sibyl_GrimoireLibTests" | mysql -u root
    echo "drop database cp_sortinghat_GrimoireLibTests" | mysql -u root
