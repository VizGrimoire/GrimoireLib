import os
import sys

def init_env():
    grimoirelib = os.path.join("..","vizgrimoire")
    metricslib = os.path.join("..","vizgrimoire","metrics")
    studieslib = os.path.join("..","vizgrimoire","analysis")
    alchemy = os.path.join("..")
    for dir in [grimoirelib,metricslib,studieslib,alchemy]:
        sys.path.append(dir)

    # env vars for R
    os.environ["LANG"] = ""
    os.environ["R_LIBS"] = "../../r-lib"

if __name__ == '__main__':

    init_env()
    from GrimoireUtils import getPeriod, read_main_conf, createJSON
    from metrics_filter import MetricFilters
    from query_builder import SCRQuery
    from report import Report
    from scr_metrics import Pending

    db = "cp_gerrit_GrimoireLibTests"
    db_identities = "cp_cvsanaly_GrimoireLibTests"

    filters = MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["repository", "review.openstack.org_openstack/nova"])
    dbcon = SCRQuery("root", "", db, db_identities)

    pending = Pending(dbcon, filters)
    print pending.get_ts()
