#!/usr/bin/env python

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
    from scr_pull_metrics import Submitted, Merged, Abandoned, TimeToReview, Pending
    from scr_pull_metrics import Submitters, Closers, Reviewers, People
    from query_builder import PullpoQuery

    db = "liferay_pull_requests"
    db_identities = "cp_cvsanaly_GrimoireLibTests"

    filters = []
    filters.append(MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["repository", "'liferay-portal'"]))
    filters.append(MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["company", "'liferay'"]))
    filters.append(MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["country", "'spain'"]))
    filters.append(MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["domain", "'liferay'"]))
    filters.append(MetricFilters("month", "'2010-09-01'", "'2014-10-01'", ["project", "liferay"]))
    # filters = MetricFilters("month", "'2010-09-01'", "'2014-10-01'", None)
    dbcon = PullpoQuery("root", "", db, db_identities)

    for filter_ in filters:
        submitted = Submitted(dbcon, filter_)
        print submitted.get_agg()
#    merged = Merged(dbcon, filters)
#    abandoned = Abandoned(dbcon, filters)
#    pending = Pending(dbcon, filters)
#    time_to_review = TimeToReview(dbcon, filters)
#    print submitted.get_agg()
#    print submitted.get_ts()
#    print merged.get_agg()
#    print merged.get_ts()
#    print abandoned.get_agg()
#    print abandoned.get_ts()
#    print pending.get_agg()
#    print pending.get_ts()
#    print time_to_review.get_agg()
#    # People
#    submitters = Submitters(dbcon, filters)
#    print submitters.get_agg()
#    print submitters.get_ts()
#    print submitters.get_list(None, 0)
#    closers = Closers(dbcon, filters)
#    print closers.get_agg()
#    print closers.get_list(None, 0)
#    # print closers.get_ts()
#    reviewers = Reviewers(dbcon, filters)
#    print reviewers.get_agg()
#    print reviewers.get_list(None, 0)
#    # print reviewers.get_ts()
#    people = People(dbcon, filters)
#    print people.get_agg()
#    # print people.get_ts()
#    print people.get_list(None, 0)