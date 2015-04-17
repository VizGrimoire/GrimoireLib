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
## This file is a part of GrimoireLib
##  (an Python library for the MetricsGrimoire and vizGrimoire systems)
##
##
## Authors:
##   Alvaro del Castillo <acs@bitergia.com>

""" Wikimedia specific Metrics for the source code review system """

from datetime import datetime, timedelta
import logging
import MySQLdb
from numpy import median, average
from sets import Set

from vizgrimoire.GrimoireUtils import completePeriodIds, checkListArray, medianAndAvgByPeriod, removeDecimals
from vizgrimoire.metrics.metrics import Metrics
from vizgrimoire.metrics.metrics_filter import MetricFilters
from vizgrimoire.SCR import SCR

class TimeToReviewPendingSCR(Metrics):
    """
        Upload time: last patch uploaded to the review (not all reviews have patches)
    """
    id = "review_time_pending_total"
    name = "Total Review Time Pending"
    desc = "Total time to review for pending reviews"
    data_source = SCR

    def _get_agg_all(self):
        # All items data is returned together

        # Show review and upload time for all pending reviews
        # and just for reviews pending for reviewers
        reviewers_pending = True

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        identities_db = self.db.identities_db
        type_analysis =  self.filters.type_analysis
        bots = []

        # First, we need to group by the filter field the data
        all_items = self.db.get_all_items(self.filters.type_analysis)
        group_field = self.db.get_group_field(all_items)
        id_field = group_field.split('.')[1] # remove table name

        time_to = {"review_time_pending_days_median":[],
                   "review_time_pending_days_avg":[],
                   "review_time_pending_ReviewsWaitingForReviewer_days_median":[],
                   "review_time_pending_ReviewsWaitingForReviewer_days_avg":[],
                   "review_time_pending_upload_days_median":[],
                   "review_time_pending_upload_days_avg":[],
                   "review_time_pending_upload_ReviewsWaitingForReviewer_days_median":[],
                   "review_time_pending_upload_ReviewsWaitingForReviewer_days_avg":[]
                   }

        q = self.db.GetTimeToReviewPendingQuerySQL(self.filters, identities_db, bots)
        ttr_data = self.db.ExecuteQuery(q)
        checkListArray(ttr_data)

        q = self.db.GetTimeToReviewPendingQuerySQL(self.filters, identities_db, bots, reviewers_pending)
        ttr_reviewers_data = self.db.ExecuteQuery(q)
        checkListArray(ttr_reviewers_data)

        q = self.db.GetTimeToReviewPendingQuerySQL (self.filters, identities_db, bots, False, True)
        ttr_upload_data = self.db.ExecuteQuery(q)
        checkListArray(ttr_upload_data)

        # This query is really slow.
        q = self.db.GetTimeToReviewPendingQuerySQL (self.filters, identities_db, bots, reviewers_pending, True)
        ttr_reviewers_upload_data = self.db.ExecuteQuery(q)
        checkListArray(ttr_reviewers_upload_data)

        all_items_ids = []
        for items in [ttr_data,ttr_reviewers_data,ttr_upload_data,ttr_reviewers_upload_data]:
            # Get the list of items and add them to the global list
            all_items_ids = list(Set(items[id_field] + all_items_ids))

        time_to['name'] = all_items_ids

        # Review time
        for item in time_to['name']:
            data_item_revtime = []
            # Get all values for the item
            for i in range(0, len(ttr_data[id_field])):
                if ttr_data[id_field][i] == item:
                    data_item_revtime.append(ttr_data['revtime'][i])

            if (len(data_item_revtime) == 0):
                ttr_median = float("nan")
                ttr_avg = float("nan")
            else:
                ttr_median = median(removeDecimals(data_item_revtime))
                ttr_avg = average(removeDecimals(data_item_revtime))

            time_to["review_time_pending_days_median"].append(ttr_median)
            time_to["review_time_pending_days_avg"].append(ttr_avg)

        # Review time for reviewers
        for item in time_to['name']:
            data_item_revtime = []
            # Get all values for the item
            for i in range(0, len(ttr_reviewers_data[id_field])):
                if ttr_reviewers_data[id_field][i] == item:
                    data_item_revtime.append(ttr_reviewers_data['revtime'][i])

            if (len(data_item_revtime) == 0):
                ttr_reviewers_median = float("nan")
                ttr_reviewers_avg = float("nan")
            else:
                ttr_reviewers_median = median(removeDecimals(data_item_revtime))
                ttr_reviewers_avg = average(removeDecimals(data_item_revtime))

            time_to["review_time_pending_ReviewsWaitingForReviewer_days_median"].append(ttr_reviewers_median)
            time_to["review_time_pending_ReviewsWaitingForReviewer_days_avg"].append(ttr_reviewers_avg)

        # Upload time
        for item in time_to['name']:
            data_item_revtime = []
            # Get all values for the item
            for i in range(0, len(ttr_upload_data[id_field])):
                if ttr_upload_data[id_field][i] == item:
                    data_item_revtime.append(ttr_upload_data['revtime'][i])

            if (len(data_item_revtime) == 0):
                ttr_median_upload = float("nan")
                ttr_avg_upload = float("nan")
            else:
                ttr_median_upload = median(removeDecimals(data_item_revtime))
                ttr_avg_upload = average(removeDecimals(data_item_revtime))

            time_to["review_time_pending_upload_days_median"].append(ttr_median_upload)
            time_to["review_time_pending_upload_days_avg"].append(ttr_median_upload)

        # Upload time for reviewers
        for item in time_to['name']:
            data_item_revtime = []
            # Get all values for the item
            for i in range(0, len(ttr_reviewers_upload_data[id_field])):
                if ttr_reviewers_upload_data[id_field][i] == item:
                    data_item_revtime.append(ttr_reviewers_upload_data['revtime'][i])

            if (len(data_item_revtime) == 0):
                ttr_reviewers_median_upload = float("nan")
                ttr_reviewers_avg_upload = float("nan")
            else:
                ttr_reviewers_median_upload = median(removeDecimals(data_item_revtime))
                ttr_reviewers_avg_upload = average(removeDecimals(data_item_revtime))

            time_to["review_time_pending_upload_ReviewsWaitingForReviewer_days_median"].append(ttr_reviewers_median_upload)
            time_to["review_time_pending_upload_ReviewsWaitingForReviewer_days_avg"].append(ttr_reviewers_avg_upload)

        # In SCR the item field name must be url for repository
        if self.filters.type_analysis[0] == 'repository':
            time_to['url'] = time_to.pop('name')

        return time_to


    def get_agg(self):

        if self.filters.type_analysis and self.filters.type_analysis[1] is None:
            return self._get_agg_all()

        # Show review and upload time for all pending reviews
        # and just for reviews pending for reviewers
        reviewers_pending = True

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        identities_db = self.db.identities_db
        type_analysis =  self.filters.type_analysis
        bots = []

        # Review time
        q = self.db.GetTimeToReviewPendingQuerySQL(self.filters, identities_db, bots)
        data = self.db.ExecuteQuery(q)
        data = data['revtime']
        if (isinstance(data, list) == False): data = [data]
        if (len(data) == 0):
            ttr_median = float("nan")
            ttr_avg = float("nan")
        else:
            ttr_median = median(removeDecimals(data))
            ttr_avg = average(removeDecimals(data))

        # Review time for reviewers
        q = self.db.GetTimeToReviewPendingQuerySQL(self.filters, identities_db, bots, reviewers_pending)
        data = self.db.ExecuteQuery(q)
        data = data['revtime']
        if (isinstance(data, list) == False): data = [data]
        if (len(data) == 0):
            ttr_reviewers_median = float("nan")
            ttr_reviewers_avg = float("nan")
        else:
            ttr_reviewers_median = median(removeDecimals(data))
            ttr_reviewers_avg = average(removeDecimals(data))

        # Upload time
        q = self.db.GetTimeToReviewPendingQuerySQL (self.filters, identities_db, bots, False, True)
        data = self.db.ExecuteQuery(q)
        data = data['revtime']
        if (isinstance(data, list) == False): data = [data]
        if (len(data) == 0):
            ttr_median_upload = float("nan")
            ttr_avg_upload = float("nan")
        else:
            ttr_median_upload = median(removeDecimals(data))
            ttr_avg_upload = average(removeDecimals(data))


        # Upload time for reviewers
        q = self.db.GetTimeToReviewPendingQuerySQL (self.filters, identities_db, bots, reviewers_pending, True)
        data = self.db.ExecuteQuery(q)
        data = data['revtime']
        if (isinstance(data, list) == False): data = [data]
        if (len(data) == 0):
            ttr_reviewers_median_upload = float("nan")
            ttr_reviewers_avg_upload = float("nan")
        else:
            ttr_reviewers_median_upload = median(removeDecimals(data))
            ttr_reviewers_avg_upload = average(removeDecimals(data))


        time_to = {"review_time_pending_days_median":ttr_median,
                   "review_time_pending_days_avg":ttr_avg,
                   "review_time_pending_ReviewsWaitingForReviewer_days_median":ttr_reviewers_median,
                   "review_time_pending_ReviewsWaitingForReviewer_days_avg":ttr_reviewers_avg,
                   "review_time_pending_upload_days_median":ttr_median_upload,
                   "review_time_pending_upload_days_avg":ttr_avg_upload,
                   "review_time_pending_upload_ReviewsWaitingForReviewer_days_median":ttr_reviewers_median_upload,
                   "review_time_pending_upload_ReviewsWaitingForReviewer_days_avg":ttr_reviewers_avg_upload
                   }
        return time_to

    def get_ts(self):
        # Get all reviews pending time for each month and compute the median.
        # Return a list with all the medians for all months

        def get_date_from_month(monthid):
            # month format: year*12+month
            year = (monthid-1) / 12
            month = monthid - year*12
            # We need the last day of the month
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            current = str(year)+"-"+str(month)+"-"+str(last_day)
            return (current)

        # SQL for all, for upload  or for waiting for reviewer reviews
        def get_sql(month, reviewers = False, uploaded = False):
            current = get_date_from_month(month)

            sql_max_patchset = self.db.get_sql_max_patchset_for_reviews (current)
            sql_reviews_reviewed = self.db.get_sql_reviews_reviewed(startdate, current)
            sql_reviews_closed = self.db.get_sql_reviews_closed(startdate, current)

            # List of pending reviews before a date: time from new time and from last upload
            fields  = "TIMESTAMPDIFF(SECOND, submitted_on, '"+current+"')/(24*3600) AS newtime,"
            if (uploaded):
                fields = "TIMESTAMPDIFF(SECOND, ch.changed_on, '"+current+"')/(24*3600) AS uploadtime,"
            fields += " YEAR(i.submitted_on)*12+MONTH(i.submitted_on) as month"

            all_items = self.db.get_all_items(self.filters.type_analysis)
            if all_items:
                group_field = self.db.get_group_field(all_items)
                fields = group_field + ", " + fields

            tables = Set([])
            filters = Set([])

            tables.add("issues i")
            tables.add("people")
            tables.add("issues_ext_gerrit ie")
            if (uploaded):
                tables.add("changes ch")
                tables.add("("+sql_max_patchset+") last_patch")
            tables.union_update(self.db.GetSQLReportFrom(self.filters))
            tables = self.db._get_tables_query(tables)

            filters.add("people.id=i.submitted_by")
            filters.add("ie.issue_id=i.id")
            filters.add("i.id NOT IN ("+ sql_reviews_closed +")")
            if (uploaded):
                filters.add("ch.issue_id = i.id")
                filters.add("i.id = last_patch.issue_id")
                filters.add("ch.old_value = last_patch.maxPatchset")
                filters.add("ch.field = 'Upload'")
            if reviewers:
                filters.add("i.id NOT IN (%s) " % (sql_reviews_reviewed))

            filters.union_update(self.db.GetSQLReportWhere(self.filters))
            filters = self.db._get_filters_query(filters)
            # All reviews before the month: accumulated key point
            filters += " HAVING month<=" + str(month)
            # Not include future submissions for current month analysis
            # We should no need it with the actual SQL which is correct
            if (uploaded):
                filters += " AND uploadtime >= 0"
            else:
                filters += " AND newtime >= 0"

            filters += " ORDER BY i.submitted_on"

            q = self.db.GetSQLGlobal('i.submitted_on', fields, tables, filters,
                                     startdate,enddate)
            return q

        def get_values_median(values):
            if not isinstance(values, list): values = [values]
            values = removeDecimals(values)
            if (len(values) == 0): values = float('nan')
            else: values = median(values)
            return values

        def _get_ts_all():
            """ Get the metrics for all items at the same time """

            all_items = self.db.get_all_items(self.filters.type_analysis)
            group_field = self.db.get_group_field(all_items)
            id_field = group_field.split('.')[1] # remove table name

            metrics = ["review_time_pending_reviews",
                       "review_time_pending_days_acc_median",
                       "review_time_pending_upload_reviews",
                       "review_time_pending_upload_days_acc_median",
                       "review_time_pending_ReviewsWaitingForReviewer_days_acc_median",
                       "review_time_pending_ReviewsWaitingForReviewer_reviews",
                       "review_time_pending_upload_ReviewsWaitingForReviewer_days_acc_median",
                       "review_time_pending_upload_ReviewsWaitingForReviewer_reviews"]

            metrics = ["review_time_pending_reviews",
                       "review_time_pending_days_acc_median",
                       "review_time_pending_upload_reviews",
                       "review_time_pending_upload_days_acc_median",
                       "review_time_pending_ReviewsWaitingForReviewer_days_acc_median",
                       "review_time_pending_ReviewsWaitingForReviewer_reviews",
                       "review_time_pending_upload_ReviewsWaitingForReviewer_days_acc_median",
                       "review_time_pending_upload_ReviewsWaitingForReviewer_reviews"]

            acc_pending_time_median_month = {"month":[],"name":[]} # Used to store each month all items data
            acc_pending_time_median = {"month":[]} # Used to store the final format

            for metric in metrics:
                acc_pending_time_median_month[metric] = []
                acc_pending_time_median[metric] = []

            # months = 2 # to debug

            for i in range(0, months+1):
                # Complete the skeletom of the data dict
                acc_pending_time_median_month["month"].append(start_month+i)
                acc_pending_time_median["month"].append(start_month+i)
                acc_pending_time_median_month["name"].append([])
                for metric in metrics:
                    acc_pending_time_median_month[metric].append([])
                    acc_pending_time_median[metric].append([])

            for i in range(0, months+1):
                # First get all data from SQL
                newtime = self.db.ExecuteQuery(get_sql(start_month+i))
                uploadtime = self.db.ExecuteQuery(get_sql(start_month+i, False, True))
                newtime_rev = self.db.ExecuteQuery(get_sql(start_month+i, True))
                # This is the slow query
                uploadtime_rev = self.db.ExecuteQuery(get_sql(start_month+i, True, True))
                # Build a common list for all items
                all_items_month_ids = []
                # for data_sql in [newtime, uploadtime, newtime_rev, uploadtime_rev]:
                for data_sql in [newtime, uploadtime, newtime_rev, uploadtime_rev]:
                    checkListArray(data_sql)
                    all_items_month_ids = list(Set(data_sql[id_field]+all_items_month_ids))
                acc_pending_time_median_month["name"][i] = all_items_month_ids

                # Now add the data in a common dict for all metrics in this month
                # review time
                for item in all_items_month_ids:
                    data_item = []
                    for j in range(0, len(newtime[id_field])):
                        if newtime[id_field][j] == item:
                            data_item.append(newtime['newtime'][j])
                    values = get_values_median(data_item)
                    # print start_month+i, "newtime", item, values
                    nreviews = len(data_item)
                    acc_pending_time_median_month['review_time_pending_reviews'][i].append(nreviews)
                    acc_pending_time_median_month['review_time_pending_days_acc_median'][i].append(values)

                # upload time
                for item in all_items_month_ids:
                    data_item = []
                    for j in range(0, len(uploadtime[id_field])):
                        if uploadtime[id_field][j] == item:
                            data_item.append(uploadtime['uploadtime'][j])

                    values = get_values_median(data_item)
                    # print start_month+i, "upload", item, values
                    nreviews = len(data_item)
                    acc_pending_time_median_month['review_time_pending_upload_reviews'][i].append(nreviews)
                    acc_pending_time_median_month['review_time_pending_upload_days_acc_median'][i].append(values)
                # review time reviewers
                for item in all_items_month_ids:
                    # Now just for reviews waiting for Reviewer
                    data_item = []
                    for j in range(0, len(newtime_rev[id_field])):
                        if newtime_rev[id_field][j] == item:
                            data_item.append(newtime_rev['newtime'][j])
                    values = get_values_median(data_item)
                    nreviews = len(data_item)
                    acc_pending_time_median_month['review_time_pending_ReviewsWaitingForReviewer_reviews'][i].append(nreviews)
                    acc_pending_time_median_month['review_time_pending_ReviewsWaitingForReviewer_days_acc_median'][i].append(values)

                # upload time reviewers
                for item in all_items_month_ids:
                    data_item = []
                    for j in range(0, len(uploadtime_rev[id_field])):
                        if uploadtime_rev[id_field][j] == item:
                            data_item.append(uploadtime_rev['uploadtime'][j])
                    values = get_values_median(data_item)
                    nreviews = len(data_item)
                    acc_pending_time_median_month['review_time_pending_upload_ReviewsWaitingForReviewer_reviews'][i].append(nreviews)
                    acc_pending_time_median_month['review_time_pending_upload_ReviewsWaitingForReviewer_days_acc_median'][i].append(values)

            # Now we need to consolidate all names in a single list
            all_items = []
            for lnames in acc_pending_time_median_month['name']:
                all_items = list(Set(lnames+all_items))
            # And now time to create the final version that should be completePeriod
            for item in all_items:
                # Add the ts for the item to the final dict
                for i in range(0, months+1):
                    mitems = acc_pending_time_median_month['name'][i]
                    found_item = False
                    for k in range(0, len(mitems)):
                        if mitems[k] == item:
                            # Found the item, get all metrics for this month
                            found_item = True
                            for metric in metrics:
                                item_metric_month_value = acc_pending_time_median_month[metric][i][k]
                                acc_pending_time_median[metric][i].append(item_metric_month_value)
                    if not found_item:
                        for metric in metrics:
                            # 0 reviews, 0 review time
                            acc_pending_time_median[metric][i].append(0)

            # Now we need to completePeriods to add time series fields
            # All the time series are already complete because the way they are built
            # but we miss some time series fields
            ts_fields = ['unixtime','date','month','id']
            ts_aux = {}
            ts_aux['month'] =  acc_pending_time_median['month']
            ts_aux = completePeriodIds(ts_aux, self.filters.period,
                                       self.filters.startdate, self.filters.enddate)
            for field in ts_fields:
                acc_pending_time_median[field] = ts_aux[field]

            # After completing the time series, add the name/url series
            acc_pending_time_median["url"] = all_items

            # And now we need to adjust the format from
            # month:[M1, M2, M3], url:[URL1, URL2. ...], metric:[[URL1_M1,URL2_M1], [URL1_M2, URL2_M2],[URL1_M3...]...]
            # to
            # month:[M1, M2, M3], url:[URL1, URL2. ...], metric:[[URL1_M1, URL1_M2, URL1_M3],[URL2_M1...]...]
            time_to = {}
            for field in acc_pending_time_median:
                if field not in metrics:
                    time_to[field] = acc_pending_time_median[field]
                else:
                    # The new metric field will have an array per item with the time series
                    time_to[field] = []
                    for i in range(0,len(all_items)):
                        time_to[field].append([])
                    for metrics_month in acc_pending_time_median[field]:
                        for j in range(0,len(all_items)):
                            time_to[field][j].append(metrics_month[j])
            return time_to

        startdate = self.filters.startdate
        enddate = self.filters.enddate
        identities_db = self.db.identities_db
        type_analysis =  self.filters.type_analysis
        period = self.filters.period
        bots = []

        start = datetime.strptime(startdate, "'%Y-%m-%d'")
        end = datetime.strptime(enddate, "'%Y-%m-%d'")

        if (period != "month"):
            logging.error("Period not supported in " + self.id  + " " + period)
            return None

        start_month = start.year*12 + start.month
        end_month = end.year*12 + end.month
        months = end_month - start_month


        all_items = self.db.get_all_items(self.filters.type_analysis)

        if all_items:
            return _get_ts_all()

        acc_pending_time_median = {"month":[],
                                   "review_time_pending_reviews":[],
                                   "review_time_pending_days_acc_median":[],
                                   "review_time_pending_upload_reviews":[],
                                   "review_time_pending_upload_days_acc_median":[],
                                   "review_time_pending_ReviewsWaitingForReviewer_days_acc_median":[],
                                   "review_time_pending_ReviewsWaitingForReviewer_reviews":[],
                                   "review_time_pending_upload_ReviewsWaitingForReviewer_days_acc_median":[],
                                   "review_time_pending_upload_ReviewsWaitingForReviewer_reviews":[],}

        for i in range(0, months+1):
            acc_pending_time_median['month'].append(start_month+i)

            reviews = self.db.ExecuteQuery(get_sql(start_month+i))
            values = get_values_median(reviews['newtime'])
            if isinstance(reviews['newtime'], list): nreviews = len(reviews['newtime'])
            else: nreviews = 1 # sure 1?
            acc_pending_time_median['review_time_pending_reviews'].append(nreviews)
            acc_pending_time_median['review_time_pending_days_acc_median'].append(values)
            # upload time
            reviews = self.db.ExecuteQuery(get_sql(start_month+i, False, True))
            values = get_values_median(reviews['uploadtime'])
            if isinstance(reviews['uploadtime'], list): nreviews = len(reviews['uploadtime'])
            else: nreviews = 1
            acc_pending_time_median['review_time_pending_upload_reviews'].append(nreviews)
            acc_pending_time_median['review_time_pending_upload_days_acc_median'].append(values)

            # Now just for reviews waiting for Reviewer
            reviews = self.db.ExecuteQuery(get_sql(start_month+i, True))
            values = get_values_median(reviews['newtime'])
            if isinstance(reviews['newtime'], list): nreviews = len(reviews['newtime'])
            else: nreviews = 1
            acc_pending_time_median['review_time_pending_ReviewsWaitingForReviewer_reviews'].append(nreviews)
            acc_pending_time_median['review_time_pending_ReviewsWaitingForReviewer_days_acc_median'].append(values)

            reviews = self.db.ExecuteQuery(get_sql(start_month+i, True, True))
            values = get_values_median(reviews['uploadtime'])
            if isinstance(reviews['uploadtime'], list): nreviews = len(reviews['uploadtime'])
            else: nreviews = 1
            acc_pending_time_median['review_time_pending_upload_ReviewsWaitingForReviewer_reviews'].append(nreviews)
            acc_pending_time_median['review_time_pending_upload_ReviewsWaitingForReviewer_days_acc_median'].append(values)

        # Normalize values removing NA and converting to 0. Maybe not a good idea.
        for m in acc_pending_time_median.keys():
            for i in range(0,len(acc_pending_time_median[m])):
                acc_pending_time_median[m][i] = float(acc_pending_time_median[m][i])

        return completePeriodIds(acc_pending_time_median, self.filters.period,
                                 self.filters.startdate, self.filters.enddate)
