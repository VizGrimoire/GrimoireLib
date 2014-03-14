## Copyright (C) 2012, 2013 Bitergia
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
## This file is a part of the vizGrimoire R package
##  (an R library for the MetricsGrimoire and vizGrimoire systems)
##
## ITS.R
##
## Queries for ITS data analysis
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>
##   Luis Cañas-Díaz <lcanas@bitergia.com>
#################
# Micro studies
#################

EvolBMIIndex <- function(period, startdate, enddate, identities_db, type_analysis, closed_condition){
    #Metric based on chapter 4.3.1
    #Metrics and Models in Software Quality Engineering by Stephen H. Kan

    #This will fail if dataframes have different lenght (to be fixe)
    closed = EvolIssuesClosed(period, startdate, enddate, identities_db, type_analysis, closed_condition)
    opened = EvolIssuesOpened(period, startdate, enddate, identities_db, type_analysis)
    evol_bmi = (closed$closed / opened$opened) * 100

    closed$closers <- NULL
    opened$openers <- NULL

    data = merge(closed, opened, ALL=TRUE)
    data = data.frame(data, evol_bmi)
    return (data)
}




BuildWeekDate <- function(date){
       return(paste(getISOWEEKYear(date), getISOWEEKWeek(date), sep=""))
}

GetEvolBacklogTickets <- function (period, startdate, enddate, statuses, name.logtable, filter="") {
    options(stringsAsFactors = FALSE) # avoid merge factors for Python processing
    # Return backlog of tickets in the statuses passed as parameter
    q <- paste("SELECT DISTINCT issue_id, status, date FROM ",name.logtable," ", filter ," ORDER BY date ASC")
    query <- new("Query", sql = q)
    res <- run(query)

    pending.tickets <- data.frame()
    start = as.POSIXlt(gsub("'", "", startdate))
    end = as.POSIXlt(gsub("'", "", enddate))

    if (period == "month") {
        samples <- GetMonthsBetween(start, end, extra=TRUE)
        pending.tickets <- CountBacklogTickets(samples, res, statuses, period)
        # FIXME: month_unix is wrong. Just exists for compatibility. Remove!
        colnames(pending.tickets) <- c('month_unix', 'pending_tickets', 'month')
        posixdates = as.POSIXlt(as.numeric(pending.tickets$month_unix), origin="1970-01-01")
        dates = as.Date(posixdates)
        dates = as.numeric(format(dates, "%Y"))*12 + as.numeric(format(dates, "%m"))
        pending.tickets$month_unix = dates
    }
    else if (period == "week"){
        samples <- GetWeeksBetween(start, end, extra=TRUE)
        pending.tickets <- CountBacklogTickets(samples, res, statuses, period)
        colnames(pending.tickets) <- c('week_unix', 'pending_tickets')
        posixdates = as.POSIXlt(as.numeric(pending.tickets$week_unix), origin="1970-01-01")
        dates = as.Date(posixdates)
        #It's needed in this case to call a function to build the correct
        #yearweek value according to how this is done in MySQL
        dates = lapply(dates, BuildWeekDate)
        dates = as.numeric(dates)
        pending.tickets$week = dates
    }

    return(pending.tickets)
}


CountBacklogTickets <- function(samples, res, statuses, period){
    # return number of tickets in status = statuses per period of time
    #
    # Warning: heavy algorithm, it could be improved if the backlog is
    # calculated backwards and the data is reduced in every iteration
    #
    # Fixme: it is needed to check if there are more that a status for
    # an issue at the same time
    #
    backlog_tickets = data.frame()
    periods <- length(samples$unixtime)
    for (p in (1:periods)){

        if ( p == periods){
            break
        }

        date_unixtime <- samples$unixtime[p]
        next_unixtime_str <- samples$unixtime[p+1]

        next_date <- as.POSIXlt(as.numeric(next_unixtime_str), origin="1970-01-01")
        #print(paste("[" , date() , "] date_unixtime = ",date_unixtime, " next_date = ", next_date)) # debug mode?

        resfilter <- subset(res,res$date < next_date)

        if (nrow(resfilter) > 0){
            maxs <- aggregate(date ~ issue_id, data = resfilter, FUN = max)
            resultado <- merge(maxs, resfilter)
            # filtering by status
            total <- 0
            for (s in statuses){
                aux <- nrow(subset(resultado, resultado$status==s))
                total <- aux + total
            }
            ## print(paste("[" , date() , "] backlog tickets:", total)) # debug mode?
        }else{
            total <- 0
        }
        if (period == "month") {
            aux_df <- data.frame(month=samples$unixtime[p], backlog_tickets = total, month_real=samples$month[p])
        }
        else if (period == "week"){
            aux_df <- data.frame(week=samples$unixtime[p], backlog_tickets = total)
        }
        if (nrow(backlog_tickets)){
            backlog_tickets <- merge(backlog_tickets,aux_df, all=TRUE)
        }else{
            backlog_tickets <- aux_df
        }
    }
    return(backlog_tickets)
}

# Generic function to obtain the current photo of a given issue
# This is based on the field "status" from the issues table

GetCurrentStatus <- function(period, startdate, enddate, identities_db, status){
    # This functions provides  of the status specified by 'status'
    # group by submitted date. Thus, as an example, for those issues 
    # in status = open, it is possible to know when they were submitted
    fields = paste(" count(distinct(id)) as `current_", status, "`", sep="")
    # Fix commented lines in order to make generic this function to any
    # type of granularity (per company, repository, etc)
    #tables = paste(" issues ", GetITSSQLReportFrom(identities_db, list(NA, NA)), sep="")
    tables = " issues "
    #filters = paste(" status = '", status, "' and ", GetITSSQLReportWhere(list(NA, NA)) , sep="")
    filters = paste(" status = '", status, "'", sep="")
    q <- GetSQLPeriod(period,'submitted_on', fields, tables, filters,
            startdate, enddate)
    query <- new ("Query", sql = q)
    data <- run(query)
    return (data)
}

# Demographics
ReportDemographicsAgingITS <- function (enddate, destdir) {
    d <- new ("Demographics","its",6)
    people <- Aging(d)
    people$age <- as.Date(enddate) - as.Date(people$firstdate)
    people$age[people$age < 0 ] <- 0
    aux <- data.frame(people["id"], people["age"])
    new <- list()
    new[['date']] <- enddate
    new[['persons']] <- aux
    createJSON (new, paste(c(destdir, "/its-demographics-aging.json"), collapse=''))
}

ReportDemographicsBirthITS <- function (enddate, destdir) {
    d <- new ("Demographics","its",6)
    newcomers <- Birth(d)
    newcomers$age <- as.Date(enddate) - as.Date(newcomers$firstdate)
    newcomers$age[newcomers$age < 0 ] <- 0
    aux <- data.frame(newcomers["id"], newcomers["age"])
    new <- list()
    new[['date']] <- enddate
    new[['persons']] <- aux
    createJSON (new, paste(c(destdir, "/its-demographics-birth.json"), collapse=''))
}

# Time to close
ReportTimeToCloseITS <- function (backend, destdir) {
    if (backend == 'bugzilla' ||
                    backend == 'allura' ||
                    backend == 'jira' ||
                    backend == 'launchpad') {
        ## Quantiles
        ## Which quantiles we're interested in
        quantiles_spec = c(.99,.95,.5,.25)

        ## Closed tickets: time ticket was open, first closed, time-to-first-close
        closed <- new ("ITSTicketsTimes")

        ## Yearly quantiles of time to fix (minutes)
        events.tofix <- new ("TimedEvents",
                            closed$open, closed$tofix %/% 60)
        quantiles <- QuantilizeYears (events.tofix, quantiles_spec)
        JSON(quantiles, paste(c(destdir,'/its-quantiles-year-time_to_fix_min.json'), collapse=''))

        ## Monthly quantiles of time to fix (hours)
        events.tofix.hours <- new ("TimedEvents",
                            closed$open, closed$tofix %/% 3600)
        quantiles.month <- QuantilizeMonths (events.tofix.hours, quantiles_spec)
        JSON(quantiles.month, paste(c(destdir,'/its-quantiles-month-time_to_fix_hour.json'), collapse=''))

        ## Changed tickets: time ticket was attended, last move
        changed <- new ("ITSTicketsChangesTimes")
        ## Yearly quantiles of time to attention (minutes)
        events.toatt <- new ("TimedEvents",
                            changed$open, changed$toattention %/% 60)
        quantiles <- QuantilizeYears (events.tofix, quantiles_spec)
        JSON(quantiles, paste(c(destdir,'/its-quantiles-year-time_to_attention_min.json'), collapse=''))
    }
}

MarkovChain<-function()
{
    #warning: this function needs some more attention...
    # some variables at the end are not used
    q<-paste("select distinct(new_value) as value
                                  from changes 
                                  where field like '%status%'")

    query <- new ("Query", sql = q)
    status <- run(query)

    print(status)
    T<-status[order(status$value),]
    T1<-gsub("'", "", T)

    print(T1)
    new_value<-function(old)
    {
        q<-paste("select old_value, new_value, count(*) as issue
                                          from changes 
                                          where field like '%status%' and
                        old_value like '%", old , "%' 
                                          group by old_value, new_value;", sep="")

        query <- new ("Query", sql = q)
        table <- run(query)
        f<-table$issue/sum(table$issue)
        x<-cbind(table,f)
        x1<-gsub("'", "",x$new_value)
        x[,2]<-x1

        i<-0
        all<-0
        end<-NULL

        for( i in 1:length(T1)){
            if(is.element(T1[i],x$new_value)){
                i<-i+1
            }
            else{
                c<-data.frame(old_value=0,new_value=T1[i],issue=0,f=0)
                x<-rbind(x,c)
                i<-i+1
            }
        }
        good<-x[order(x$new_value),]
        return(good)
    }

    j<-0
    all<-c()
    markov_result = list()

    for( j in 1:length(T1)) {
        v<-new_value(T1[j])
        markov_result[[T1[j]]] <- v
        good<-v[order(v$new_value),]
        g<-good$f
        all<-c(all,g)
        j<-j+1
    }
    #MARKOV<-matrix(all,ncol=12,nrow=12,byrow=TRUE)
    
    #print(MARKOV)
    #colnames(MARKOV)<-v$new_value
    #rownames(MARKOV)<-v$new_value
    #return(MARKOV)
    return(markov_result)
}

ReportMarkovChain <- function(destdir) {
    options(stringsAsFactors = FALSE) # avoid merge factors for toJSON 
    markov <- MarkovChain()
    createJSON (markov, paste(c(destdir,"/its-markov.json"), collapse=''))
}