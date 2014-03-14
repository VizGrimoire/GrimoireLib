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
## Auxiliary.R
##
## Auxiliary code for the classes in the package
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>
##
## Note: this file should be (alphabetically) the first one in the list
##  of files with R source code for the package. It seems that files are
##  run in alphabetic order, and this one includes facilities needed by
##  the rest. An alternative would be to use a "Collate" field in the
##  DESCRIPTION file.
##


##
## Penalize scientific notation by 10 chars
##
options(scipen=10)

##
## Database-related classes & functions
##

library(RMySQL)
##
## Connect to the database and prepare...
##

SetDBChannel <- function (user=NULL, password=NULL, database,
                          host="127.0.0.1", port=3306,
                          group=NULL) {
  if (is.null(group)) {
    mychannel <<- dbConnect(MySQL(), user=user, password=password,
                            db=database, host=host, port=port)
  } else {
    mychannel <<- dbConnect(MySQL(), group=group,
                            db=database)
  }    
  dbGetQuery(mychannel, "SET NAMES 'utf8'")
}

##
## Close connection to the database
##

CloseDBChannel <- function () {
  dbDisconnect(mychannel)
}

##
## Find out kind of repository (bugzilla, launchpad, etc.) and
##  store it in common configuration list
##
FindoutRepoKind <- function () {
  q <- new ("Query", sql = "SELECT name FROM supported_trackers")
  kind <- run(q)$name[1]
  return (kind)
}

##
## toTextDate: convert two vectors with integers for year, month into
##  a vector with text
##
## Example: 2012, 2 -> Feb 2012
##
toTextDate <- function (year, month) {
  abb.months <- c('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')
  text <- paste (abb.months[month], as.character(year))
  return (text)
}

##
## GetYear
##
## Get the year of a datetime object
##
## Year as four digits (eg: 2012)
##
GetYear <- function (time) {
  return (1900 + as.POSIXlt(time)$year)
}

##
## GetMonth
##
## Get the month of a datetime object
##
## Month as an integer 0:11 (eg: Jan is 0)
##
GetMonth <- function (time) {
  return (as.POSIXlt(time)$mon)
}

##
## GetWeek
##
## Get the week of a datetime object
##
## Week of the year as decimal number (00–53) as defined in ISO 8601. 
## If the week (starting on Monday) containing 1 January has four or 
## more days in the new year, then it is considered week 1. 
## Otherwise, it is the last week of the previous year, and the next week is week 1. 
## (Accepted but ignored on input.)
##
GetWeek  <- function (time) {
    return (format(time, "%V"))
}

## Group daily samples by selected period
completePeriod <- function (data, period, conf) {
        
    if (length(data) == 0) {
        data <- data.frame(id=numeric(0))
    }
    new_data <- completeZeroPeriod(data, period, conf$str_startdate, conf$str_enddate)
    new_data$week <- as.Date(conf$str_startdate) + new_data$id * period
    new_data$date  <- toTextDate(GetYear(new_data$week), GetMonth(new_data$week)+1)
    new_data[is.na(new_data)] <- 0
    new_data <- new_data[order(new_data$id), ]
        
    return (new_data)
}

GetPeriod <- function(period, date) {
    val = NULL
    
    if (!(period %in% c('weeks','months','years'))) 
        stop (paste("WRONG PERIOD", period))
    if (period == "weeks") val = GetWeek(date)
    else if (period == "months") val = GetMonth(date)
    else if (period == "years") val = GetYear(date)
    
    return (val)
}

GetDateText <- function(period, date) {
    val = NULL
    
    if (!(period %in% c('weeks','months','years'))) 
        stop (paste("WRONG PERIOD", period))
    if (period == "weeks") val = strftime(date, "%W %d %b %Y")
    else if (period == "months") val = strftime(date, "%d %b %Y")
    else if (period == "years") val = strftime(date, "%d %b %Y")
    
    return (val)
}

# Normalize JSON for all metrics
completePeriodMulti <- function(data, metrics, period,startdate, enddate) {    
    for (metric in metrics) {        
        metric_data <- data.frame(id=data$id,metric=data[[metric]])
        colnames(metric_data)[2]<-metric
        print(metric_data)      
        metric_data <- completePeriod2(metric_data, period, 
                conf$str_startdate, conf$str_enddate)
        if (!(exists('new_data'))) new_data <- metric_data
        else new_data <- merge(new_data, metric_data, all = TRUE)
    }
    print(new_data)
    return(new_data)    
}


completeZeroPeriodIdsYears <- function (data, start, end) {    
    last = end$year - start$year  + 1   
    samples <- list('id'=c(0:(last-1)))    
    
    new_date = start
    new_date$mday = 1
    new_date$mon = 0
    for (i in 1:last) {
        # convert to Date to remove DST from start of month
        samples$unixtime[i] = toString(as.numeric(as.POSIXlt(as.Date(new_date))))
        samples$date[i]=format(new_date, "%b %Y")
        samples$year[i]=(1900+new_date$year)*12
        new_date$year = new_date$year + 1
    }
    completedata <- merge (data, samples, all=TRUE)
    completedata[is.na(completedata)] <- 0        
    return(completedata)    
}


completeZeroPeriodIdsMonths <- function (data, start, end) {
    samples <- GetMonthsBetween(start, end)
    completedata <- merge (data, samples, all=TRUE)
    completedata[is.na(completedata)] <- 0        
    return(completedata)    
}

completeZeroPeriodIdsWeeks <- function(data, start, end) {
  # this function fills with 0's those weeks with no data between
  # start and end for data. An initial sample is filled with all
  # information necessary and later merged with data.

  samples <- GetWeeksBetween(start, end)
  completedata <- merge(data, samples, all=TRUE)
  completedata[is.na(completedata)] <- 0
  return(completedata)
}


#######################
# Code to count weeks as done by 
# MySQL using the function yearweek
#######################
library(ISOweek)
#Example of diff: number of weeks between 2010-05-27 and 2013-07-23 = 166. 

getISOWEEKYear <- function(date){
  #expected a date like '2013-12-01'
  isoweekdate <- date2ISOweek(date)
  year_week <- strsplit(isoweekdate, '-')
  year <- year_week[[1]][1]
  return (year)
}

getISOWEEKWeek <- function(date){
  #expected a date like '2013-12-01'
  isoweekdate <- date2ISOweek(date)
  year_week <- strsplit(isoweekdate, '-')
  week <- year_week[[1]][2]
  week <- strsplit(week, 'W')[[1]][2]
  return(week)
  
}

getMaxWeekYear <- function(year){
  #this will provide the maximum number of weeks for a given
  #year
  iso_year = year + 1
  final_date = ""
  day = 31
  while (iso_year > year){
    final_date = paste(year, "-12-", day, sep="")
    iso_year <- getISOWEEKYear(final_date)
    day = day - 1
  }
  
  max_week = getISOWEEKWeek(final_date)
  
  return (max_week)
}

diffISOWeekTime <- function(initdate, enddate){
  #diffweeks for 2013-07-23 and 2010-05-27
  #using typical difftime, there are 164 weeks, 
  #but there should be 166 if this is compared to the
  #yearweek function in mysql group.
  
  inityear = as.numeric(getISOWEEKYear(initdate))
  initweek = as.numeric(getISOWEEKWeek(initdate))
  endyear = as.numeric(getISOWEEKYear(enddate))
  endweek = as.numeric(getISOWEEKWeek(enddate))
  
  diffweeks = 0
  if (inityear == endyear){
    diffweeks = endweek - initweek + 1
  } else if (endyear > inityear) {
    for (i in inityear:endyear){
      if (i == inityear){
        #init of the loop
        diffweeks = diffweeks + as.numeric(getMaxWeekYear(i)) - initweek + 1
      } else if (i == endyear){
        # end of the loop
        diffweeks = diffweeks + endweek
      } else {
        # any year between inityear and endyear
        diffweeks = diffweeks + as.numeric(getMaxWeekYear(i))
      }
    }  
  } else {
    print ("Error, enddate < initdate")
  }

  return (diffweeks)  
}



# Work in seconds as a future investment
completeZeroPeriodIdsDays <- function (data, start, end) {        
    # units should be one of “auto”, “secs”, “mins”, “hours”, “days”, “weeks”
    last = ceiling (difftime(end, start,units=period))               
    samples <- list('id'=c(0:(last-1))) 
    lastdate = start
    start_dst = start$isdst
    dst = start_dst
    dst_offset_hour = 0
    hour.secs = 60*60
    day.secs = hour.secs*24
    for (i in 1:last) {        
        unixtime = as.numeric(start)+((i-1)*day.secs)
        new_date = as.POSIXlt(unixtime,origin="1970-01-01") 
        if (new_date$isdst != dst) {
            dst = new_date$isdst            
            if (dst == start_dst) dst_offset_hour = 0
            else if (start_dst == 0) dst_offset_hour = -hour.secs
            else if (start_dst == 1) dst_offset_hour = hour.secs
        }
        unixtime = unixtime + dst_offset_hour
        lastdate = as.POSIXlt(unixtime, origin="1970-01-01")
        samples$unixtime[i] = toString(unixtime)
        # samples$datedbg[i]=format(lastdate,"%H:%M %d-%m-%y")
        samples$date[i]=format(lastdate, "%b %Y")
    }
    completedata <- merge (data, samples, all=TRUE)
    completedata[is.na(completedata)] <- 0
    return (completedata)
}

completeZeroPeriodIds <- function (data, period, startdate, enddate){           
    start = as.POSIXlt(startdate)
    end = as.POSIXlt(enddate)    
    if (period == "days") {
        return (completeZeroPeriodIdsDays(data, start, end))
    }    
    else if (period == "weeks") {
        return (completeZeroPeriodIdsWeeks(data, start, end))
    }
    else if (period == "months") {
        return (completeZeroPeriodIdsMonths(data, start, end))
    }
    else if (period == "years") {
        return (completeZeroPeriodIdsYears(data, start, end))
    } 
    else {
        stop (paste("Unknow period", period))
    } 
    
}

completePeriodIds <- function (data, period, conf) {    
    if (length(data) == 0) {
        #data is initialized, although nrow(data) is still 0
        data <- data.frame(id=numeric(0))
    }
    new_data <- completeZeroPeriodIds(data, period, conf$str_startdate, conf$str_enddate)
    new_data[is.na(new_data)] <- 0
    new_data <- new_data[order(new_data$id), ]    
    return (new_data)
}

GetMonthsBetween <- function(start, end, extra=FALSE){
    ## Returns a list of months with unixtime, string label and number of month
    ## If extra is true, the last month returned is one month ahead!
    ## From Jan13th to Feb26th it returns (Jan 1st, Feb 1st and March 1st),
    ## unixtime and the label of month/year
    start_month = ((1900+start$year)*12)+start$mon+1
    end_month =  ((1900+end$year)*12)+end$mon+1
    last = end_month - start_month + 1

    if (extra == TRUE){
        last <- last + 1 #simpler than adding a month to as.Date
    }

    samples <- list('id'=c(0:(last-1)))
    new_date = start
    new_date$mday = 1
    for (i in 1:last) {
        # convert to Date to remove DST from start of month
        samples$unixtime[i] = toString(as.numeric(as.POSIXlt(as.Date(new_date))))
        samples$date[i]=format(new_date, "%b %Y")
        samples$month[i]=((1900+new_date$year)*12)+new_date$mon+1
        new_date$mon = new_date$mon + 1
    }
    return(samples)
}

GetWeeksBetween <- function(start, end, extra=FALSE){
    # number of total weeks (those are natural weeks starting on Monday)
    # This should behave in the same way as the yearweek function in MYSQL does
    # With this approach, periods of 9 days may imply up to three weeks
    # (Sunday plus full week plus Monday).

    if (extra == TRUE){
        end <- as.Date(end) + 7
    }

    totalWeeks = diffISOWeekTime(start, end)

    inityear = as.numeric(getISOWEEKYear(start))
    initweek = as.numeric(getISOWEEKWeek(start))
    endyear = as.numeric(getISOWEEKYear(end))
    endweek = as.numeric(getISOWEEKWeek(end))

    samples <- list('id' = c(1:totalWeeks))
    cont = 1
    for (i in inityear:endyear){
        #
        for (j in 1:getMaxWeekYear(i)){
            if (inityear == endyear) {
                if ((j >= initweek) && (j <= endweek)){
                    year = as.character(i)
                    extra = ""
                    if (j<10) extra = "0"
                    week = paste(extra, as.character(j), sep="")
                    isoweekdate = paste(year, "-W", week, "-1", sep="")
                    normal_date = as.POSIXlt(as.Date(ISOweek2date(isoweekdate)))

                    samples$unixtime[cont] = toString(as.numeric(normal_date))
                    samples$date[cont] = format(normal_date, "%b %Y")
                    samples$week[cont] = paste(year, week, sep="")
                    cont = cont + 1
                }

            } else {
                if ((i == inityear && j >= initweek) || (i == endyear && j <= endweek) || (i > inityear && i< endyear)){
                    # same code as above, to be refactored...
                    year = as.character(i)
                    extra = ""
                    if (j<10) extra = "0"
                    week = paste(extra, as.character(j), sep="")
                    isoweekdate = paste(year, "-W", week, "-1", sep="")
                    normal_date = as.POSIXlt(as.Date(ISOweek2date(isoweekdate)))

                    samples$unixtime[cont] = toString(as.numeric(normal_date))
                    samples$date[cont] = format(normal_date, "%b %Y")
                    samples$week[cont] = paste(year, week, sep="")
                    cont = cont + 1
                }
            }
        }
    }
    return(samples)
}

GetDates <- function(init_date, days) {
    # This functions returns an array with three dates
    # First: init_date
    # Second: init_date - days
    # Third: init_date - days - days
    enddate = gsub("'", "", init_date)

    enddate = as.Date(enddate)
    startdate = enddate - days
    prevdate = enddate - days - days

    chardates <- c(paste("'", as.character(enddate),"'", sep=""),
                   paste("'", as.character(startdate), "'", sep=""),
                   paste("'", as.character(prevdate), "'", sep=""))
    return (chardates)
}

GetPercentageDiff <- function(value1, value2){
    # This function returns whe % diff between value 1 and value 2.
    # The difference could be positive or negative, but the returned value
    # is always > 0

    percentage = 0

    if (is.na(value1) || is.na(value2)) return (NA)

    if (value1 < value2){
        diff = value2 - value1
        percentage = as.integer((diff/abs(value1)) * 100)
    }
    if (value1 > value2){
        percentage = as.integer((1-(value2/value1)) * 100)
    }
    return(percentage)
}
##
## Generic JSON function for using it in hierarchies of objects that need it
##
setGeneric (
 name= "JSON",
 def=function(.Object,...){standardGeneric("JSON")}
 )

##
## Generic CSV function for using it in hierarchies of objects that need it
##
setGeneric (
  name= "CSV",
  def=function(.Object,...){standardGeneric("CSV")}
  )

##
## Generic Plot function for using it in hierarchies of objects that need it
##
setGeneric (
  name= "Plot",
  def=function(.Object,...){standardGeneric("Plot")}
  )

##
## Generic PlotCharts function for using it in hierarchies of objects
##  that need it
##
setGeneric (
  name= "PlotCharts",
  def=function(.Object,...){standardGeneric("PlotCharts")}
  )

##
## Generic PlotShares function for using it in hierarchies of objects
##  that need it
##
setGeneric (
  name= "PlotShares",
  def=function(.Object,...){standardGeneric("PlotShares")}
  )

##
## Generic PlotDist function for using it in hierarchies of objects that need it
##
setGeneric (
 name= "PlotDist",
 def=function(object,...){standardGeneric("PlotDist")}
 )

##
## Generic RegionTZ function for using it in hierarchies of objects that need it
##
setGeneric (
 name= "RegionTZ",
 def=function(.Object,...){standardGeneric("RegionTZ")}
 )


##
## Code for producing JSON files suitable for vizGrimoire.JS
##
## All of this should still be re-coded as classes, but is here
## now for convenience, as a first step to support vizGrimoire.JS
##
library(rjson)

##
## Create a JSON file with some R object
##
createJSON <- function (data, filename) {
   sink(filename)
   cat(toJSON(data))
   sink()
}


library(zoo) 

RollMean<-function(serie,s,l)
{ 
    #serie;data where you want to calculate rollmeans.
    #s<l; Periods of time.
    #function; draws rollmeans for two different periods of time. 
    ms<-rollapply(serie,s,mean)
    ml<-rollapply(serie,l,mean)
    w<-rep(0,l-1)
    v<-rep(0,s-1)
    mms<-c(v,ms)
    mml<-c(w,ml)
    scale<-c(min(mms):max(mms))
    #plot(mml, type="l", col="blue") 
    #lines(mms, type="l", col="red")
    means<-data.frame(mms,mml)
    return(means)
}

DiffRoll<-function(serie,s,l)
{
#serie=data s=time_period l=time_period s<l  
#This function gives the difference between short and long rollmean.
k<-l-s
ms<-rollapply(serie,s,mean)
ml<-rollapply(serie,l,mean)
short<-ms[-c(1:k)]
metric<-short-ml
central<-rep(0,length(metric))
#plot(metric, type="l", xlab="weeks", ylab="Short mean-Long mean", main="Rollmean signals")
#lines(central, type="l")
end<-data.frame(metric,central)
return(end)
}


ExpoAv<-function(serie,s,l)
#This function gives a type of moving average that is similar to a simple moving average, except that more weight is given to the latest data.
#serie= data to apply function 
#s= short period of time to apply moving average
#l= long period of time to apply moving average
{ alphas=2/(1+s)
 
  alphal=2/(1+l)
  
  short<-rollapply(serie,s,mean)
  long<-rollapply(serie,l,mean)
  
   getEMA2<-function(x,alpha){
    v<-vector()
    for (i in 1:length(x)){
      if (i==1){
        v[i]<-x[i]
               }
      else{
        v[i]<-alpha*x[i]+(1-alpha)*v[i-1]
          }
    }
    
  return(v)
}
  
  AS<-getEMA2(short,alphas)
  AL<-getEMA2(long,alphal)

  #plot(serie, type="l", col="black", lty=2,ylab="Commits", xlab="weeks", main="EMA")
  #lines(AL,type="l",col="blue")
  #lines(AS,type="l", col="red")
  #legend("topleft",col=c("red","blue","black"),lty=c(1,1,2) ,legend=c("Short","Long","commits"))
  w<-rep(0,l-1)
  v<-rep(0,s-1)
  shortA<-c(v,short)
  longA<-c(w,long)
  ex_means<-data.frame(shortA,longA)
  return(ex_means)
}



Histogram<-function(field,title)
{#This function gives distributions 
n_cl = floor(sqrt(length(field)))

p_cor<-seq(min(field),max(field),length=n_cl+1)

hist(field,breaks=p_cor,col="steelblue4",freq=FALSE, main=paste("Histogram of",title))
}


remove_outliers <- function(x) 
#This function omits outliers
#x=vector 
    {
    qnt <- quantile(x, probs=c(.25, .75))
    H <- 1.5 * IQR(x)
    y <- x
    y[x < (qnt[1] - H)] <- NA
    y[x > (qnt[2] + H)] <- NA
    return(y)
  }


BBollinger<-function(serie,s,confi)
#This functions gives Bands of Bollinger of a moving average.
#serie= data (We assume Normal distribution)
#s=period of time to apply moving average 
#confi=[0,1] level of confidence.
	{alpha<-(1-confi)/2
  
 	 rollmean<-rollapply(serie,s,mean)
  
 	 bbsup<-rollmean+abs(qnorm(alpha))*rollapply(serie,s,sd)
  
 	 bbinf<-rollmean-abs(qnorm(alpha))*rollapply(serie,s,sd)
  
#plot(c(0:max(bbsup)), col="white" , xlim=c(0,length(serie)) , ylab="commits", xlab="weeks", main="Bollinger Bands") 
#lines(bbsup,type="l",col="red")
#lines(rollmean,type="l",col="green")
#lines(bbinf,type="l",col="red")
#lines(serie, type="l", col="black", lty=2)
#legend("topleft",col=c("red","green","black"),lty=c(1,1,2),legend=c("Bollinger Bands","Rollmean","Commits"), bty="n", cex=0.8)
 	 end<-data.frame(rollmean,bbsup,bbinf)
	return(end)
	}

source('R/SCM.R')
source('R/MLS.R')
source('R/ITS.R')
