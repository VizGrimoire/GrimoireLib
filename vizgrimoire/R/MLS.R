## Copyright (C) 2013 Bitergia
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
## Authors:
##   Daniel Izquierdo <dizquierdo@bitergia.com>
##   Alvaro del Castillo <acs@bitergia.com>


ReportDemographicsAgingMLS <- function (enddate, destdir, unique = FALSE) {
    d <- new ("Demographics","mls",6, unique)
    people <- Aging(d)
    people$age <- as.Date(enddate) - as.Date(people$firstdate)
    people$age[people$age < 0 ] <- 0
    aux <- data.frame(people["id"], people["age"])
    new <- list()
    new[['date']] <- enddate
    new[['persons']] <- aux
    createJSON (new, paste(c(destdir, "/mls-demographics-aging.json"), collapse=''))
}

ReportDemographicsBirthMLS <- function (enddate, destdir, unique = FALSE) {
    d <- new ("Demographics","mls",6, unique)
    newcomers <- Birth(d)
    newcomers$age <- as.Date(enddate) - as.Date(newcomers$firstdate)
    newcomers$age[newcomers$age < 0 ] <- 0
    aux <- data.frame(newcomers["id"], newcomers["age"])
    new <- list()
    new[['date']] <- enddate
    new[['persons']] <- aux
    createJSON (new, paste(c(destdir, "/mls-demographics-birth.json"), collapse=''))
}

ReportTimeToAttendMLS <- function (destdir) {
    quantiles_spec = c(.99,.95,.5,.25)
    ## Replied messages: time ticket was submitted, first replied
    replied <- new ("MLSTimes")
    ## Yearly quantiles of time to attention (minutes)
    events.toattend <- new ("TimedEvents",
            replied$submitted_on, replied$toattend %/% 60)
    # print(events.toattend)
    quantiles <- QuantilizeYears (events.toattend, quantiles_spec)
    JSON(quantiles, paste(c(destdir,'/mls-quantiles-year-time_to_attention_min.json'), collapse=''))

    ## Monthly quantiles of time to attention (hours)
    events.toattend.hours <- new ("TimedEvents",
            replied$submitted_on, replied$toattend %/% 3600)
    quantiles.month <- QuantilizeMonths (events.toattend.hours, quantiles_spec)
    JSON(quantiles.month, paste(c(destdir,'/mls-quantiles-month-time_to_attention_hour.json'), collapse=''))
}


