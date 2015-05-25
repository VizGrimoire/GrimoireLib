#!/usr/bin/env python

# Copyright (C) 2014 Bitergia
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Authors:
#     Daniel Izquierdo Cortazar <dizquierdo@bitergia.com>

# Mailing lists main topics study

import vizgrimoire.GrimoireUtils
import vizgrimoire.GrimoireSQL
from vizgrimoire.GrimoireSQL import ExecuteQuery

class Email(object):
    """This class contains the main attributes of an email
    """

    def __init__(self, message_id, i_db):
        self.message_id = message_id
        self.i_db = i_db # Identities database
        self.subject = None # Email subject
        self.body = None # Email body
        self.date = None # Email sending date
        self.url = None # Domain of the archive
        self._buildEmail() # Constructor

    def _buildEmail(self):
        # This method retrieves items of information of a given
        # email, specified by its email id.

        query = """
                select distinct m.message_ID,
                       m.subject,
                       m.message_body,
                       m.first_date,
                       pro.name as initiator_name,
                       u.uuid as initiator_id,
                       m.mailing_list_url as url
                from messages m,
                     messages_people mp,
                     people_uidentities pup,
                     %s.uidentities u,
                     %s.profiles pro
                where m.message_ID = '%s' and
                      m.message_ID = mp.message_id and
                      mp.type_of_recipient = 'From' and
                      mp.email_address = pup.people_id and
                      pup.uuid = u.uuid and
                      pup.uuid = pro.uuid
                limit 1
                """  % (self.i_db, self.i_db, self.message_id)
        # WARNING: There may appear in some cases repeated emails.
        # This may be because the same email was sent to different
        # mailing lists. Forcing the query to 1 row, allows to 
        # avoid this issue till we understand why this behaviour
        results = ExecuteQuery(query)

        self.subject = results["subject"]
        self.body = results["message_body"]
        self.date = results["first_date"]
        self.initiator_name = results["initiator_name"]
        self.initiator_id = results["initiator_id"]
        self.url = results["url"]


class Threads(object):
    """This class contains the analysis of the mailing list from the point
       of view of threads. The main topics are those with the longest,
       the most crowded or the thread with the most verbose emails.
    """

    def __init__ (self, initdate, enddate, i_db):
        self.initdate = initdate # initial date of analysis
        self.enddate = enddate  # final date of analysis
        self.i_db = i_db # identities database
        self.list_message_id = [] # list of messages id
        self.list_is_response_of = [] #list of 'father' messages
        self.threads = {} # General structure, keys = root message_id,
                          # values = list of messages in that thread
        self.crowded = None # the thread with most people participating
        self.longest = None # the thread with the longest queue of emails
        self.verbose = None # the thread with the most verbose emails.

        self._init_threads()

    def _build_threads (self, message_id):
        # Constructor of threads.

        sons = []
        messages = []
        if message_id not in self.list_is_response_of:
            # this a leaf of the tree!
            return []

        else:
            cont = 0
            for msg in self.list_is_response_of:
                #Retrieving all of the sons of a given msg
                if msg == message_id:
                    sons.append(self.list_message_id[cont])
                cont = cont + 1
            for msg in sons:
                messages.extend([msg])
                messages.extend(self._build_threads(msg))


        return messages

    def _init_threads(self):
        # Returns dictionary of message_id threads. Each key contains a list
        # of emails associated to that thread (not ordered).

        # Retrieving all of the messages. 
        query = """
                select DISTINCT message_ID, is_response_of
                from messages
                where first_date >= %s and first_date < %s
                """ % (self.initdate, self.enddate)
        list_messages = ExecuteQuery(query)
        self.list_message_id = list_messages["message_ID"]
        self.list_is_response_of = list_messages["is_response_of"]

        messages = {}
        for message_id in self.list_message_id:
            # Looking for messages in the thread
            index = self.list_message_id.index(message_id)

            # Only analyzing those whose is_response_of is None, 
            # those are the message 'root' of each thread.
            if self.list_is_response_of[index] is None:
                messages[message_id] = self._build_threads(message_id)
                # Adding the root message to the list in first place
                messages[message_id].insert(0, message_id)

        self.threads = messages

    def crowdedThread (self):
        # Returns the most crowded thread.
        # This is defined as the thread with the highest number of different
        # participants
        if self.crowded == None:
            # variable was not initialize
            pass

    def topCrowdedThread(self, numTop):
        # Returns list ordered by the longest thread

        top_threads = [] # [(message_id, number of different upeople_id), (...,...), ...]

        for thread in self.threads.values():
            # this loop counts number of different people
            # in each of the threads and provides a 
            # dictionary with root message_id as each of the keys
            # and a list of upeople_id as the value.
            # Sets were considered as an option, but it implies that
            # we may find with a higher probability equal sets, what
            # would provide incorrect sets to their correspondant message_id
            # when ordering them (at least using this algorithm).
            # So, not using sets, and manual order of the lists is done 
            people = set([])
            for message in thread:
                query = """
                        select distinct pup.uuid as upeople_id
                        from messages m,
                             messages_people mp,
                             people_uidentities pup
                        where m.message_ID = '%s' and
                              m.message_ID = mp.message_id and
                              mp.type_of_recipient = 'From' and
                              mp.email_address = pup.people_id
                        """ % (message)
                result = ExecuteQuery(query)
                upeople_id = result["upeople_id"]
                people.add(upeople_id)
            top_threads.append((message, len(people)))

        sorted_threads = sorted(top_threads, key=lambda thread: thread[1], reverse = True)

        top_threads_emails = []
        for top in sorted_threads[:numTop]:
            # Create a list of emails
            message_id = top[0]
            email = Email(message_id, self.i_db)
            top_threads_emails.append((email, top[1]))
        return top_threads_emails

    def longestThread (self):
        # Returns the longest thread
        if self.longest == None:
            # variable was not initialize
            self.longest = ""
            longest = 0
            for message_id in self.threads.keys():
                if len(self.threads[message_id]) > longest:
                    longest = len(self.threads[message_id])
                    self.longest = message_id

        return Email(self.longest, self.i_db)

    def topLongestThread(self, numTop):
        numTop = int(numTop)
        # Returns list ordered by the longest threads
        top_threads = []
        top_root_msgs = []
        top_threads_emails = []

        # Retrieving the lists of threads
        values = self.threads.values()
        # sorted(values, lambda x,y: 1 if len(x)>len(y) else -1 if len(x)<len(y) else 0)
        values = sorted(values, key = len, reverse = True)

        # Checks if numTop is higher than len of the list
        if numTop > len(values):
            top_threads = values
        else:
            top_threads = values[0:numTop]

        for thread in top_threads:
            # the root message is the first of the list 
            # (the rest of them are not ordered)
            top_root_msgs.append(thread[0])

        for message_id in top_root_msgs:
            # Create a list of emails
            email = Email(message_id, self.i_db)
            top_threads_emails.append(email)

        return top_threads_emails

    def verboseThread (self):
        # TODO: at some point these numbers should be calculated when
        # retrieving the initial list of message_id, is_response_of values
        # Returns the most verbose thread (the biggest emails)
        if self.verbose == None:
            # variable was not initialize
            self.verbose = ""
            current_len = 0
            # iterating through the root messages
            for message_id in self.threads.keys():
                total_len_bodies = 0 # len of all of the body messages
                # iterating through each of the messages of the thread
                for msg in self.threads[message_id]:
                    query = """
                            select length(message_body) as length
                            from messages
                            where message_ID = '%s'
                            """ % (msg)
                    result = ExecuteQuery(query)
                    length = int(result["length"])
                    total_len_bodies = total_len_bodies + length
                    if total_len_bodies > current_len:
                        # New bigger thread found
                        self.verbose = message_id
                        current_len = total_len_bodies
        return Email(self.verbose, self.i_db)

    def threads (self):
        # Returns the whole data structure
        return self.threads

    def numThreads (self):
        # Returns number of threads
        return len(self.threads)

    def lenThread(self, message_id):
        # Returns the number of message in a given thread
        # Each thread is identified by the message_id of the 
        # root message
        return len(self.threads[message_id])

if __name__ == '__main__':
    GrimoireSQL.SetDBChannel (database = "openstack_mls", user="root", password="")
    main_topics = Threads("'2012-01-01'", "'2014-01-01'", "openstack_scm")
    print main_topics.topCrowdedThread(10)

