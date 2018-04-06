
# coding: utf-8

# In[58]:


import csv
import time
import os
from collections import defaultdict

#Getting all input files
os.chdir('./input')

#Setting all input files
inactivity_filename = 'inactivity_period.txt'
filename = 'log.csv'
output_filename = 'sessionization.txt'

#Opening inactivity period file and setting inactivity period
inactivity_file = open(inactivity_filename, 'r')
inactivity_period = inactivity_file.read()
inactivity_file.close()
#Unit test on inactivity period to ensure it is an integer between 1 and 86400(1 second to 24 hrs)
try:
    inactivity_period = int(inactivity_period)
    assert inactivity_period<86401
    assert inactivity_period>0
except:
    print("Invalid inactivity period, please ensure the inactivity period is an integer time in seconds from 1 to 86,400.")
    raise

#Initializing storage variables
storage_dictionary = defaultdict(list)
session = []
ip_addresses = set()

#Initializing time
current_time = '00:00:00'

#Opening main csv file 
with open(filename, 'rt') as csvfile: 
    #Opening file
    datareader = csv.reader(csvfile, delimiter = ',')
    
    #Obtaining first row(headers) of the open csv file and separating out the useful parameters, returning an error if 
    #ip, date, time, cik, accession, and extension columns are not detected
    csvheader = next(datareader)
    try:
        ip_column_index = csvheader.index('ip')
        date_column_index = csvheader.index('date')
        time_column_index = csvheader.index('time')
        cik_column_index = csvheader.index('cik')
        accession_column_index = csvheader.index('accession')
        extention_column_index = csvheader.index('extention')
    except:
        print("Header Column ID Value incorrect or missing, please make sure there exist column names for", 
              "ip, date, time, cik, accession, and extension")
        raise

    for row in datareader:
        #Reading data out of each csv column
        ip, date, access_time, cik, accession, extention = row[ip_column_index], row[date_column_index], row[time_column_index], row[cik_column_index], row[accession_column_index], row[extention_column_index]
        #Initializing and resetting ip addresses that have been sessionized and therefore need to be removed
        ip_addresses_toRemove = []
        #Starting analysis of whether data should be sessionized whenever whenever time increases
        if(not access_time==current_time):
            #Looping through all the current ip addresses logged
            for ip_address in ip_addresses:
                #Analyzing time between the last time this ip address made a request and the current time
                last_activity_hr, last_activity_min, last_activity_sec = storage_dictionary[ip_address][-1][1].split(':')
                current_hr, current_min, current_sec = access_time.split(':')
                
                last_activity_in_seconds = int(last_activity_hr)*3600 + int(last_activity_min)*60 + int(last_activity_sec)
                current_activity_in_seconds = int(current_hr)*3600 + int(current_min*60) + int(current_sec)
                activity_time = current_activity_in_seconds-last_activity_in_seconds

                #When the time passed exceeds or equals the inactivity period, move the data to the sessionization list,
                #add it to a set to be removed, and remove it from the dictionary storing row values
                if(activity_time>inactivity_period):
                    #Obtaining start date for this ip
                    start_date = storage_dictionary[ip_address][0][0]
                    
                    #Obtaining start time for this ip
                    start_time = storage_dictionary[ip_address][0][1]
                    
                    #Obtaining end date for this end
                    end_date = storage_dictionary[ip_address][-1][0]
                    
                    #Obtaining end time for this ip
                    end_time = storage_dictionary[ip_address][-1][1]
                    
                    #Calculating the time this ip spent in a session
                    start_hr, start_min, start_sec = start_time.split(':')
                    start_time_in_seconds = int(start_hr)*3600 + int(start_min*60) + int(start_sec)
                    
                    #Adding one because the session time is inclusive
                    session_time = last_activity_in_seconds - start_time_in_seconds + 1
                    
                    #Obtaining the number of documents accessed by calculating number of requests sent by that user 
                    #in this current session
                    doc_number = len(storage_dictionary[ip_address])
                    
                    #Adding to the sessionization list
                    session.append([ip_address,start_date,start_time,end_date,end_time,session_time,doc_number])
                    
                    #Adding ip address for removal in the ip tracking set
                    ip_addresses_toRemove.append(ip_address)
                    
                    #Removing all information for ip for the session above
                    storage_dictionary.pop(ip_address)
                    
            current_time = access_time
        
        #Removing all ip addresses that were sessionized
        if(len(ip_addresses_toRemove)>0):
            for ip_address in ip_addresses_toRemove:
                ip_addresses.discard(ip_address)
        
        #If new ip address detected in row, add the ip address to the ip address set
        if(not ip in ip_addresses):
            ip_addresses.add(ip)
        
        #Append row data into dictionary keyed by ip address as a list
        storage_dictionary[ip].append([date, access_time, cik, accession, extention])

#After reaching the end of the file
#Sessionizing any remaining information in the storage dictionary after the last line of the file
for ip in storage_dictionary:
    #Obtaining start date of the remaining ips
    start_date = storage_dictionary[ip][0][0]
    
    #Obtaining start time of the remaining ips
    start_time = storage_dictionary[ip][0][1]
    
    #Obtaining end date of the remaining ips
    end_date = storage_dictionary[ip][-1][0]
    
    #Obtaining end time of remaining ips
    end_time = storage_dictionary[ip][-1][1]
    last_activity_hr, last_activity_min, last_activity_sec = end_time.split(':')
    last_activity_in_seconds = int(last_activity_hr)*3600 + int(last_activity_min)*60 + int(last_activity_sec)
    
    #Calculating session time using the first time ip address requested anything and the last time in the file 
    start_hr, start_min, start_sec = start_time.split(':')
    start_time_in_seconds = int(start_hr)*3600 + int(start_min*60) + int(start_sec)
    
    #Adding one because the session time is inclusive
    session_time = last_activity_in_seconds - start_time_in_seconds + 1
    
    #Obtaining the number of documents accessed by calculating number of requests sent by that user 
    #in this current session
    doc_number = len(storage_dictionary[ip])
    
    #Appending to the sessionization list
    session.append([ip,start_date,start_time,end_date,end_time,session_time,doc_number])

#Changing directory to output directory
os.chdir('../output')

#Writing output file using data in the session list
file = open(output_filename,'w')
for sess in session:
    sess_string = str(sess[0]) + ',' + str(sess[1]) + ' ' + str(sess[2]) + ',' + str(sess[3]) + ' ' + str(sess[4]) + ',' + str(sess[5]) + ',' + str(sess[6])
    file.write("%s\n" % sess_string)
file.close()

#Changing directory back to src
os.chdir('../src')

