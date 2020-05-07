#Imports
import pandas as pd
import numpy as np

#Python Standard Libs Imports
import json
import urllib2
import sys
from datetime import datetime
from os.path import isfile, join, splitext
from glob import glob
import os
import time
#from distributed import Executor, hdfs

#Constants
MIN_NUM_ARGS = 6

#Functions
def printUsage():
	print "Script Usage:"
	print "spark-submit %s <file-date> <od-matrix-folderpath> <buste-data-folderpath> <otp-server-url> <results-folderpath>" % (sys.argv[0])


#Basic Functions

def advance_od_matrix_start_time(od_matrix,extra_seconds):
    return od_matrix.assign(o_datetime = lambda x: pd.to_datetime(x['date'] + ' ' + x['o_timestamp']), 
	                    d_datetime = lambda x: pd.to_datetime(x['date'] + ' ' + x['timestamp'])) \
	            .assign(executed_duration = lambda x: (x['d_datetime'] - x['o_datetime']) / pd.Timedelta(minutes=1),
	                    o_base_datetime = lambda x: pd.to_datetime(x['o_datetime']) - pd.Timedelta(minutes=2))

def clean_buste_data(buste_data):
    return buste_data[["date","route","busCode","tripNum","stopPointId","timestamp"]] \
                        .dropna(subset=["date","route","busCode","tripNum","stopPointId","timestamp"]) \
                        .drop_duplicates(subset=['date','route','busCode','tripNum','stopPointId']) \
                        .assign(route = lambda x: pd.to_numeric(x['route'], errors='coerce'),
                                timestamp = lambda x: pd.to_datetime(x['date'] + ' ' + x['timestamp'], format='%Y_%m_%d %H:%M:%S', errors='coerce')) \
                        .assign(date = lambda x: x['date'].str.replace('_','-'))

#OTP Functions

def get_router_id(query_date):
    INTERMEDIATE_OTP_DATE = datetime.strptime("2017-06-30", "%Y-%m-%d")
    
    router_id = ''
    date_timestamp = datetime.strptime(query_date, "%Y-%m-%d")   
    
    if (date_timestamp <= INTERMEDIATE_OTP_DATE):
        return 'ctba-2017-1'
    else:
        return 'ctba-2017-2'

def get_otp_itineraries(otp_url,o_lat,o_lon,d_lat,d_lon,date,time,verbose=False):
    otp_http_request = 'routers/{}/plan?fromPlace={},{}&toPlace={},{}&mode=TRANSIT,WALK&date={}&time={}'
	
    router_id = get_router_id(date)
    otp_request_url = otp_url + otp_http_request.format(router_id,o_lat,o_lon,d_lat,d_lon,date,time)
    if verbose:
        print otp_request_url
    return json.loads(urllib2.urlopen(otp_request_url).read())

def get_otp_suggested_trips(od_matrix,otp_url):
    trips_otp_response = {}
    counter = 0
    for index, row in od_matrix.iterrows():
        id=long(row['user_trip_id'])
        start_time = row['o_timestamp']
        trip_plan = get_otp_itineraries(otp_url,row['o_shape_lat'], row['o_shape_lon'], row['shapeLat'], row['shapeLon'],row['date'],start_time)
        trips_otp_response[id] = trip_plan
        counter+=1

    return trips_otp_response

def get_otp_scheduled_trips(od_matrix,otp_url):
    trips_otp_response = {}
    counter = 0
    for index, row in od_matrix.iterrows():
        id=long(row['user_trip_id'])
        start_time = row['o_timestamp']
        trip_plan = get_executed_trip_schedule(otp_url,row['o_shape_lat'], row['o_shape_lon'], row['shapeLat'], row['shapeLon'],
                                               row['date'],start_time,row['route'],row['o_stop_id'])
        trips_otp_response[id] = trip_plan
        counter+=1

    return trips_otp_response

def extract_otp_trips_legs(otp_trips):
    trips_legs = []

    for trip in otp_trips.keys():
        if 'plan' in otp_trips[trip]:
            itinerary_id = 1
            for itinerary in otp_trips[trip]['plan']['itineraries']:
                date = otp_trips[trip]['plan']['date']/1000
                leg_id = 1
                for leg in itinerary['legs']:
                    route = leg['route'] if leg['route'] != '' else None
                    fromStopId = leg['from']['stopId'].split(':')[1] if leg['mode'] == 'BUS' else None
                    toStopId = leg['to']['stopId'].split(':')[1] if leg['mode'] == 'BUS' else None
                    start_time = long(leg['startTime'] + leg['agencyTimeZoneOffset'])/1000
                    end_time = long(leg['endTime'] + leg['agencyTimeZoneOffset'])/1000
                    duration = (end_time - start_time)/60
                    trips_legs.append((date,trip,itinerary_id,leg_id,start_time,end_time,leg['mode'],route,fromStopId,toStopId, duration))
                    leg_id += 1
                itinerary_id += 1
    return trips_legs

def prepare_otp_legs_df(otp_legs_list):
    labels=['date','user_trip_id','itinerary_id','leg_id','otp_start_time','otp_end_time','mode','route','from_stop_id','to_stop_id','otp_duration_mins']
    return pd.DataFrame.from_records(data=otp_legs_list, columns=labels) \
                    .assign(date = lambda x: pd.to_datetime(x['date'],unit='s').dt.strftime('%Y-%m-%d'),
                            otp_duration_mins = lambda x : (x['otp_end_time'] - x['otp_start_time'])/60,
                            route = lambda x : pd.to_numeric(x['route'],errors='coerce'),
                            from_stop_id = lambda x : pd.to_numeric(x['from_stop_id'],errors='coerce'),
                            to_stop_id = lambda x : pd.to_numeric(x['to_stop_id'],errors='coerce')) \
                    .assign(otp_start_time = lambda x : pd.to_datetime(x['otp_start_time'], unit='s'),
                            otp_end_time = lambda x : pd.to_datetime(x['otp_end_time'], unit='s')) \
                    .sort_values(by=['date','user_trip_id','itinerary_id','otp_start_time'])

def get_executed_trip_schedule(otp_url,o_lat,o_lon,d_lat,d_lon,date,time,route,start_stop_id,verbose=False):
    DEF_AGENCY_NAME = 'URBS'
    DEF_AGENCY_ID = 1
    otp_http_request = 'routers/{}/plan?fromPlace={},{}&toPlace={},{}&mode=TRANSIT,WALK&date={}&time={}&numItineraries=1&preferredRoutes={}_{}&startTransitStopId={}_{}&maxWalkingDistance=150&maxTransfers=0'
    
    router_id = get_router_id(date)
    otp_request_url = otp_url + otp_http_request.format(router_id,o_lat,o_lon,d_lat,d_lon,date,time,DEF_AGENCY_NAME,route,DEF_AGENCY_ID,start_stop_id)
    if verbose:
        print otp_request_url
    return json.loads(urllib2.urlopen(otp_request_url).read())

#Analysis Functions

def find_otp_bus_legs_actual_start_time(otp_legs_df,clean_bus_trips_df):
    legs_buste_match = otp_legs_df.assign(stopPointId = otp_legs_df['from_stop_id']) \
            .merge(clean_bus_trips_df, on=['date','route','stopPointId'], how='inner') \
            .dropna(subset=['timestamp']) \
            .assign(timediff = lambda x: np.absolute(x['timestamp'] - x['otp_start_time'])) \
            .drop('otp_duration_mins', axis=1)
    
    earliest_legs_start_times = legs_buste_match.groupby(['date','user_trip_id','itinerary_id','route','from_stop_id']) \
                                .timediff.min().reset_index()
        
    legs_st_time = legs_buste_match.merge(earliest_legs_start_times, on=['date','user_trip_id','itinerary_id','route','from_stop_id','timediff'], how='inner') \
                [['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','otp_start_time','timestamp','to_stop_id','otp_end_time']] \
                .rename(index=str, columns={'timestamp':'from_timestamp'}) \
                .assign(route = lambda x: pd.to_numeric(x['route']))
    
    return legs_st_time

def find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips):
    legs_buste_match = otp_legs_st.assign(stopPointId = otp_legs_st['to_stop_id']) \
            .merge(clean_bus_trips_data, on=['date','route','busCode','tripNum','stopPointId'], how='inner') \
            .dropna(subset=['timestamp']) \
            .assign(timediff = lambda x: np.absolute(x['timestamp'] - x['otp_end_time'])) \
                    .rename(index=str,columns={'timestamp':'to_timestamp'}) \
                    .sort_values(by=['date','route','to_stop_id','timediff'])
    return legs_buste_match

def clean_otp_legs_actual_time_df(otp_legs_st_end_df):
    clean_legs_time = otp_legs_start_end[['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','from_timestamp','to_stop_id','to_timestamp']] \
                .assign(actual_duration_mins = ((otp_legs_start_end['to_timestamp'] - otp_legs_start_end['from_timestamp'])/pd.Timedelta(minutes=1))) \
                .sort_values(by=['date','user_trip_id','itinerary_id','leg_id'])
            
    clean_legs_time = clean_legs_time[clean_legs_time['actual_duration_mins'] > 0]
    
    return clean_legs_time

def combine_otp_suggestions_with_bus_legs_actual_time(otp_suggestions,bus_legs_actual_time):
    matched_legs_suggestions = otp_suggestions \
                .merge(clean_otp_legs_actual_time, on=['date','user_trip_id','itinerary_id','leg_id', 'route', 'from_stop_id','to_stop_id'], how='left') \
                .assign(considered_duration_mins = lambda x: np.where(x['mode'] == 'BUS',
                                            x['actual_duration_mins'],
                                            x['otp_duration_mins']),
                        considered_start_time = lambda x: np.where(x['mode'] == 'BUS', 
                                         x['from_timestamp'], 
                                         x['otp_start_time']))
    return  matched_legs_suggestions

def select_itineraries_fully_identified(otp_itineraries_legs):
    itineraries_not_fully_identified = otp_itineraries_legs[(otp_itineraries_legs['mode'] == 'BUS') & (pd.isnull(otp_itineraries_legs['busCode']))] \
                                        [['date','user_trip_id','itinerary_id']] \
                                        .drop_duplicates()
    itineraries_fully_identified = pd.concat([otp_itineraries_legs[['date','user_trip_id','itinerary_id']].drop_duplicates(),itineraries_not_fully_identified]) \
                                    .drop_duplicates(keep=False)
    return otp_itineraries_legs.merge(itineraries_fully_identified, on=['date','user_trip_id','itinerary_id'], how='inner')

def filter_bus_legs_matched_to_same_observed_itinerary(otp_matched_legs):
    return otp_matched_legs \
                .merge((clean_legs_actual_time[clean_legs_actual_time['mode'] == 'BUS'] \
                                .drop_duplicates(subset=['date','user_trip_id','from_stop_id','to_stop_id','busCode','otp_start_time']) \
                                [['date','user_trip_id','itinerary_id']].drop_duplicates()), \
                       on=['date','user_trip_id','itinerary_id'], \
                       how='inner')

def filter_itineraries_without_bus_legs(otp_itineraries_legs):
    itineraries_with_bus_legs = otp_itineraries_legs[otp_itineraries_legs['mode'] == 'BUS'] \
                                    [['date','user_trip_id','itinerary_id']] \
                                    .drop_duplicates()
    return otp_itineraries_legs.merge(itineraries_with_bus_legs, on=['date','user_trip_id','itinerary_id'], how='inner')






##########################################################################################################

#Main Code

if __name__ == "__main__":
	if len(sys.argv) < MIN_NUM_ARGS:
		print "Error: Wrong parameter specification!"
		printUsage()
		sys.exit(1)
	
	file_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
	od_matrix_folderpath = sys.argv[2]
	buste_data_folderpath = sys.argv[3]
	otp_server_url = sys.argv[4]
	results_folderpath = sys.argv[5]

	start_time = time.time()
	print "Reading OD-Matrix Data..."
	od_matrix_day_folderpath = od_matrix_folderpath + os.sep + file_date.strftime('%Y_%m_%d') + '_od'
	od_matrix = pd.read_csv(od_matrix_day_folderpath + os.sep + file_date.strftime('%Y_%m_%d') + '_od_od_matrix.csv')

	print "Fixing OD Matrix dates due to bug on date saving on cluster..."
	SECONDS_OFFSET = 10800
	od_matrix.loc[:,'date'] = pd.to_datetime(od_matrix['date'], unit='s').dt.strftime('%Y-%m-%d')
	od_matrix['user_trip_id'] = od_matrix['o_boarding_id']

	print od_matrix[['date']].head(4)

	print "Preprocessing Data..."
	od_matrix = advance_od_matrix_start_time(od_matrix,120)

	#print "Reducing OD Matrix size to 50 trips for testing purposes..."
	#od_matrix = od_matrix.head(50)

	print "Getting OTP suggested itineraries..."
	otp_suggestions = get_otp_suggested_trips(od_matrix,otp_server_url)

	print "Extracting OTP Legs info..."
	otp_legs_df = prepare_otp_legs_df(extract_otp_trips_legs(otp_suggestions))

	print "Writing OTP suggestions to file..."
	otp_legs_df.to_csv(results_folderpath + os.sep + file_date.strftime('%Y_%m_%d') + '_otp_suggestions.csv',index=False)

	otp_suggestions = None

	#otp_legs_df = read_hdfs_folder(sqlContext,results_folderpath+'/trip_plans')

	print "Getting OTP schedule info for executed trips..."
	executed_trips_schedule = get_otp_scheduled_trips(od_matrix,otp_server_url)

	print "Extracting OTP Legs info..."
	executed_trips_schedule_df = prepare_otp_legs_df(extract_otp_trips_legs(executed_trips_schedule)) \
                                [lambda x: x['mode'] == 'BUS'] \
                                .assign(itinerary_id = 0)

	matched_executed_trips = od_matrix \
                            .rename(index=str, columns={'o_stop_id':'from_stop_id', 'stopPointId':'to_stop_id'}) \
                            .merge(executed_trips_schedule_df, 
                                 on=['date','user_trip_id','from_stop_id','to_stop_id'], how='inner') \
                            [['date','user_trip_id','itinerary_id','otp_duration_mins','otp_start_time']] \
                            .rename(index=str, columns={'otp_duration_mins':'planned_duration_mins', 'otp_start_time':'planned_start_time'})

	total_num_itineraries = len(otp_legs_df[['user_trip_id','itinerary_id']].drop_duplicates())
	total_num_legs = len(otp_legs_df)
	num_bus_legs = len(otp_legs_df[otp_legs_df['mode'] == 'BUS'])

	print "Total num itineraries:", total_num_itineraries
	print "Total num legs:", total_num_legs
	print "Total num bus legs:", num_bus_legs, '(', 100*(num_bus_legs/float(total_num_legs)), '%)'

	
	print "Reading BUSTE data..."
	buste_day_folderpath = buste_data_folderpath + os.sep + file_date.strftime('%Y_%m_%d')
	bus_trips_data = pd.read_csv(buste_day_folderpath + os.sep + file_date.strftime('%Y_%m_%d') + '_buste.csv') \
		            .assign(tripNum = lambda x: pd.to_numeric(x['tripNum'], errors='coerce'))

	clean_bus_trips_data = clean_buste_data(bus_trips_data)

	print "Finding OTP Bus Legs Actual Start Times in Bus Trips Data..."
	otp_legs_st = find_otp_bus_legs_actual_start_time(otp_legs_df,clean_bus_trips_data)

	num_bus_legs_st = len(otp_legs_st)
	print "Num Bus Legs whose start was found:", num_bus_legs_st, '(', 100*(num_bus_legs_st/float(num_bus_legs)), '%)'

	print "Finding OTP Bus Legs Actual End Times in Bus Trips Data..."
	otp_legs_start_end = find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips_data)


	clean_otp_legs_actual_time = clean_otp_legs_actual_time_df(otp_legs_start_end)

	num_matched_bus_legs_st = len(clean_otp_legs_actual_time)
	print "Num Bus Legs whose end was found:", num_matched_bus_legs_st, '(', 100*(num_matched_bus_legs_st/float(num_bus_legs)), '%)'

	print "Enriching OTP suggestions legs with actual time data..."
	all_legs_actual_time = combine_otp_suggestions_with_bus_legs_actual_time(otp_legs_df,clean_otp_legs_actual_time)

	print "Filtering out itineraries with bus legs not identified in bus data..."
	clean_legs_actual_time = select_itineraries_fully_identified(all_legs_actual_time)


	print "Filtering out itineraries with matched bus legs duplicated for the same trip..."
	clean_legs_actual_time = filter_bus_legs_matched_to_same_observed_itinerary(clean_legs_actual_time)


	print "Filtering out itineraries without bus legs..."
	clean_legs_actual_time = filter_itineraries_without_bus_legs(clean_legs_actual_time)

	num_itineraries_fully_identified = len(clean_legs_actual_time[['user_trip_id','itinerary_id']].drop_duplicates())
	print "Num Itineraries fully identified in BUSTE data:", num_itineraries_fully_identified, '(', 100*(num_itineraries_fully_identified/float(total_num_itineraries)), '%)'

	
	print "Gathering all trips alternative/executed itineraries info"
	first_boarding_time = clean_legs_actual_time[clean_legs_actual_time['mode'] == 'BUS'] \
		                .groupby(['date', 'user_trip_id', 'itinerary_id']) \
		                .agg({'otp_start_time':'first','considered_start_time':'first'}) \
		                .reset_index() \
		                .rename(index=str,columns={'otp_start_time': 'planned_start_time', 'considered_start_time': 'actual_start_time'}) \
		                .sort_values(['date','user_trip_id','itinerary_id'])        


	user_trips_time_info = od_matrix.rename(index=str,columns={'executed_duration':'exec_duration_mins','o_datetime':'exec_start_time'}) \
                        [['date','user_trip_id','exec_duration_mins','exec_start_time']]

	print "Gathering information from executed itineraries whose schedule was found..."

	executed_legs = user_trips_time_info \
            .merge(matched_executed_trips, on=['date','user_trip_id'], how='left') \
            .assign(actual_duration_mins = lambda x: x['exec_duration_mins'], \
                    actual_start_time = lambda x: x['exec_start_time'],
                    itinerary_id = 0) \
            [['date','user_trip_id','itinerary_id','planned_duration_mins','actual_duration_mins','exec_duration_mins',
                     'planned_start_time','actual_start_time','exec_start_time']]

	print "Gathering information from OTP suggestions..."
            
	matched_otp_legs = clean_legs_actual_time \
                            .groupby(['date', 'user_trip_id', 'itinerary_id']) \
                            .agg({'otp_duration_mins':'sum',
                                  'considered_duration_mins':'sum'}) \
                            .rename(index=str,columns={'otp_duration_mins':'planned_duration_mins',
                                                        'considered_duration_mins':'actual_duration_mins'}) \
                            .reset_index() \
                            .assign(user_trip_id = lambda x : pd.to_numeric(x['user_trip_id']), \
                                    itinerary_id = lambda x : pd.to_numeric(x['itinerary_id'])) \
                            .merge(first_boarding_time, on=['date','user_trip_id','itinerary_id'], how='inner') \
                            .merge(user_trips_time_info, on=['date','user_trip_id'], how='inner') \
                            .sort_values(['date','user_trip_id','itinerary_id']) \
                            [['date','user_trip_id','itinerary_id','planned_duration_mins','actual_duration_mins','exec_duration_mins',
                              'planned_start_time','actual_start_time','exec_start_time']]
                 
	executed_trips_with_sugestions_matched = matched_otp_legs[['user_trip_id']]\
                                            .drop_duplicates()

	print "Assembling trips itineraries dataframe..."

	all_trips_alternatives = pd.concat([matched_otp_legs, executed_legs]) \
                .merge(executed_trips_with_sugestions_matched, on='user_trip_id',how='inner') \
                .sort_values(['date','user_trip_id','itinerary_id'])

	print "Writing Trips Alternatives Data to file..."

	all_trips_alternatives.to_csv(path_or_buf=results_folderpath + os.sep + file_date.strftime('%Y_%m_%d') + '_itinerary.csv',index=False)

	print len(all_trips_alternatives), len(od_matrix), len(executed_trips_with_sugestions_matched)

	print("--- Execution took %s seconds ---" % (time.time() - start_time))

	print "Finishing Script..."
