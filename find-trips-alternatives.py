#Spark Imports
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

#Python Standard Libs Imports
import json
import urllib2
import sys
from datetime import datetime
from os.path import isfile, join, splitext
from glob import glob

#Imports to enable visualizations
#import seaborn as sns
#import matplotlib.pyplot as plt

#Constants
MIN_NUM_ARGS = 7

#Functions
def printUsage():
	print "Script Usage:"
	print "spark-submit %s <initial-date> <final-date> <od-matrix-folderpath> <buste-data-folderpath> <otp-server-url> <results-folderpath>" % (sys.argv[0])


#Basic Functions

def rename_columns(df, list_of_tuples):
    for (old_col, new_col) in list_of_tuples:
        df = df.withColumnRenamed(old_col, new_col)
    return df

def read_folders(path, sqlContext, sc, initial_date, final_date, folder_suffix):
    extension = splitext(path)[1]

    if extension == "":
        path_pattern = path + "/*/part-*"
        if "hdfs" in path:
            URI = sc._gateway.jvm.java.net.URI
            Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
            FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
            Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

            hdfs = "/".join(path_pattern.split("/")[:3])
            dir = "/" + "/".join(path_pattern.split("/")[3:])

            fs = FileSystem.get(URI(hdfs), Configuration())

            status = fs.globStatus(Path(dir))

            files = map(lambda file_status: str(file_status.getPath()), status)

        else:
            files = glob(path_pattern)

        #print initial_date, final_date
        #print datetime.strptime(files[0].split('/')[-2],('%Y_%m_%d' + folder_suffix))

        files = filter(lambda f: initial_date <= datetime.strptime(f.split("/")[-2], ('%Y_%m_%d' + folder_suffix)) <=
                                 final_date, files)
        
        #print len(files)
        #print files
        if folder_suffix == '_od':
            return reduce(lambda df1, df2: df1.unionAll(df2),
                      map(lambda f: read_hdfs_folder(sqlContext,f), files))
        else:
            return reduce(lambda df1, df2: df1.unionAll(df2),
                      map(lambda f: read_buste_data_v3(sqlContext,f), files))
    else:
        return read_file(path, sqlContext)

def read_hdfs_folder(sqlContext, folderpath):
    data_frame = sqlContext.read.csv(folderpath, header=True,
                                     inferSchema=True,nullValue="-")
    return data_frame

def read_buste_data_v3(sqlContext, folderpath):
    data_frame = read_hdfs_folder(sqlContext,folderpath)
    data_frame = data_frame.withColumn("date", F.unix_timestamp(F.col("date"),'yyyy_MM_dd'))
    
    return data_frame

def printdf(df,l=10):
    return df.limit(l).toPandas()

def get_timestamp_in_tz(unixtime_timestamp,ts_format,tz):
    return F.from_utc_timestamp(F.from_unixtime(unixtime_timestamp, ts_format),tz)


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

def get_executed_trip_schedule(otp_url,o_lat,o_lon,d_lat,d_lon,date,time,route,start_stop_id,verbose=False):
    DEF_AGENCY_NAME = 'URBS'
    DEF_AGENCY_ID = 1
    otp_http_request = 'routers/{}/plan?fromPlace={},{}&toPlace={},{}&mode=TRANSIT,WALK&date={}&time={}&numItineraries=1&preferredRoutes={}_{}&startTransitStopId={}_{}&maxWalkDistance=150&maxTransfers=0'
    
    router_id = get_router_id(date)
    otp_request_url = otp_url + otp_http_request.format(router_id,o_lat,o_lon,d_lat,d_lon,date,time,DEF_AGENCY_NAME,route,DEF_AGENCY_ID,start_stop_id)
    if verbose:
        print otp_request_url
    return json.loads(urllib2.urlopen(otp_request_url).read())

def get_otp_suggested_trips(od_matrix,otp_url):
    trips_otp_response = {}
    counter = 0
    for row in od_matrix.collect():
        id=long(row['user_trip_id'])
        start_time = row['o_base_datetime'].split(' ')[1]
        trip_plan = get_otp_itineraries(otp_url,row['o_shape_lat'], row['o_shape_lon'], row['shapeLat'], row['shapeLon'],row['date'],start_time)
        trips_otp_response[id] = trip_plan
        counter+=1

    return trips_otp_response

def get_otp_scheduled_trips(od_matrix,otp_url):
    trips_otp_response = {}
    counter = 0
    for row in od_matrix.collect():
        id=long(row['user_trip_id'])
        start_time = row['o_base_datetime'].split(' ')[1]
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
                    start_time = long(leg['startTime'])/1000
                    end_time = long(leg['endTime'])/1000
                    duration = (end_time - start_time)/60
                    trips_legs.append((date,trip,itinerary_id,leg_id,start_time,end_time,leg['mode'],route,fromStopId,toStopId, duration))
                    leg_id += 1
                itinerary_id += 1
    return trips_legs

def prepare_otp_legs_df(otp_legs_list):
    labels=['date','user_trip_id','itinerary_id','leg_id','otp_start_time','otp_end_time','mode','route','from_stop_id','to_stop_id','otp_duration_mins']
    otp_legs_df = sqlContext.createDataFrame(otp_legs_list, labels) \
                        .withColumn('date',F.from_unixtime(F.col('date'),'yyyy-MM-dd')) \
                        .withColumn('otp_duration_mins',((F.col('otp_end_time') - F.col('otp_start_time'))/60)) \
                        .withColumn('otp_start_time',F.from_unixtime(F.col('otp_start_time'),'yyyy-MM-dd HH:mm:ss').astype('timestamp')) \
                        .withColumn('otp_end_time',F.from_unixtime(F.col('otp_end_time'),'yyyy-MM-dd HH:mm:ss').astype('timestamp')) \
                        .withColumn('route', F.col('route').astype('integer')) \
                        .withColumn('from_stop_id', F.col('from_stop_id').astype('integer')) \
                        .withColumn('to_stop_id', F.col('to_stop_id').astype('integer')) \
                        .orderBy(['date','user_trip_id','itinerary_id','otp_start_time'])

    return otp_legs_df

#Analysis Functions

def advance_od_matrix_start_time(od_matrix,extra_seconds):
    return od_matrix.withColumn('o_datetime', F.concat(F.col('date'), F.lit(' '), F.col('o_timestamp'))) \
                    .withColumn('d_datetime', F.concat(F.col('date'), F.lit(' '), F.col('timestamp'))) \
                    .withColumn('executed_duration', (F.unix_timestamp('d_datetime') - F.unix_timestamp('o_datetime'))/60) \
                    .withColumn('o_base_datetime', F.from_unixtime(F.unix_timestamp(F.col('o_datetime'),'yyyy-MM-dd HH:mm:ss') - extra_seconds, 'yyyy-MM-dd HH:mm:ss')) \

def get_df_stats(df,filtered_df,df_label,filtered_df_label):
    df_size = df.count()
    filtered_df_size = filtered_df.count()
    print "Total", df_label,":", df_size
    print "Total", filtered_df_label, ":", filtered_df_size, "(", 100*(filtered_df_size/float(df_size)), "%)"

def get_filtered_df_stats(filtered_df,full_df_size,filtered_df_label,full_df_label):
    filtered_df_size = filtered_df.count()
    print filtered_df_label, "in Total", full_df_label, ":", filtered_df_size, "(", 100*(filtered_df_size/float(full_df_size)), "%)"

def clean_buste_data(buste_data):
    return buste_data.select(["date","route","busCode","tripNum","stopPointId","timestamp"]) \
        .na.drop(subset=["date","route","busCode","tripNum","stopPointId","timestamp"]) \
        .dropDuplicates(['date','route','busCode','tripNum','stopPointId']) \
        .withColumn('route',F.col('route').astype('float')) \
        .withColumn('date',F.from_unixtime(F.col('date'),'yyyy-MM-dd')) \
        .withColumn('timestamp',F.from_unixtime(F.unix_timestamp(F.concat(F.col('date'),F.lit(' '),F.col('timestamp')), 'yyyy-MM-dd HH:mm:ss')))

def find_otp_bus_legs_actual_start_time(otp_legs_df,clean_bus_trips_df):
    w = Window.partitionBy(['date','user_trip_id','itinerary_id','route','from_stop_id']).orderBy(['timediff'])
    return otp_legs_df \
        .withColumn('stopPointId', F.col('from_stop_id')) \
        .join(clean_bus_trips_df, ['date','route','stopPointId'], how='inner') \
        .na.drop(subset=['timestamp']) \
        .withColumn('timediff',F.abs(F.unix_timestamp(F.col('timestamp')) - F.unix_timestamp(F.col('otp_start_time')))) \
        .drop('otp_duration') \
        .withColumn('rn', F.row_number().over(w)) \
        .where(F.col('rn') == 1) \
        .select(['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','otp_start_time','timestamp','to_stop_id','otp_end_time']) \
        .withColumnRenamed('timestamp','from_timestamp')

def find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips):
    return otp_legs_st \
                .withColumnRenamed('to_stop_id','stopPointId') \
                .join(clean_bus_trips, ['date','route','busCode','tripNum','stopPointId'], how='inner') \
                .na.drop(subset=['timestamp']) \
                .withColumn('timediff',F.abs(F.unix_timestamp(F.col('timestamp')) - F.unix_timestamp(F.col('otp_end_time')))) \
                .withColumnRenamed('timestamp', 'to_timestamp') \
                .withColumnRenamed('stopPointId','to_stop_id') \
                .orderBy(['date','route','stopPointId','timediff'])

def clean_otp_legs_actual_time_df(otp_legs_st_end_df):
    return otp_legs_start_end \
                .select(['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','from_timestamp','to_stop_id','to_timestamp']) \
                .withColumn('actual_duration_mins', (F.unix_timestamp(F.col('to_timestamp')) - F.unix_timestamp(F.col('from_timestamp')))/60) \
                .orderBy(['date','user_trip_id','itinerary_id','leg_id']) \
                .filter('actual_duration_mins > 0')

def combine_otp_suggestions_with_bus_legs_actual_time(otp_suggestions,bus_legs_actual_time):
    return otp_legs_df \
                .join(clean_otp_legs_actual_time, on=['date','user_trip_id','itinerary_id','leg_id', 'route', 'from_stop_id','to_stop_id'], how='left_outer') \
                .withColumn('considered_duration_mins', F.when(F.col('mode') == F.lit('BUS'), F.col('actual_duration_mins')).otherwise(F.col('otp_duration_mins'))) \
                .withColumn('considered_start_time', F.when(F.col('mode') == F.lit('BUS'), F.col('from_timestamp')).otherwise(F.col('otp_start_time')))

def select_itineraries_fully_identified(otp_itineraries_legs):
    itineraries_not_fully_identified = otp_itineraries_legs \
                                        .filter((otp_itineraries_legs.mode == 'BUS') & (otp_itineraries_legs.busCode.isNull())) \
                                        .select(['date','user_trip_id','itinerary_id']).distinct()
    itineraries_fully_identified = otp_itineraries_legs.select(['date','user_trip_id','itinerary_id']).subtract(itineraries_not_fully_identified)
    return otp_itineraries_legs.join(itineraries_fully_identified, on=['date','user_trip_id','itinerary_id'], how='inner')

##########################################################################################################

#Main Code

if __name__ == "__main__":
	if len(sys.argv) < MIN_NUM_ARGS:
		print "Error: Wrong parameter specification!"
		printUsage()
		sys.exit(1)
	
	initial_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
	final_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
	od_matrix_folderpath = sys.argv[3]
	buste_data_folderpath = sys.argv[4]
	otp_server_url = sys.argv[5]
	results_folderpath = sys.argv[6]


	#Get Spark Session
	spark  = SparkSession.builder.getOrCreate()
	spark.conf.set('spark.sql.crossJoin.enabled', 'true')

	sc = spark.sparkContext
	sc.setLogLevel("ERROR")
	sqlContext = pyspark.SQLContext(sc)

	print "Got Spark Context"

	print "Reading OD-Matrix Data..."
	od_matrix_day_folderpath = od_matrix_folderpath + '/' + initial_date.strftime('%Y_%m_%d') + '_od'
	od_matrix = read_hdfs_folder(sqlContext, od_matrix_day_folderpath) \
                .withColumnRenamed('date','date_in_secs') \
                .withColumn('date', F.from_unixtime(F.col('date_in_secs'), 'yyyy-MM-dd')) \
                .withColumnRenamed('o_boarding_id','user_trip_id')	

	print "Preprocessing Data..."
	od_matrix = advance_od_matrix_start_time(od_matrix,120)

	#print "Reducing OD Matrix size to 50 trips for testing purposes..."
	#od_matrix = od_matrix.limit(50)

	print "Getting OTP suggested itineraries..."
	otp_suggestions = get_otp_suggested_trips(od_matrix,otp_server_url)

	print "Extracting OTP Legs info..."
	otp_legs_df = prepare_otp_legs_df(extract_otp_trips_legs(otp_suggestions))
	otp_legs_df.write.csv(path=results_folderpath+'/trip_plans',header=True, mode='overwrite')

	otp_suggestions = None

	#otp_legs_df = read_hdfs_folder(sqlContext,results_folderpath+'/trip_plans')

	print "Getting OTP schedule info for executed trips..."
	executed_trips_schedule = get_otp_scheduled_trips(od_matrix,otp_server_url)

	print "Extracting OTP Legs info..."
	executed_trips_schedule_df = prepare_otp_legs_df(extract_otp_trips_legs(executed_trips_schedule)) \
                                .filter('mode == \'BUS\'') \
                                .withColumn('itinerary_id',F.lit(0))

	executed_trips_schedule = None
	
	print "Filtering executed itineraries whose schedule was not found..."
	matched_executed_trips = od_matrix \
                     .withColumnRenamed('o_stop_id','from_stop_id') \
                     .withColumnRenamed('stopPointId','to_stop_id') \
                .join(executed_trips_schedule_df, \
                    on=['date','user_trip_id','from_stop_id','to_stop_id'], how='inner') \
                .select(['date','user_trip_id','itinerary_id','otp_duration_mins','otp_start_time']) \
                .withColumnRenamed('otp_duration_mins','planned_duration_mins') \
                .withColumnRenamed('otp_start_time','planned_start_time')

	print "Total num Trips: " + str(od_matrix.count())
	print "Num Executed Itineraries with Schedule found: " + str(matched_executed_trips.count())

	total_num_itineraries = otp_legs_df.select('user_trip_id','itinerary_id').distinct().count()
	total_num_legs = otp_legs_df.count()
	num_bus_legs = otp_legs_df.filter('mode == \'BUS\'').count()

	print "Total num itineraries:", total_num_itineraries
	print "Total num legs:", total_num_legs
	print "Total num bus legs:", num_bus_legs, '(', 100*(num_bus_legs/float(total_num_legs)), '%)'

	print "Reading BUSTE data..."
	bus_trips_data = read_folders(buste_data_folderpath, sqlContext, sc, initial_date, final_date,'_veiculos')
	clean_bus_trips_data = clean_buste_data(bus_trips_data)

	print "Finding OTP Bus Legs Actual Start Times in Bus Trips Data..."
	otp_legs_st = find_otp_bus_legs_actual_start_time(otp_legs_df,clean_bus_trips_data)

	num_bus_legs_st = otp_legs_st.count()
	print "Num Bus Legs whose start was found:", num_bus_legs_st, '(', 100*(num_bus_legs_st/float(num_bus_legs)), '%)'

	#Clean memory
	otp_legs_df.unpersist(blocking=True)
	executed_trips_schedule_df.unpersist(blocking=True)
	bus_trips_data.unpersist(blocking=True)
	clean_bus_trips_data.unpersist(blocking=True)

	print "Reading BUSTE data again..."
	bus_trips_data2 = read_folders(buste_data_folderpath, sqlContext, sc, initial_date, final_date,'_veiculos')
	clean_bus_trips_data2 = clean_buste_data(bus_trips_data2)

	print "Finding OTP Bus Legs Actual End Times in Bus Trips Data..."
	otp_legs_start_end = find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips_data2)
	clean_otp_legs_actual_time = clean_otp_legs_actual_time_df(otp_legs_start_end)

	#Clean Memory
	otp_legs_st.unpersist(blocking=True)
	bus_trips_data2.unpersist(blocking=True)
	clean_bus_trips_data2.unpersist(blocking=True)
	otp_legs_start_end.unpersist(blocking=True)

	print "Enriching OTP suggestions legs with actual time data..."
	all_legs_actual_time = combine_otp_suggestions_with_bus_legs_actual_time(otp_legs_df,clean_otp_legs_actual_time)
	
	print "Filtering out itineraries with bus legs not identified in bus data..."
	clean_legs_actual_time = select_itineraries_fully_identified(all_legs_actual_time)

	num_itineraries_fully_identified = clean_legs_actual_time.select('user_trip_id','itinerary_id').distinct().count()
	print "Num Itineraries fully identified in BUSTE data:", num_itineraries_fully_identified, '(', 100*(num_itineraries_fully_identified/float(total_num_itineraries)), '%)'
	
	#print "Writing OTP suggested itineraries legs with actual time to file..."
	#clean_legs_actual_time.write.csv(path=results_folderpath+'/otp_legs_matched',header=True, mode='append')

	#Ciea Memory
	clean_otp_legs_actual_time.unpersist(blocking=True)
	all_legs_actual_time.unpersist(blocking=True)
	
	print "Gathering all trips alternative/executed itineraries info..."
	first_boarding_time = clean_legs_actual_time \
                        .filter('mode == \'BUS\'') \
                        .groupby(['date', 'user_trip_id', 'itinerary_id']) \
                        .agg(F.first('otp_start_time').alias('planned_start_time'), \
                             F.first('considered_start_time').alias('actual_start_time')) \
                        .orderBy(['date','user_trip_id','itinerary_id'])        
                

	user_trips_time_info = od_matrix \
                        .withColumnRenamed('executed_duration','exec_duration_mins') \
                        .withColumnRenamed('o_datetime','exec_start_time') \
                        .select(['date','user_trip_id','exec_duration_mins','exec_start_time'])

	print "Gathering information from executed itineraries whose schedule was found..."

	executed_legs = user_trips_time_info \
            .join(matched_executed_trips, on=['date','user_trip_id'], how='left') \
            .withColumn('actual_duration_mins',F.col('exec_duration_mins')) \
            .withColumn('actual_start_time',F.col('exec_start_time')) \
            .withColumn('itinerary_id',F.lit(0)) \
            .select(['date','user_trip_id','itinerary_id','planned_duration_mins','actual_duration_mins','exec_duration_mins',
                     'planned_start_time','actual_start_time','exec_start_time'])

	print "Gathering information from OTP suggestions..."

	matched_otp_legs = clean_legs_actual_time \
                            .groupBy(['date', 'user_trip_id', 'itinerary_id']) \
                            .agg(F.sum('otp_duration_mins').alias('planned_duration_mins'), \
                                 F.sum('considered_duration_mins').alias('actual_duration_mins')) \
                        .join(first_boarding_time, on=['date','user_trip_id','itinerary_id']) \
                        .join(user_trips_time_info, on=['date','user_trip_id'], how='inner') \
                        .orderBy(['date','user_trip_id','itinerary_id']) \
                        .select(['date','user_trip_id','itinerary_id','planned_duration_mins','actual_duration_mins','exec_duration_mins',
                     'planned_start_time','actual_start_time','exec_start_time'])
                 
	executed_trips_with_sugestions_matched = matched_otp_legs.select('user_trip_id')\
                                            .drop_duplicates()

	#Clean Memory
	clean_legs_actual_time.unpersist(blocking=True)
	first_boarding_time.unpersist(blocking=True)
	user_trips_time_info.unpersist(blocking=True)

	print "Assembling trips itineraries dataframe..."

	all_trips_alternatives = matched_otp_legs \
                .union(executed_legs) \
                .join(executed_trips_with_sugestions_matched, on='user_trip_id',how='inner') \
                .orderBy(['date','user_trip_id','itinerary_id'])

	print "Num Trips: " + str(od_matrix.count())
	print "Num Trips with Matched Suggestions: " + str(executed_trips_with_sugestions_matched.count())
	print "Num Trips Alternatives: " + str(all_trips_alternatives.count())

	#Clean Memory
	executed_legs.unpersist(blocking=True)
	matched_otp_legs.unpersist(blocking=True)
	executed_trips_with_sugestions_matched.unpersist(blocking=True)

	print "Writing Trips Alternatives Data to file..."
	all_trips_alternatives.write.csv(path=results_folderpath + '/' + initial_date.strftime('%Y_%m_%d')  + '_itineraries',header=True,mode='overwrite')

	print "Finishing Script..."
	sc.stop()

