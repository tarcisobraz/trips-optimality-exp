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

#Constants
MIN_NUM_ARGS = 7

#Functions
def printUsage():
	print "Script Usage:"
	print "spark-submit %s <initial-date> <final-date> <od-matrix-folderpath> <buste-data-folderpath> <otp-server-url> <results-folderpath>" % (sys.argv[0])


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
    
    date = "-".join(folderpath.split("/")[-2].split("_")[:3])

    data_frame = data_frame.withColumn("date", F.lit(date))
    
    return data_frame

def printdf(df,l=10):
    return df.limit(l).toPandas()

def get_timestamp_in_tz(unixtime_timestamp,ts_format,tz):
    return F.from_utc_timestamp(F.from_unixtime(unixtime_timestamp, ts_format),tz)

def get_otp_itineraries(otp_url,o_lat,o_lon,d_lat,d_lon,date,time,verbose=False):
    otp_http_request = 'routers/ctba/plan?fromPlace={},{}&toPlace={},{}&mode=TRANSIT,WALK&date={}&time={}'
    otp_request_url = otp_url + otp_http_request.format(o_lat,o_lon,d_lat,d_lon,date,time)
    if verbose: 
		print otp_request_url
    return json.loads(urllib2.urlopen(otp_request_url).read())

def get_otp_suggested_trips(od_matrix,otp_url):
	trips_otp_response = {}
	counter = 0
	for row in od_matrix.collect():
		id=long(row['o_boarding_id'])
		start_time = row['o_base_datetime'].split(' ')[1]
		trip_plan = get_otp_itineraries(otp_url,row['o_shape_lat'], row['o_shape_lon'], row['shapeLat'], row['shapeLon'],row['date_str'],start_time)
		trips_otp_response[id] = trip_plan
		counter+=1
	
	return trips_otp_response

def advance_od_matrix_start_time(od_matrix,extra_seconds):
	return od_matrix.withColumn('date_str', F.col('date')) \
					.withColumn('o_datetime', F.concat(F.col('date_str'), F.lit(' '), F.col('o_timestamp'))) \
					.withColumn('d_datetime', F.concat(F.col('date_str'), F.lit(' '), F.col('timestamp'))) \
					.withColumn('executed_duration', (F.unix_timestamp('d_datetime') - F.unix_timestamp('o_datetime'))/60) \
					.withColumn('o_base_datetime', F.from_unixtime(F.unix_timestamp(F.col('o_datetime'),'yyyy-MM-dd HH:mm:ss') - extra_seconds, 'yyyy-MM-dd HH:mm:ss')) \

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
		.withColumn('date',F.col('date').astype('string')) \
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

def rank_otp_itineraries_by_actual_duration(trips_itineraries):
	itineraries_window = Window.partitionBy(['date','user_trip_id']).orderBy(['duration'])
	return trips_itineraries.withColumn('rank', F.row_number().over(itineraries_window))

def get_trips_itineraries_pool(trips_otp_alternatives,od_mat):
	return trips_otp_alternatives \
				.union(od_mat.withColumnRenamed('o_boarding_id','user_trip_id') \
				.withColumn('itinerary_id', F.lit(0)) \
				.withColumnRenamed('executed_duration','duration') \
				.withColumnRenamed('o_datetime', 'alt_start_time') \
				.select(['date','user_trip_id','itinerary_id','duration','alt_start_time']))

def determining_trips_alternatives_feasibility(otp_itineraries_legs,od_mat):
	trips_itineraries_possibilities = otp_itineraries_legs \
						.groupBy(['date', 'user_trip_id', 'itinerary_id']) \
						.agg(F.sum('considered_duration_mins').alias('duration'), \
                           	 F.first('considered_start_time').alias('alt_start_time')) \
						.orderBy(['date','user_trip_id','itinerary_id']) \
			.join(od_mat \
						.withColumnRenamed('o_boarding_id','user_trip_id') \
						.withColumnRenamed('o_datetime','exec_start_time') \
						.select(['date','user_trip_id','exec_start_time']),
				on=['date','user_trip_id']) \
			.withColumn('start_diff', (F.abs(F.unix_timestamp(F.col('exec_start_time')) - F.unix_timestamp(F.col('alt_start_time')))/60))

	filtered_trips_possibilities = trips_itineraries_possibilities \
										.filter(F.col('start_diff') <= 20) \
                                		.drop('exec_start_time', 'start_diff')

	return (trips_itineraries_possibilities,filtered_trips_possibilities)

def select_best_trip_itineraries(itineraries_pool):
	return rank_otp_itineraries_by_actual_duration(itineraries_pool).filter('rank == 1') \
									.drop('rank')

def compute_improvement_capacity(best_itineraries,od_mat):
	return  od_mat \
				.withColumnRenamed('o_boarding_id','user_trip_id') \
				.withColumnRenamed('o_datetime','exec_start_time') \
				.select(['date','user_trip_id','cardNum','birthdate','gender','exec_start_time','executed_duration']) \
			.join(best_itineraries, on=['date','user_trip_id']) \
			.withColumn('imp_capacity', F.col('executed_duration') - F.col('duration'))



#Main Code

if __name__ == "__main__":
	if len(sys.argv) < MIN_NUM_ARGS:
		print "Error: Wrong parameter specification!"
		printUsage()
		sys.exit(1)
	
	#od_matrix_folderpath, otp_server_url, results_folderpath = sys.argv[1:3]:w
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
					.withColumn('date', F.from_unixtime(F.col('date'),'yyyy-MM-dd')) \
					.withColumn('date_in_secs', F.unix_timestamp(F.col('date'), 'yyyy-MM-dd'))

	print "Preprocessing Data..."
	od_matrix = advance_od_matrix_start_time(od_matrix,120)

	print "Getting OTP suggested itineraries..."
	otp_suggestions = get_otp_suggested_trips(od_matrix,otp_server_url)

	print "Extracting OTP Legs info..."
	otp_legs_df = prepare_otp_legs_df(extract_otp_trips_legs(otp_suggestions))
	#otp_legs_df.write.csv(path=results_folderpath+'/trip_plans',header=True, mode='append')

	otp_suggestions = None

	#otp_legs_df = read_hdfs_folder(sqlContext,results_folderpath+'/trip_plans')

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
	
	print "Writing OTP suggested itineraries legs with actual time to file..."
	#clean_legs_actual_time.write.csv(path=results_folderpath+'/otp_legs_matched',header=True, mode='append')

	#Clean Memory
	clean_otp_legs_actual_time.unpersist(blocking=True)
	all_legs_actual_time.unpersist(blocking=True)
	

	print "Identifying itinerary alternatives which are feasible..."
	trips_itineraries_possibilities, filtered_trips_possibilities = determining_trips_alternatives_feasibility(clean_legs_actual_time,od_matrix)	

	print "Writing itineraries possibilities with feasibility to file..."
	#trips_itineraries_possibilities.write.csv(path=results_folderpath+'/itineraries_alternatives',header=True, mode='append')

	print "Adding executed trips to the pool of itinerary possibilities..."
	trips_itineraries_pool = get_trips_itineraries_pool(filtered_trips_possibilities,od_matrix)

	print "Selecting best otp itineraries by actual duration..."
	best_trips_itineraries = select_best_trip_itineraries(trips_itineraries_pool)

	#Clean Memory
	clean_legs_actual_time.unpersist(blocking=True)
	trips_itineraries_possibilities.unpersist(blocking=True)
	filtered_trips_possibilities.unpersist(blocking=True)
	trips_itineraries_pool.unpersist(blocking=True)


	print "Computing Improvement Capacity..."
	duration_improvement_capacity = compute_improvement_capacity(best_trips_itineraries,od_matrix)

	best_trips_itineraries.unpersist(blocking=True)
	od_matrix.unpersist(blocking=True)
	
	print "Writing duration improvement capacity to file..."
	duration_improvement_capacity.write.csv(path=results_folderpath+'/duration_improvement_capacity',header=True, mode='append')

	print "Finishing Script..."
	sc.stop()

