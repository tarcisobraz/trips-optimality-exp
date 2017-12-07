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

def read_folders(path, sqlContext, sc, initial_date, final_date):
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

        #print files

        files = filter(lambda f: initial_date <= datetime.strptime(f.split("/")[-2], '%Y_%m_%d_veiculos') <=
                                 final_date, files)

        #print files

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

    data_frame = data_frame.withColumn("date", F.date_sub(F.lit(date),1))
    
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
	otp_legs_start = otp_legs_df.withColumn('stopPointId', F.col('from_stop_id'))
	otp_legs_start = otp_legs_start.join(clean_bus_trips_df, ['date','route','stopPointId'], how='inner') \
                        .na.drop(subset=['timestamp']) \
                        .withColumn('timediff',F.abs(F.unix_timestamp(F.col('timestamp')) - F.unix_timestamp(F.col('otp_start_time')))) \
                        .drop('otp_duration')
	w = Window.partitionBy(['date','user_trip_id','itinerary_id','route','from_stop_id']).orderBy(['timediff'])
	otp_legs_start = otp_legs_start.withColumn('rn', F.row_number().over(w)) \
                    .where(F.col('rn') == 1)
	otp_legs_start = otp_legs_start \
		.select(['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','otp_start_time','timestamp','to_stop_id','otp_end_time']) \
		.withColumnRenamed('timestamp','from_timestamp')

	return otp_legs_start

def find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips):
	trip_plans_df_end = otp_legs_st.withColumnRenamed('to_stop_id','stopPointId')
	trip_plans_start_end = trip_plans_df_end.join(clean_bus_trips, ['date','route','busCode','tripNum','stopPointId'], how='inner') \
		.na.drop(subset=['timestamp']) \
		.withColumn('timediff',F.abs(F.unix_timestamp(F.col('timestamp')) - F.unix_timestamp(F.col('otp_end_time'))))
	trip_plans_start_end = trip_plans_start_end.withColumnRenamed('timestamp', 'to_timestamp') \
		.withColumnRenamed('stopPointId','to_stop_id') \
		.orderBy(['date','route','stopPointId','timediff'])
	
	return trip_plans_start_end

def clean_otp_legs_actual_time_df(otp_legs_st_end_df):
	clean_otp_legs_df = otp_legs_start_end.select(['date','user_trip_id','itinerary_id','leg_id','route','busCode','tripNum','from_stop_id','from_timestamp','to_stop_id','to_timestamp']) \
                        .withColumn('actual_duration_mins', (F.unix_timestamp(F.col('to_timestamp')) - F.unix_timestamp(F.col('from_timestamp')))/60) \
                        .orderBy(['date','user_trip_id','itinerary_id','leg_id'])
	return clean_otp_legs_df.filter(clean_otp_legs_df.actual_duration_mins > 0)
	
	
def combine_otp_suggestions_with_bus_legs_actual_time(otp_suggestions,bus_legs_actual_time):
	return otp_legs_df.join(clean_otp_legs_actual_time, on=['date','user_trip_id','itinerary_id','leg_id', 'route', 'from_stop_id','to_stop_id'], how='left_outer') \
				.withColumn('considered_duration_mins', F.when(F.col('mode') == F.lit('BUS'), F.col('actual_duration_mins')).otherwise(F.col('otp_duration_mins')))


def select_itineraries_fully_identified(otp_itineraries_legs):
	legs_not_fully_identified = otp_itineraries_legs.filter((otp_itineraries_legs.mode == 'BUS') & (otp_itineraries_legs.busCode.isNull()))
	itineraries_not_fully_identified = legs_not_fully_identified.select(['date','user_trip_id','itinerary_id']).distinct()
	itineraries_fully_identified = otp_itineraries_legs.select(['date','user_trip_id','itinerary_id']).subtract(itineraries_not_fully_identified)
	return otp_itineraries_legs.join(itineraries_fully_identified, on=['date','user_trip_id','itinerary_id'], how='inner')

def rank_otp_itineraries_by_actual_duration(otp_itineraries_legs):
	trips_itineraries_duration = otp_itineraries_legs.groupBy(['date', 'user_trip_id', 'itinerary_id']) \
                                .agg(F.sum('considered_duration_mins').alias('duration')) \
                                .orderBy(['date','user_trip_id','itinerary_id'])
	itineraries_window = Window.partitionBy(['date','user_trip_id']).orderBy(['duration'])
	return trips_itineraries_duration.withColumn('rank', F.row_number().over(itineraries_window))


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

#Print Args
#print "initial-date:", initial_date
#print "final-date:", final_date
#print "od-matrix-folderpath:", od_matrix_folderpath
#print "buste-data-folderpath:", buste_data_folderpath
#print "otp-server-url:", otp_server_url
#print "results-folderpath:", results_folderpath


#Get Spark Session
spark  = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.crossJoin.enabled', 'true')

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlContext = pyspark.SQLContext(sc)

print "Got Spark Context"

print "Reading OD-Matrix Data..."
od_matrix = read_hdfs_folder(sqlContext,od_matrix_folderpath)

print "Filtering OD-Matrix Data according to analysis input dates..."
initial_date_secs = int(initial_date.strftime("%s"))
final_date_secs = int(final_date.strftime("%s"))
od_matrix = od_matrix.withColumn('date', F.from_unixtime(F.col('date'),'yyyy-MM-dd')) \
				.withColumn('date_in_secs', F.unix_timestamp(F.col('date'), 'yyyy-MM-dd')) \
				.filter((F.col('date_in_secs') >= initial_date_secs) & (F.col('date_in_secs') <= final_date_secs))

print "Preprocessing Data..."
od_matrix = advance_od_matrix_start_time(od_matrix,120)

print "Getting OTP suggested itineraries..."
otp_suggestions = get_otp_suggested_trips(od_matrix,otp_server_url)

print "Extracting OTP Legs info..."
otp_legs_df = prepare_otp_legs_df(extract_otp_trips_legs(otp_suggestions))
get_df_stats(otp_legs_df,otp_legs_df.filter('mode == \'BUS\''),'Num OTP Legs','Num OTP Bus Legs')
otp_legs_df.write.csv(path=results_folderpath+'/trip_plans',header=True, mode='append')

#otp_legs_df = read_hdfs_folder(sqlContext,results_folderpath+'/trip_plans')

print "Reading BUSTE data..."
#bus_trips_data = read_buste_data_v3(sqlContext, buste_data_folderpath)
bus_trips_data = read_folders(buste_data_folderpath, sqlContext, sc, initial_date, final_date)
clean_bus_trips_data = clean_buste_data(bus_trips_data)

print "Finding OTP Bus Legs Actual Start Times in Bus Trips Data..."
otp_legs_st = find_otp_bus_legs_actual_start_time(otp_legs_df,clean_bus_trips_data)
get_filtered_df_stats(otp_legs_st,otp_legs_df.count(),'Num Legs whose Start was found','Num Legs')

print "Finding OTP Bus Legs Actual End Times in Bus Trips Data..."
#bus_trips_data2 = read_buste_data_v3(sqlContext,buste_data_folderpath)
bus_trips_data2 = read_folders(buste_data_folderpath, sqlContext, sc, initial_date, final_date)
clean_bus_trips_data2 = clean_buste_data(bus_trips_data2)

otp_legs_start_end = find_otp_bus_legs_actual_end_time(otp_legs_st,clean_bus_trips_data2)
clean_otp_legs_actual_time = clean_otp_legs_actual_time_df(otp_legs_start_end)

print "Enriching OTP suggestions legs with actual time data..."
all_legs_actual_time = combine_otp_suggestions_with_bus_legs_actual_time(otp_legs_df,clean_otp_legs_actual_time)

print "Filtering out itineraries with bus legs not identified in bus data..."
clean_legs_actual_time = select_itineraries_fully_identified(all_legs_actual_time)

print "Ranking OTP suggested itineraries by actual duration..."
ranked_otp_itineraries = rank_otp_itineraries_by_actual_duration(clean_legs_actual_time)

print "Selecting best otp itineraries by actual duration..."
best_itineraries_duration = ranked_otp_itineraries.filter(ranked_otp_itineraries.rank == 1) \
                                .drop('rank')

print "Computing executed trips duration..."
executed_trips_duration =  executed_trips_duration = od_matrix.withColumnRenamed('o_boarding_id','user_trip_id') \
                            .select(['date','user_trip_id','o_datetime','executed_duration'])                            
print "Comparing Executed Trips with Best Suggested trips and calculating improvement capacity..."
duration_improvement_capacity = best_itineraries_duration.join(executed_trips_duration, on=['date','user_trip_id']) \
	.withColumn('imp_capacity', F.col('executed_duration') - F.col('duration'))

print "Writing duration improvement capacity to file..."
duration_improvement_capacity.write.csv(path=results_folderpath+'/duration_improvement_capacity',header=True, mode='append')

print "Writing OTP suggested itineraries with actual time to file..."
clean_legs_actual_time.write.csv(path=results_folderpath+'/clean_otp_legs_actual_time',header=True, mode='append')

print "Finishing Script..."


























