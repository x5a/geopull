# Main functions for this "geopull" project
# Description:
# Twitter collects a large amount of geocoding data volunteered
# from its users. I wanted to:
# 1. find out what type of data is collected
# 2. systematically collect some of that data without being detected.
#    (this project mines potentially valuable/proprietary data)
# 3. examine how "complete" this data is - how close are the points
#    from the queried location?

# 8-12 hours, approximately

# import standard packages
import csv, os, time, pickle, pprint, random

# import non-standard classes
import arcpy
from twitter import *

# import our classes
import points

# this program requires
# pyshp (shapefile) http://code.google.com/p/pyshp/
# and Python Twitter Tools(twitter) http://mike.verdone.ca/twitter/

# STATIC VARIABLES
SEED_POINT_FILE = 'seedpoints.csv'
POINT_SEQUENCE_DATA = 'seedpoints.save'
RETURNED_POINTS_FILE = 'returnpoints.csv'
RETURNED_POINTS_DATA = 'returnpoints.save'

SPATIAL_REF_ID = 4326 # WGS 84

RESULTS_NAME = 'resultpoints.shp'
SEEDS_NAME = 'querypoints.shp'
LINES_NAME = 'queryresultlines.shp'

# twitter rate limiting window, in seconds.
TWITTER_TIME_BLOCK = 15*60
# twitter max requests per window
TWITTER_MAX_REQ = 15

# set these auth keys (from dev.twitter.com)
OAUTH_TOKEN = ''
OAUTH_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

# METHODS
def createSeedPoints(pts):
	# generate and save random points to check in twitter
	print 'creating ' + str(pts) + ' points...'
	# helper function to generate 'pts' number of points randomly distributed
	# x-y
	seed_points = points.generatePoints(pts, 'Shorelines/WGS84/shorelines')
	print 'writing points to ' + SEED_POINT_FILE + '...'
	with open(SEED_POINT_FILE, 'wb') as csvfile:
	    csvhandle = csv.writer(csvfile, dialect='excel')
	    csvhandle.writerow(['ID','long','lat'])
	    for p_i in range(pts):
			csvhandle.writerow([p_i, seed_points[p_i][0], seed_points[p_i][1]])
	return [(p_i, seed_points[p_i][0], seed_points[p_i]) for p_i in range(pts)]

def getSeedPoints():
	# load random points to check in twitter from CSV file
	print 'reading points...'
	seed_points = []
	with open(SEED_POINT_FILE, 'r') as csvfile:
		csvhandle = csv.reader(csvfile, dialect='excel')
		csvhandle.next() # first row contains header
		for row in csvhandle:
			# ID, lat, long
			seed_points.append((int(row[0]), float(row[1]), float(row[2])))
	return seed_points

def searchSaveLoop(sorted_points):
	# this is the main loop of our program, cycles through points querying for
	# geodata

	# open a twitter object with authentication
	t = Twitter(auth=OAuth(OAUTH_TOKEN, OAUTH_SECRET,
					CONSUMER_KEY, CONSUMER_SECRET))
	
	# basically, we expect to have to rerun this program to get through all the points.
	# here, we load our progress or create new files to save to.
	if os.path.exists(RETURNED_POINTS_FILE) and os.path.exists(RETURNED_POINTS_DATA):
		print 'found previous feed saved, loading...'
		call_dictionary = pickle.load(open(RETURNED_POINTS_DATA,'rb'))
		csvhandle = csv.writer(open(RETURNED_POINTS_FILE,'ab'), dialect='excel')
	else:
		print 'found no feed, creating new save files...'
		call_dictionary = {}	# dictionary that contains subject and response for each API Query
		csvhandle = csv.writer(open(RETURNED_POINTS_FILE,'wb'), dialect='excel')
		csvhandle.writerow(['search_id','search_long','search_lat','street_address','full_name','name','twitter_id','place_type','url','long','lat'])

	# start from old position in points list
	current_position = len(call_dictionary.keys())

	# start looping through search points
	for point in sorted_points[current_position:]:

		print 'querying point: ' + str(point)

		# perform query
		query = geoSearch(point[1], point[2], t)

		# take the results of the query that contain place data
		place_list = query['result']['places']
		# print results of query
		print(query)

		# we pickle the, in case we want to come back later
		# and retrieve other data from the API call
		to_pickle = []
		print('saving output...')
		for place in place_list:
			# save each place to the csv
			csvhandle.writerow(list(point) + placeToRow(place))
			# save each place to the 
			to_pickle.append(dict(place))

		# keeps dictionary of query place results
		call_dictionary[point[0]] = to_pickle

		# save returned points data to pickle.
		with open(RETURNED_POINTS_DATA, 'wb') as picklefile:
			pickle.dump(call_dictionary, picklefile)

		# pause here to prevent exceeding query limit
		print('waiting...')
		time.sleep(float(TWITTER_TIME_BLOCK)/TWITTER_MAX_REQ +
			random.random() * 15)

def placeToRow(place_dictionary):
	# produces a list for tablular form from a twitter api place dicitionary
	# ['street_address','full_name','name','twitter_id','place_type','url','long','lat']
	keys = ["[u'attributes'][u'street_address']",
			"['full_name']",
			"['name']",
			"['id']",
			"['place_type']",
			"['url']",
			"['bounding_box']['coordinates'][0][0][0]", # we take one coordinate
			"['bounding_box']['coordinates'][0][0][1]"] # pair of the bounding box
	out_row = []
	# we cycle through various dictionary lookups using eval()
	# the api specifies that not all fields have to be returned, hence
	# the try/except pattern
	for key in keys:
		try:
			eval("out_row.append(place_dictionary" + key + ".encode('ascii'))")
		except:
			try: 
				eval("out_row.append(str(place_dictionary" + key + ").encode('ascii'))")
			except:
				out_row.append(' ')

	return out_row

def geoSearch(x, y, t):
	# oneliner to return results of search using twitter API
	return t.geo.search(lat=y, long=x, granularity='poi', max_results=20)

def loadDatatoSHPs(seed_points):
	# let's turn our data into points, with some statistics for averages
	results_list = []
	# open csv of files
	with open(RETURNED_POINTS_FILE, 'rU') as csvfile:
		#['search_id','search_long','search_lat','street_address','full_name','name','twitter_id','place_type','url','long','lat']
		csvhandle = csv.reader(csvfile, dialect='excel')
		csvhandle.next() # remove header
		for row in csvhandle:
			results_list.append(row)

	# find out how many points were queried
	queried_points = [row[0] for row in results_list]
	# only some results are actually points of interest, rather than city descriptions
	results_list_poi = [row for row in results_list if row[7] == 'poi']
	# determine the points of interest
	pois = [row[6] for row in results_list_poi]

	# print some statistics
	print 'Results: ' + str(len(results_list))
	print 'PoI Results: ' + str(len(results_list_poi))
	print 'Queried Points: ' + str(len(set(queried_points)))
	print 'Unique PoIs: ' + str(len(set(pois)))

	sr = arcpy.SpatialReference(SPATIAL_REF_ID)

	# create feature classes for storage
	arcpy.CreateFeatureclass_management(os.getcwd(), RESULTS_NAME, geometry_type='POINT', spatial_reference=sr)
	arcpy.CreateFeatureclass_management(os.getcwd(), SEEDS_NAME, geometry_type='POINT', spatial_reference=sr)
	arcpy.CreateFeatureclass_management(os.getcwd(), LINES_NAME, geometry_type='POLYLINE', spatial_reference=sr)

	# add fields
	fields = ['search_id','str_addr','full_name','name','twitter_id']
	types = ['LONG','STRING','STRING','STRING','STRING']
	for field, typ in zip(fields, types):
		arcpy.AddField_management(RESULTS_NAME, field, typ)

	fields2 = ['search_id','count','avgdist']
	types2 = ['LONG','LONG','DOUBLE']
	for field, typ in zip(fields2, types2):
		arcpy.AddField_management(SEEDS_NAME, field, typ)

	fields3 = ['search_id','twitter_id']
	types3 = ['LONG','STRING']
	for field, typ in zip(fields3, types3):
		arcpy.AddField_management(LINES_NAME, field, typ)

	# populate results
	with arcpy.da.InsertCursor(RESULTS_NAME, fields + ['SHAPE@X', 'SHAPE@Y']) as cursor:
		for row in results_list_poi:
			cursor.insertRow((row[0], row[3], row[4], row[5], row[6], float(row[9]), float(row[10])))
	
	# populate seed points/lines
	total_distance = 0.0
	line_cursor = arcpy.da.InsertCursor(LINES_NAME,  fields3 + ['SHAPE@'])
	with arcpy.da.InsertCursor(SEEDS_NAME,  fields2 + ['SHAPE@X', 'SHAPE@Y']) as cursor:
		for row in seed_points:

			# calculate count/avg distance of query results
			d = 0.0
			matching_results = [r for r in results_list_poi if str(r[0])==str(row[0])]
			for result in matching_results:
				d = d + points.getDistance((row[1], row[2]), (float(result[9]), float(result[10])))
			try:
				avg = d/len(matching_results)
			except:
				avg = -1.0

			# keep track of total poi - query distance
			total_distance = total_distance + d
			# update seed point
			cursor.insertRow((row[0], len(matching_results), avg, float(row[1]), float(row[2])))

			# add lines connecting queries and seed points
			for result in matching_results:
				p1 = arcpy.Point(float(row[1]), float(row[2]))
				p2 = arcpy.Point(float(result[9]), float(result[10]))
				line_cursor.insertRow((row[0], result[6], arcpy.Polyline(arcpy.Array([p1, p2]), sr)))
	del line_cursor

	print 'Average Query to Result distance (deg): ' + str(total_distance/len(results_list_poi))


# MAIN FUNCTION
def __init__():
	# check if we've already generated and sorted the "seed" points
	if os.path.exists(POINT_SEQUENCE_DATA):
		# if we have, load them
		with open(POINT_SEQUENCE_DATA, 'rb') as picklefile:
			sp = pickle.load(picklefile)
			print 'loaded points...'
	else:
		# otherwise, create them?
		if os.path.exists(SEED_POINT_FILE):
			sp = getSeedPoints()
		else:
			sp = createSeedPoints(2000)
		# in case twitter cuts off our access, how do we make sure we get a good coverage in our sample?
		# this algorithm tries to maximize the initial spread of our points
		print 'sorting points...'
		sp = points.getBestSequence(sp)
		# we save the data to a CSV
		with open(POINT_SEQUENCE_DATA, 'wb') as picklefile:
			pickle.dump(sp, picklefile)

	# execute search loop
	searchSaveLoop(sp)

	# get some statistics about the data and load it into a shapefile
	loadDatatoSHPs(sp)

