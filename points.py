# Zak Lee - GEOG 495
# May 14, 2013

# Utility functions "geopull" assignment related to 
# generating random points.
# pyshp (shapefile) http://code.google.com/p/pyshp/

# special classes
import shapefile
# standard classes
import random, math, itertools

# credit for this algorithm goes to:
# http://geospatialpython.com/2011/01/point-in-polygon.html

def pointInPoly(x,y,poly):
    # Check
    # the idea of this ray-casting algorithm is to check the
    # number of intersections between the edge of the poly
    # . If it's odd, it's false.
    n = len(poly)
    inside = False

    # corresponds to the arbitrary first point in the polygon
    p1x,p1y = poly[0]

    # cycle through each point pair in shapefile
    for i in range(n+1):
        # the next point in the polygon border
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                # y coordinate falls in y bounding box
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        # we've detected that the line crosses
                        inside = not inside
        # reset to check next point pair
        p1x,p1y = p2x,p2y


    return inside

def getShapesWithFieldValue(shpf, field='TYPE', value='LND'):
    # returns lists of lists of points
    # for multi-point shapes, only return first list

    # shapefile from https://data.seattle.gov/dataset/Shorelines/gf6u-sgut
    sf = shapefile.Reader(shpf)
    shapes = sf.shapes()
    records = sf.records()
    # find the field's index
    field_index = [v[0] for v in sf.fields[1:]].index(field)
    
    matching_records = []

    # find records with matching fields
    for record_index in range(len(records)):
        if records[record_index][field_index] == value:
            matching_records.append(record_index)

    # return a list of matching shape geometries, ignoring non-first part shapes
    matching_shapes = []
    for r_i in matching_records:
        if len(shapes[r_i].parts) > 1:
            matching_shapes.append(shapes[r_i].points[0:shapes[r_i].parts[1]])
        else:
            matching_shapes.append(shapes[r_i].points)
    return matching_shapes

def getMultiBBox(shapes):
    # returns a bounding box for shapes represented by list of lists
    # of points
    point_x = [point[0] for shape in shapes for point in shape]
    point_y = [point[1] for shape in shapes for point in shape]
    return [max(point_x),
            max(point_y),
            min(point_x),
            min(point_y)]

def generateRandomPoint(bbox,precision=13):
    # returns a random point with a given precision inside a bounding box
    x = random.random()
    y = random.random()
    x = (bbox[0] - bbox[2])*x + bbox[2]
    y = (bbox[1] - bbox[3])*y + bbox[3]
    return [round(x, precision), round(y, precision)]

def generatePointInside(shapes, bbox):
    # returns a randomly generated point within a series of shapes
    while True:
        x, y = generateRandomPoint(bbox)
        inside = False
        for shape in shapes:
            if pointInPoly(x,y,shape):
                return [x, y]

def generatePoints(num_points, shapefile):
    # returns a list of randomly distributed x,y
    # point pairs inside a shapefile
    shapes =getShapesWithFieldValue(shapefile)
    bbox = getMultiBBox(shapes)
    return [generatePointInside(shapes, bbox) for i in xrange(num_points)]

# Everything after here trys somewhat sucessfully to implement an algorithm of
# questionable utility to try to find points far away from each other to query
# In retrospect, a stratification strategy might have been more logical.

def getDistance(a, b):
    # returns distance between a and b
    return math.sqrt((a[0]-b[0])**2+(a[1]-b[1])**2)

def getSubset(full_list, count):
    # get a randomized subset of a list
    if count >= len(full_list):
        return list(full_list)
    else:
        return random.sample(full_list, count)

def addEdges(traveled_points, choice_points, edge_dict):
    # generate graph edges
    for p1 in traveled_points:
        if p1[0] not in edge_dict:
            edge_dict[p1[0]] = {p2[0]: getDistance(p1[1:],p2[1:]) for p2 in choice_points}
        else:
            for p2 in choice_points:
                if p2[0] not in edge_dict[p1[0]]:
                    edge_dict[p1[0]][p2[0]] = getDistance(p1[1:],p2[1:])
    for p2 in choice_points:
        if p2[0] not in edge_dict:
            edge_dict[p2[0]] = {p1[0]: getDistance(p1[1:],p2[1:]) for p1 in traveled_points}
        else:
            for p1 in traveled_points:
                if p1[0] not in edge_dict[p2[0]]:
                    edge_dict[p2[0]][p1[0]] = getDistance(p1[1:],p2[1:])
    return edge_dict

def findFurthest(traveled_points, choice_points, edge_dict):
    # from a group of traveled and choice points,
    # returns the choice for a point that maximizes the minimum
    # distance to another point

    edge_dict = addEdges(traveled_points, choice_points, edge_dict)
    
    point = None
    pd = 0
    for p1 in choice_points[1:]:    
        tdict = edge_dict[p1[0]]
        p2 = min(traveled_points, key=lambda x:tdict[x[0]])
        if not point or getDistance(p1,p2) > pd:
            point = p1
            pd = getDistance(p1,p2)
    return point

def SplitListAlg(traveled_points, choice_points, edge_dict,
                point_limit=200):
    # recursive function to create a sequence maximizing travel time along a graph
    # this is a lazy, lazy algorithm, the bogo-sort of graph traversal alogrithms
    if len(choice_points) == 1:
        # end condition
        return traveled_points + choice_points
    # only searches a subset of future options.
    point = findFurthest(getSubset(traveled_points, point_limit),
                         getSubset(choice_points, point_limit),
                         edge_dict)
    choice_points.remove(point)
    traveled_points.append(point)
    return SplitListAlg(traveled_points, choice_points, edge_dict, point_limit)


def getBestSequence(point_list):
    # master function for this algorithm.
    # so I realized here that python has a hard-coded max recursion of 999 runs
    # so we arbitrarily partition here.
    max_depth = 800
    outlist = []
    for part in range(len(point_list)/max_depth+1):
        # take the first point in the partition
        traveled_points = point_list[part*max_depth:max_depth*part+1]
        #take the remaining parts of the partition
        if len(point_list) == part:
            choice_points = point_list[part*max_depth+1:]
        else:
            choice_points = point_list[part*max_depth+1:(part+1)*max_depth]
        edge_dict = {}
        outlist = outlist + SplitListAlg(traveled_points,choice_points, edge_dict)
    return outlist
