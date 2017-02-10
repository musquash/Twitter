from pyspark.sql import SQLContext
from pyspark.sql.types import *
import json
import re
from urllib.request import urlopen
import codecs

from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, LinearSegmentedColormap, PowerNorm
import numpy as np
import time


import os
os.chdir('/Users/Philipp/ownCloud/Twitter/')
os.curdir


# Hier werden die Twitterdaten in ein DataFrame geladen.
data = spark.read.json("/Users/Philipp/ownCloud/Twitter/json/foo.json")

#Methode zum Plotten von Daten als Heatmap
def plot_map(dataFrame, color_mode='screen',
             out_filename='tweets_map_mpl.png', absolute=True):
    """Plots the given CSV data files use matplotlib basemap and saves it to
    a PNG file.
    Args:
        in_filename: Filename of the CSV containing the data points.
        out_filename: Output image filename
        color_mode: Use 'screen' if you intend to use the visualisation for
                    on screen display. Use 'print' to save the visualisation
                    with printer-friendly colors.
        absolute: set to True if you want coloring to depend on your dataset
                  parameter value (ie for comparison).
                  When set to false, each coordinate pair gets a different
                  color.
    """

    if color_mode == 'screen':
        bg_color = (0.0, 0.0, 0, 1.0)
        coast_color = (204/255.0, 0, 153/255.0, 0.7)
        color_list = [(0, 0.5, 0, 0.03),
                      (0.7, 0, 0, 1.0),
                      (0.0, 0, 0.7, 1.0)]
    else:
        bg_color = (1.0, 1.0, 1.0, 1.0)
        coast_color = (10.0/255.0, 10.0/255.0, 10/255.0, 0.8)
        color_list = [(0.0, 0.0, 0.0, 0.0),
                      (100/143.0, 188, 100/143.0, 0.6),
                      (255/200.0, 60/100.0, 70/100.0, 1.0)]

    json_cols = ('departure','dep_lat', 'dep_long', 'arrival' ,'arr_lat', 'arr_long')
    
    routes = dataFrame.collect() #Liste des DataFrame, welches die Routen beinhaltet
        
    num_routes = len(routes)

    # create a linear color scale with enough colors
    if absolute:
        n = routes[0][6]
    else:
        n = num_routes
    cmap = LinearSegmentedColormap.from_list('cmap_flights', color_list,
                                             N=n)
    
    # create the map and draw country boundaries
    fig = plt.figure(figsize=(20,10))  # predefined figure size, change to your liking. 
    # But doesn't matter if you save to any vector graphics format though (e.g. pdf)
    ax = fig.add_axes([0.05,0.05,0.9,0.85])
    bot, top, left, right = 5.87, 15.04, 47.26, 55.06 # just to zoom in to only Germany
    m = Basemap(projection='merc', resolution='l',
    llcrnrlat=left,
    llcrnrlon=bot,
    urcrnrlat=right,
    urcrnrlon=top)
    m.readshapefile('./DEU_adm/DEU_adm1', 'adm_1', drawbounds=True)  # plots the state boundaries, read explanation below code
    m.drawcoastlines()
    # Main city coordinates
    FRAx, FRAy = m(8.663785, 50.107149)
    BERx, BERy = m(13.369549, 52.525589)
    MUCx, MUCy = m(11.558339, 48.140229)
    STUx, STUy = m(9.181636, 48.784081)
    HAMx, HAMy = m(9.997434, 53.557110)
    HANx, HANy = m(9.741017, 52.376764)
    NURx, NURy = m(11.082989, 49.445615)
    LEIx, LEIy = m(12.383333, 51.346546)
    CGNx, CGNy = m(6.967206, 50.941312)
    DUSx, DUSy = m(6.794317, 51.219960)
    ESSx, ESSy = m(7.014795, 51.451351)
    DORx, DORy = m(7.459294, 51.517899)
    DUIx, DUIy = m(6.775907, 51.429786)
    KARx, KARy = m(8.402181, 48.993512)
    

    # plot each route with its color depending on the number of flights
    for i, route in enumerate(routes):
        route = route[1]
        if absolute:
            color = cmap(int(routes[i].number))
        else:
            color = cmap(i * 1.0 / num_routes)

        line, = m.drawgreatcircle(routes[i].dep_long, routes[i].dep_lat,
                                  routes[i].arr_long, routes[i].arr_lat,
                                  linewidth=0.5, color=color)
        # if the path wraps the image, basemap plots a nasty line connecting
        # the points at the opposite border of the map.
        # we thus detect path that are bigger than 30km and split them
        # by adding a NaN
        path = line.get_path()
        cut_point, = np.where(np.abs(np.diff(path.vertices[:, 0])) > 30000e3)
        if len(cut_point) > 0:
            cut_point = cut_point[0]
            vertices = np.concatenate([path.vertices[:cut_point, :],
                                      [[np.nan, np.nan]],
                                      path.vertices[cut_point+1:, :]])
            path.codes = None  # treat vertices as a serie of line segments
            path.vertices = vertices

    # Plot the main cities on the map
    plt.plot(FRAx, FRAy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(BERx, BERy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(MUCx, MUCy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(STUx, STUy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(HAMx, HAMy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(HANx, HANy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(NURx, NURy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(LEIx, LEIy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(CGNx, CGNy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(DUSx, DUSy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(ESSx, ESSy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(DORx, DORy, mfc='w', mec='k', marker='s', markersize=5)
    plt.plot(KARx, KARy, mfc='w', mec='k', marker='s', markersize=5)
    
    plt.text(FRAx+ 20000, FRAy+ 20000,'Frankfurt')
    plt.text(BERx+ 20000, BERy+ 20000, 'Berlin')
    plt.text(MUCx+ 20000, MUCy+ 20000, 'Muenchen')
    plt.text(STUx+ 20000, STUy- 20000, 'Stuttgart')
    plt.text(HAMx+ 20000, HAMy+ 20000, 'Hamburg')
    plt.text(HANx+ 20000, HANy+ 20000, 'Hannover')
    plt.text(NURx+ 20000, NURy+ 20000, 'Nuernberg')
    plt.text(LEIx+ 20000, LEIy+ 20000, 'Leipzig')
    plt.text(CGNx+ 20000, CGNy- 20000, 'Koeln')
    plt.text(DUSx+ 20000, DUSy- 15000, 'Dusseldorf')
    plt.text(ESSx+ 10000, ESSy- 20000, 'Essen')
    plt.text(DORx+ 20000, DORy+ 20000, 'Dortmund')
    plt.text(KARx+ 20000, KARy+ 00000, 'Karlsruhe')
    
    plt.savefig(out_filename, format='png', bbox_inches='tight')
    plt.close()


def createRoutes(data):
    '''
    Diese Methode erstellt ein DataFrame und eine Liste, in der die Routen aller TwitterUser erstellt werden.
    DatenHeader: departure, dep_long, dep_lat, arrival, arr_long, arr_lat
    '''
    data.createOrReplaceTempView("TwitterTweets")
    tmp = spark.sql("SELECT user.id AS id, place.name, count(place), count(id) AS Number FROM TwitterTweets GROUP BY created_at, user.id, place.name  HAVING count(place.name)>1 AND count(id)>1 ORDER BY id")

    tmp.createOrReplaceTempView("tweets")
    listOfIDs = spark.sql("SELECT tweets.id, count(tweets.id) AS Anz_Orte FROM tweets GROUP BY tweets.id HAVING count(tweets.id)>1 ").collect()

    tmpTime = time.localtime()[0:5]
    print("Insgesamt werden " + str(len(listOfIDs)) + " Daten verarbeitet. Aktuelle Uhrzeit: " + str(tmpTime))
    traffic = []
    global geoLocation
    counter = 0
    for i in listOfIDs:
        counter += 1
        user = data.filter(data['user.id'] == i[0])
        user.createOrReplaceTempView("tmp")
        route = spark.sql("SELECT created_at AS date, user.id AS id, place.country AS country, place.name AS name, place.bounding_box.coordinates[0][0][0] AS long, place.bounding_box.coordinates[0][0][1] AS lat, place.bounding_box.coordinates[0][2][0] AS long2, place.bounding_box.coordinates[0][1][1] AS lat2 From tmp")
        route.createOrReplaceTempView("routes")
        route = spark.sql("SELECT * FROM routes WHERE country == 'Deutschland'").collect()
        
        
        tmpTime = time.localtime()[0:5]
        if (len(listOfIDs) - counter) % 100 == 0:
            print(str(len(listOfIDs) - counter) + " Time: " + str(tmpTime))
        if (len(listOfIDs) - counter) <= 10:
            print(len(listOfIDs) - counter)
        for i in range(1, len(route) -1):    
            if route[i].name != route[i+1].name:
                traffic.append((route[i].name , (route[i].long+route[i].long2)/2,                                 (route[i].lat+route[i].lat2)/2, route[i+1].name ,                                (route[i+1].long+route[i+1].long2)/2, (route[i+1].lat+route[i+1].lat2)/2))
                if route[i].name not in geoLocation == True:
                    geoLocation.append(traffic[-1])
    return traffic

def cityChecker(trafficListe):
    dataListe = []
    for i in range(len(trafficListe)):
        if trafficListe[i][0] == 'Frankfurt am Main':
            dataListe.append(('Frankfurt am Main', 8.663785, 50.107149, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))
            
        elif trafficListe[i][0] == 'Berlin':
            dataListe.append(('Berlin', 13.369549, 52.525589, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))
                                
        elif trafficListe[i][0] == 'München':
            dataListe.append(('München', 11.558339, 48.140229, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                  
        elif trafficListe[i][0] == 'Stuttgart':
            dataListe.append(('Stuttgart', 9.181636, 48.784081, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                    
        elif trafficListe[i][0] == 'Hamburg':
            dataListe.append(('Hamburg', 9.997434, 53.557110, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                    
        elif trafficListe[i][0] == 'Hannover':
            dataListe.append(('Hannover', 9.741017, 52.376764, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                 
        elif trafficListe[i][0] == 'Nürnberg':
            dataListe.append(('Nürnberg', 11.082989, 49.445615, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                       
        elif trafficListe[i][0] == 'Leipzig':
            dataListe.append(('Leipzig', 12.383333, 51.346546, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                   
        elif trafficListe[i][0] == 'Köln':
            dataListe.append(('Köln', 6.967206, 50.941312, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                   
        elif trafficListe[i][0] == 'Düsseldorf':
            dataListe.append(('Düsseldorf', 6.794317, 51.219960, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
              
        elif trafficListe[i][0] == 'Essen':
            dataListe.append(('Essen', 7.014795, 51.451351, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
             
        elif trafficListe[i][0] == 'Dortmund':
            dataListe.append(('Dortmund', 7.459294, 51.517899, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                    
        elif trafficListe[i][0] == 'Duisburg':
            dataListe.append(('Duisburg', 6.775907, 51.429786, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
                        
        elif trafficListe[i][0] == 'Karlsruhe':
            dataListe.append(('Karlsruhe', 8.402181, 48.993512, trafficListe[i][3],                                  trafficListe[i][4], trafficListe[i][5]))            
             
        elif trafficListe[i][3] == 'Frankfurt am Main':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 8.663785, 50.107149))            
             
        elif trafficListe[i][3] == 'Berlin':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 13.369549, 52.525589))            
                     
        elif trafficListe[i][3] == 'München':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 11.558339, 48.140229))            
                  
        elif trafficListe[i][3] == 'Stuttgart':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 9.181636, 48.784081))            
                  
        elif trafficListe[i][3] == 'Hamburg':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 9.997434, 53.557110 ))            
                 
        elif trafficListe[i][3] == 'Hannover':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 9.741017, 52.376764 ))            
            
        elif trafficListe[i][3] == 'Nürnberg':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 11.082989, 49.445615 ))            
                       
        elif trafficListe[i][3] == 'Leipzig':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 12.383333, 51.346546 ))            
               
        elif trafficListe[i][3] == 'Köln':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 6.967206, 50.941312 ))            
                
        elif trafficListe[i][3] == 'Düsseldorf':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 6.794317, 51.219960 ))            
            
        elif trafficListe[i][3] == 'Essen':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 7.014795, 51.451351 ))            
            
        elif trafficListe[i][3] == 'Dortmund':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 7.459294, 51.517899 ))            
                    
        elif trafficListe[i][3] == 'Duisburg':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 6.775907, 51.429786 ))            
                        
        elif trafficListe[i][3] == 'Karlsruhe':
            dataListe.append((trafficListe[i][0], trafficListe[i][1], trafficListe[i][2],                                 trafficListe[i][3], 8.402181, 48.993512 ))            
        
        else:
            dataListe.append(trafficListe[i])
    return dataListe







tmpTime = time.localtime()[0:5]
print("Beginn der Berechnungen: " + str(tmpTime) )

cleanData = data.filter("user.id <> 21033096 AND user.id <> 2816571582 AND place.country <> 'Belgique' AND place.country <> 'The Netherlands' AND place.country <> 'Nederland' AND place.country <> 'France' AND place.country <> 'Schweiz' AND place.country <> 'Switzerland' AND place.country <> 'Czech Republic' AND place.country <> 'Österreich' AND place.country <> 'Austria' AND place.country <> 'Belgium' AND place.country <> 'Holanda' AND place.country <> 'Luxembourg' ")

print(cleanData.count())
print(data.count())

geoLocation = []

traffic = cityChecker(createRoutes(cleanData))

tmpTime = time.localtime()[0:5]
print("Die Variable traffic berechnet und umfasst " + str(len(traffic)) + " Eintraege. Time: " + str(tmpTime) )

#Hier machen wir aus einer Liste wieder ein DataFrame
test = spark.createDataFrame(traffic, ['departure','dep_long', 'dep_lat','arrival', 'arr_long', 'arr_lat'])
test.count()
test.createOrReplaceTempView("test")
foo = spark.sql("SELECT departure, dep_long, dep_lat, arrival, arr_long, arr_lat, count(*) AS number FROM test GROUP BY departure, dep_long, dep_lat, arrival, arr_long, arr_lat ORDER BY number DESC")
tmpTime = time.localtime()[0:5]
print("Erzeuge nun die tweets_evaluated.json." + " Time: " + str(tmpTime))
fobj = open("tweets_evaluated.json", "w")
fooList = foo.collect()
for entry in fooList:
    fobj.write(str(entry) + "\n")
fobj.close()
#print("Die JSON-Datei wurde erzeugt.")
plot_map(foo, color_mode='screen', out_filename='tweets_relative_complete.png')
plot_map(foo, color_mode='screen', out_filename='tweets_absolute_complete.png', absolute=True)

#Plotte Map ohne Fra <-> Bln Strecke
FraBln = foo.filter("number <= 2000")
plot_map(FraBln, out_filename='test.png')
