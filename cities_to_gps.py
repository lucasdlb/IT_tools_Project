from geopy.geocoders import Nominatim
geolocator = Nominatim()
import pandas as pd

file = open('department_capital.csv', 'r')
lines = file.read()
cities = lines.split('\n')
del cities[-1]

gps_location_latitude = []
gps_location_longitude = []
for i in range(len(cities)):
    gps_location_latitude.append(geolocator.geocode("%s" % cities[i]).latitude)
    gps_location_longitude.append(geolocator.geocode("%s" % cities[i]).longitude)
                               
cities_location = pd.DataFrame()
cities_location['cities'] = cities
cities_location['latidude'] = gps_location_latitude
cities_location['longitude'] = gps_location_longitude

cities_location.to_csv('cities_location.csv', sep = ',', index = False)
