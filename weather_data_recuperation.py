#!/bin/python
import urllib3
import pandas as pd
import ast
import datetime
import os


http = urllib3.PoolManager()
os.system('hadoop fs -get /tmp/projet_it/weather/cities_location.csv %s' % os.getcwd())
cities_location = pd.read_csv('/data_projet_it_tools/cities_location.csv')
print('import cities ok')
columns = ["ozone","time_description"]
index = range(cities_location.shape[0])
weather_data = pd.DataFrame(index = index, columns = columns)


for i in index:
    path = 'https://api.forecast.io/forecast/f7a7f0efa6e79d76fec4f6bbab71812e/%s,%s' % (cities_location.iloc[i][1], cities_location.iloc[i][2])
    r = http.request('GET', path)
    data = ast.literal_eval(r.data.decode('utf8'))
    weather_data.iloc[i] = [data["currently"]["ozone"], data["currently"]["icon"]]
file_name = 'weather_data_%s.csv' % datetime.datetime.now().strftime("%Y-%m-%d")    
weather_data.to_csv(file_name, sep = ',', index = False)

file_path = '%s/%s' % (os.getcwd(), file_name)
print(file_path)
os.system('hadoop fs -put %s /tmp/projet_it/weather' % file_path)
os.system('rm %s' % file_name)
