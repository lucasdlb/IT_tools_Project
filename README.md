### IT_tools_Project
#### Lucas de La Brosse, Ashutosh

The cities_to_gps.py is a simple script to transform a list of cities into gps coordinates file named cities_location.csv not executed on hdfs. The cities_location.csv file must be put into hdfs (here into /tmp/projet_it/weather/cities_location.csv).

The weather_data_recuperation.py download the cities_location.csv file from hdfs (here from /tmp/projet_it/weather/cities_location.csv ) into current execution directory of the python script node's, then it downloads the data from forecast API, select ozone and time_description info, finaly it uses it to create a csv named from the actual day and sent it into hdfs ( here into /tmp/projet_it/weather ) before to delete it from the local execution directory.
remark: needs python 3, and urllib3, pandas, ast, datetime os packages.

The HiveQuery.hql file does the query requested in the project. Big queries should be read from the middle of the query to the begining.

The HiveQuery.xml file is simply created by the oozie UI. 




