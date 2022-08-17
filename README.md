# Preparation of weather data for SWAT+

Step 3 in SWAT+ Editor "Edit Inputs and Run SWAT+" requires some weather data.

Purpose of this project is to create all those weather files based on ERA5, IMERG and GFS climate data, using google earth-engine API in Python.

<b>Parameters of script <i>retrieve_station_data.py</i>:</b>
- from_date_string
- to_date_string
- weather_station_list 


<b>Note on memory issues of GEE:</b>

Parameter "interval_size_in_days" cuts retrieval into chunks, to bypass memory issues of GEE.


<b>How to use generated files and folders in SWAT+ Editor:</b>

Weather generator data > Import data
![plot](https://user-images.githubusercontent.com/111283134/185152673-4c06b89b-f217-465f-8bbe-57c56a3ce7c1.png)

Weather stations > Import data > Start
![plot](https://user-images.githubusercontent.com/111283134/185152730-f8e24eeb-348f-49fc-b9f1-b2de5e378497.png)


