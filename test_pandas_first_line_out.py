import pandas as pd

txt_file = pd.read_csv("Data/WeatherEvents_Jan2016-Dec2021.csv", nrows=1000)
txt_file.to_csv('Data/head.csv')
