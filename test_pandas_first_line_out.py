import pandas as pd

txt_file = pd.read_csv('C:/Users/nickk/Downloads/weather-data/out_2018.txt', nrows=1000)
txt_file.to_csv('Data/head.csv')
