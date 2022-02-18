import pandas as pd

txt_file = pd.read_csv('Data/MS1.txt', nrows=1000)
txt_file.to_csv('Data/head.csv')
