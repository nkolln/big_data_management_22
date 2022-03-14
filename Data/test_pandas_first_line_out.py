import pandas as pd

txt_file = pd.read_csv("Data/MS1.txt", nrows=100)
txt_file.to_csv('head1.csv')
