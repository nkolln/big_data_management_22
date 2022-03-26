from ipaddress import ip_address, ip_network
import ipaddress
import pandas as pd
from pyspark.sql import *
import collections

def summation(x,y):
    for i in range(len(x)):
        pass

df = pd.read_csv("many_addresses.txt")
print(df)
print(ip_address(df.iloc[:, 1]))
dct = collections.defaultdict(list)
#dct = {}
print(df.shape[0])
for i in range(5000):
    key = df.iloc[i][0]
    if key not in dct.keys():
        dct.update({key:[(int(df.iloc[i][1]))]})
    else:
        #dict.setdefault(key,[]).append(ip_address(int(df.iloc[i][1])))
        dct[key].append((int(df.iloc[i][1])))
print(dct)
lst_keys = list(dct.keys())
print(lst_keys)
mode = -1
dct_vals = {}
for key in lst_keys:
    lst_keys.remove(key)
    for val in dct[key]:
        val_s = val
        for key2 in lst_keys:
            for val2 in dct[key2]:
                val2_s = val2
                val = str(val_s)
                val2 = str(val2)
                len1 = len(val)
                len2 = len(val2)
                if len2>len1:
                    len1 = len2
                sum=0
                for i in range(len1):
                    if len(val) ==0:
                        val = '0'
                    if len(val2) ==0:
                        val2 = '0'
                    sum = sum + int(val[-1]) * int(val2[-1])
                    #print(str(sum)+' '+str(val[-1])+ ' '+str(val2[-1]))
                    val = val[:-1]
                    val2 = val2[:-1]
                dct_vals.update({(val_s, val2_s):sum})
print({k:v for (k,v) in dct_vals.items() if v > 400})
print(max(dct_vals, key=dct_vals.get))
print(dct_vals[max(dct_vals, key=dct_vals.get)])