import requests
import json
import time
import pandas as pd
import datetime
import warnings
import numpy as np
import copy
warnings.filterwarnings("ignore")

session = requests.Session()
session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=3, pool_block=True))
def getdata_api(url,dataTagId,startEpoch,endEpoch):
    body = {
        "metrics": [],
        "cache_time": 0,
        "start_absolute": startEpoch, 
        "end_absolute": endEpoch
    }

    query = {
                "tags": {},
                "name": dataTagId,
                "aggregators": [
            {
              "name": "avg",
              "sampling": {
                "value": "1",
                "unit": "seconds"
              }
            }
          ]
        }

    body['metrics'].append(query)
    #print(body)

    #print (url, body)
    res = session.post(url=url,json=body)
    print(res.content)
    custJson = json.loads(res.content)
    df = {}
    df["time"]=[f[0] for f in custJson['queries'][0]['results'][0]['values']]
    for cust in custJson['queries']:
        df[ cust['results'][0]["name"]]=[f[1] for f in cust['results'][0]['values']]

    df = pd.DataFrame(df)
    sublist=df[["time",dataTagId[0]]].values.tolist()
    
    return sublist

def postDataApi(outputTag,store_vals_to_post):
    url = "http://10.36.141.32:8080/api/v1/datapoints/query"
    # url="http://20.228.168.6//kairosapi/api/v1/datapoints"
    batch_size = 10000
    for i in range(0, len(store_vals_to_post), batch_size):
        batch = store_vals_to_post[i:i+batch_size]
        body = [{
            "name": str(outputTag),
            "datapoints": batch,
            "tags":{"type":"historic"}}]
        res = requests.post(url = url,json = body,stream=True)
        time.sleep(1)
    return res.status_code

d1= [
    "HRD_3LAB40CT003.daca.pv",
    "HRD_3LCA30CT002.daca.pv",
    "HRD_3LCH40CT001.daca.pv",
    "HRD_3LCA30CT004.daca.pv",
    "HRD_3LCA30CT003.daca.pv",
    "HRD_3LAB11CT001.daca.pv",
    "HRD_3LCA30CG005.daca.pv",
    "HRD_3LAB21CT001.daca.pv",
    "HRD_3LCA30CT001.daca.pv",
    "HRD_3LAB40CT003.daca.pv",
    "HRD_3LCA30CG005.daca.pv",
    "HRD_3LCA30CT002.daca.pv",
    "HRD_3LCA30CT001.daca.pv",
    "HRD_3LCA30CT004.daca.pv",
    "HRD_3LAB21CT001.daca.pv",
    "HRD_3LCA30CT003.daca.pv",
    "HRD_3LAB11CT001.daca.pv",
    "HRD_3LCH40CT001.daca.pv"
]

d2 = [
    "HRD_3LAB40CT003.DACA.PV",
    "HRD_3LCA30CT002.DACA.PV",
    "HRD_3LCH40CT001.DACA.PV",
    "HRD_3LCA30CT004.DACA.PV",
    "HRD_3LCA30CT003.DACA.PV",
    "HRD_3LAB11CT001.DACA.PV",
    "HRD_3LCA30CG005.DACA.PV",
    "HRD_3LAB21CT001.DACA.PV",
    "HRD_3LCA30CT001.DACA.PV",
    "HRD_3LAB40CT003.DACA.PV",
    "HRD_3LCA30CG005.DACA.PV",
    "HRD_3LCA30CT002.DACA.PV",
    "HRD_3LCA30CT001.DACA.PV",
    "HRD_3LCA30CT004.DACA.PV",
    "HRD_3LAB21CT001.DACA.PV",
    "HRD_3LCA30CT003.DACA.PV",
    "HRD_3LAB11CT001.DACA.PV",
    "HRD_3LCH40CT001.DACA.PV"
]


data_url = 'http://10.36.141.32:8080/api/v1/datapoints/query' 
start_epoch = int(datetime.datetime(2020,1,1,0,0,0).strftime('%s'))*1000
end_epoch = int(datetime.datetime(2023,7,4,12,30,59).strftime('%s'))*1000

for i in range(0,len(d1)):
    sublist=getdata_api(url = data_url,dataTagId=[d1[i]],startEpoch = start_epoch,endEpoch=end_epoch)
    resp = postDataApi(d2[i],sublist)