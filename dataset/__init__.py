# -*- coding: utf-8 -*-
import os
import pandas as pd

file_list = os.listdir('./mobile_dataset/')
df_cdrs = pd.DataFrame({})
for i, f in enumerate(sorted(file_list)):
    if 'sms-call-internet-mi-' in f:
        df = pd.read_csv('./mobile_dataset/' + f, parse_dates=['datetime'])

        df = df.fillna(0)
        df['sms'] = df['smsin'] + df['smsout']
        df['call'] = df['callin'] + df['callout']
        df['internet'] = df['internet']

        print(df.head())

        df_cdrs_hour = df[['datetime', 'CellID', 'sms', 'call', 'internet']].groupby(
            ['datetime', 'CellID'], as_index=False).sum()
        df_cdrs_hour['hour'] = df_cdrs_hour.datetime.dt.hour + 24 * (
                df_cdrs_hour.datetime.dt.day - 1) + 30 * (df_cdrs_hour.datetime.dt.month - 11) * 24
        df_cdrs_hour = df_cdrs_hour[['hour', 'CellID', 'sms', 'call', 'internet']].groupby(
            ['hour', 'CellID'], as_index=False).sum()

        df_cdrs_hour = df_cdrs_hour.set_index(['hour']).sort_index()

        df_cdrs = pd.concat([df_cdrs, df_cdrs_hour])
        print(f)

print('Data load done')
df_cdrs.to_csv("crawled_feature.csv")
