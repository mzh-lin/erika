import pandas as pd
import datetime
import numpy as np
import baostock as bs
lg = bs.login()
year = '2015'
path =rf'C:\Users\linma\Documents\WeChat Files\wxid_wr6v6m8gzept21\FileStorage\File\2022-02\{year}年定增统计.xlsx'
# tmp = pd.read_excel(path, skiprows=0,index_col=0)
if year == '2015':
    tmp = pd.read_excel(path, skiprows=0, index_col=0)
else:
    tmp = pd.read_excel(path,index_col=0).dropna(how='all',axis=1)
# tmp['发行时间'] = (tmp['发行时间'].str.replace(" ","").str[:12]+tmp['发行时间'].str.replace(" ","").str[-5:]).apply(lambda x :datetime.datetime.strptime(x[:4]+x[5:7]+x[8:10]+' '+x[-5:],"%Y%m%d %H:%M"))
tmp.loc[~tmp['二级市场价格'].isna(),'发行时间'] = tmp.loc[~tmp['二级市场价格'].isna(),'发行时间'].str.replace(" ","").apply(lambda x:x.split('年')[0]+x.split('年')[1].split('月')[0].zfill(2)+x.split('年')[1].split('月')[1].split('日')[0].zfill(2)+' '+x[-5:])
tmp.loc[~tmp['二级市场价格'].isna(),'发行时间'] = tmp.loc[~tmp['二级市场价格'].isna(),'发行时间'].apply(lambda x :datetime.datetime.strptime(x,"%Y%m%d %H:%M"))
tmp["代码"] = tmp["代码"].fillna(method='ffill')
tmp["解禁日期"] = tmp["解禁日期"].fillna(method='ffill')
close_prc = pd.read_hdf('data_min/close_20152016.h5',index_col=0)
def get_price(iterrows):
    if isinstance(iterrows['代码'],float):
        stock_id = str(int(iterrows['代码'])).zfill(6)
        if stock_id[0] == '6':
            stock_id  = 'sh.'+stock_id
        else:
            stock_id  = 'sz.'+stock_id

    else:
        stock_id = iterrows['代码'][-2:].lower()+'.'+iterrows['代码'][:6]
    dt = iterrows['发行时间']
    if np.isnan(iterrows['二级市场价格']):
        return np.nan
    dt_str = (dt - datetime.timedelta(minutes=30)).strftime("%Y%m%d%H%M%S")
    if (dt_str[8:]>"113000") & (dt_str[8:]<"130000"):
        dt_str=dt_str[:8]+"113000"
    if dt_str[8:]>"150000":
        dt_str=dt_str[:8]+"150000"
    dt_date = (dt - datetime.timedelta(minutes=30)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(stock_id,
        "preclose,tradestatus",
        start_date=dt_date, end_date=dt_date,
        frequency="d", adjustflag="3")
    data = rs.get_data()
    if data.iloc[0,1] == '0':
        return data.iloc[0,0]
    else:

        return close_prc.loc[dt_str,stock_id[-6:]].round(2)

def get_free_price(iterrows):
    if isinstance(iterrows['代码'],float):
        stock_id = str(int(iterrows['代码'])).zfill(6)
        if stock_id[0] == '6':
            stock_id  = 'sh.'+stock_id
        else:
            stock_id  = 'sz.'+stock_id

    else:
        stock_id = iterrows['代码'][-2:].lower()+'.'+iterrows['代码'][:6]

    if np.isnan(iterrows['二级市场价格']):
        return np.nan
    dt_date = iterrows['解禁日期'].strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(stock_id,
        "preclose,close,tradestatus",
        start_date=dt_date, end_date=dt_date,
        frequency="d", adjustflag="3")
    data = rs.get_data()
    if data.iloc[0,-1] == '0':
        return np.round(float(data.iloc[0,0]),2)
    else:
        return np.round(float(data.iloc[0,1]),2)

tmp['市场价格'] = [get_price(j) for i,j in tmp.iterrows()]
tmp['市场价格'] = tmp['市场价格'].astype(float).round(2)
tmp['解禁价格'] = [get_free_price(j) for i,j in tmp.iterrows()]
tmp['价格不同'] = (tmp['二级市场价格'].round(2)!=tmp['市场价格'].astype(float).round(2))
tmp.to_csv(f'{year}定增统计check版本.csv')