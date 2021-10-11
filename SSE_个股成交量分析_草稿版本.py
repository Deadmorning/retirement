# -*- coding:utf-8 -*-
import datetime
from datetime import date
from chinese_calendar import is_workday,is_holiday
import akshare as ak
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine

# Summary：
# 以下方法可以作为个股模板，针对选出来的个股进行分析和跟踪：个股选取需要结合财报宏观等再做分析，在其他里会提到；

# The First Step:
# 1.通股现有python第三方模块AKshare Query 个股当天历史分笔成交数据;
# 2.AKshare参考链接：https://akshare-4gize6tod19f2d2e-1252952517.tcloudbaseapp.com/data/stock/stock.html#id14
# 3.目前该新浪接口在交易日收盘后可以Query当天交易数据，接口一般最多Query 最近五天历史分笔成交数据：
# 4.故后续需要每天自动query后，存入数据库，便于By周/月，分析个股'木桶状态和参与人员心态通过每一笔数据'
# 5.同时通过计算当天分笔成交数据也可以计算出一切需要的指标；

deal_detail_daily = ak.stock_zh_a_tick_tx_js(code="sz000063")

# The Second Step:

# 1.创建database，表明：DDD_SZ00063,(DDD为Deal_detail_daily缩写)
# 2.添加自动生成database功能
db = pymysql.connect(host='localhost',user='root',passwd='*******',db='DDD_sz000063')
cursor.execute("CREATE DATABASE DDD_sz000063")
cursor = db.cursor()

# 2.Query个股分笔数据，存入表中；
# >By 单日期存储表，每个个股database里会有一个处理汇总表，用于存储处理数据（木桶状态和参与人员心态通过每一笔数据）
# >之所以需要存储每个交易日的数据，原因一：便于后续想起来的其他分析方法使用，有数据可用；
# >至于是每个日期做一个表，也是便于后续调用使用；也可以By周，但还在在query出来的DF数据里添加日期，麻烦后续调用也麻烦；

# 3.日期处理如下：
td=datetime.date.today()
#日期调试加减方法：
#td=datetime.date.today()+datetime.timedelta(days=-3)
td_str =td.strftime("%Y%m%d")

# 交易日时间定义方式1：使用chinese_calendar 模块，先判定是否是周末，再判断是否是工作日；
# 因为只有非周末的工作日才交易，假日调休也不交易；
	dt= datetime.date.today()
	dt_num = datetime.date.isoweekday(dt)
	dt_isworkday = is_workday(dt)
	if dt_num <6 and dt_isworkday:
		getdata(dt)

# 4. 使用sqlalchemy模块，创建SQL引擎，再通过panda里 to_sql方法按日期名当作表名写入database；
conn_sz000651 = create_engine('mysql+pymysql://root:*******@localhost/DDD_sz000651',encoding='utf-8')
DDD_sz000651 = ak.stock_zh_a_tick_tx_js(code="sz000651")
DDD_sz000651.to_sql(name=td_str, con=conn_sz000651, if_exists='append', index=False)

# The Third Step
# 1.个股交易数据常规初步处理，存入表名'sz000651'里的总表里，作为下一步分析基础；
# 2.先对DataFrame数据处理.讲可转换的string转换成数字格式，便于基础计算；
data_num = DDD_sz000651.apply(pd.to_numeric,errors='ignore')
data_sum = data_num['成交量'].sum()
# OR：方式如下：data_num = data['成交量'].to_numpy().astype(float).sum()

# 剩余指标待补充；



# The Fourth Step
# 木桶分析，成交心态分析，待补充；


