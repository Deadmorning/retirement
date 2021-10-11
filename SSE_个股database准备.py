# -*- coding:utf-8 -*-
import pymysql

company_code = 'sz000651'

###create database :company code 
conn = pymysql.connect(host='localhost',user='root',passwd='cucumber',charset='utf8')
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS sz000651")
conn.close()
'''
# creat table：deal detail daily row data & table:deal detail daily review data
conn = pymysql.connect(host='localhost',user='root',passwd='cucumber',db='sz000651')
cursor = conn.cursor()
sz000651_DDD_reviewdata = """CREATE TABLE sz000651_DDD_reviewdata(
	todaydate              date         NOT NULL,
	open_price              real         NOT NULL,
	closed_price             REAL       NOT NULL,
	today_change_precent REAL       NOT NULL,
	today_turnover   REAL       NOT NULL,
	today_volume  REAL       NOT NULL,
	turnover_500K_ration  REAL       NOT NULL,
	buyorder_turnover  REAL       NOT NULL,
	sellorder_sellorder  REAL       NOT NULL,
	net_inflows  REAL       NOT NULL,
	buyorder_turnover_10M_sum  REAL       NOT NULL,
	sellorder_turnover_10M_sum  REAL       NOT NULL,
	buyorder_turnover_5M_sum  REAL       NOT NULL,
	sellorder_turnover_5M_sum  REAL       NOT NULL,
	buyorder_turnover_2M_sum  REAL       NOT NULL,
	sellorder_turnover_2M_sum  REAL       NOT NULL,
	buyorder_turnover_1M_sum  REAL       NOT NULL,
	sellorder_turnover_1M_sum  REAL       NOT NULL,
	buyorder_turnover_500K_sum  REAL       NOT NULL,
	sellorder_turnover_500K_sum  REAL       NOT NULL,
	buyorder_time_0945    REAL       NOT     NULL,
	sellorder_time_0945  REAL       NOT    NULL,
	buyorder_time_1000  REAL       NOT NULL,
	sellorder_time_1000  REAL       NOT NULL,
	buyorder_time_1030  REAL       NOT NULL,
	sellorder_time_1030  REAL       NOT NULL,
	buyorder_time_1000  REAL       NOT NULL,
	sellorder_time_1000  REAL       NOT NULL,
	buyorder_time_1130  REAL       NOT NULL,
	sellorder_time_1130  REAL       NOT NULL,
	buyorder_time_1430   REAL       NOT NULL,
	sellorder_time_1430  REAL       NOT NULL,
	buyorder_time_1500  REAL       NOT NULL,
	sellorder_time_1500  REAL       NOT NULL,
	)"""
# 疯了
cursor.execute(sz000651_DDD_reviewdata)
conn.close()
'''