#! /usr/bin python
#coding=utf8

import sys
import struct
from threading import Thread
from datetime import datetime
from pymongo import MongoClient

try:
    from settings import MONGO_HOST, MONGO_PORT
except:
    MONGO_HOST = "127.0.0.1"
    MONGO_PORT = 27017


def get_title(title, arg, open_file):
	"""
		获取文件首行
	"""
	first_line = open_file.readline()
	first_line = first_line.strip('\r\n')
	first_line = first_line.split(' ')

	for i in first_line:
		i = i.strip(' ')
		#i = i.strip(r'\t')
		if i != '':
			title.append(i.lower())


def thr_insert_quote(*args,**kwargs):
	"""
		线程函数存储数据库
	"""
	try:
		db, data = args
		db.insert_many(data)
	except Exception as e:
		print e, data


def save_qoute(qoute_data, line_data):
	"""
		缓存历史数据
	"""
	try:
		updatetime = datetime.strptime(line_data[4] + ' ' + line_data[5],'%Y-%m-%d %H:%M:%S')

		qoute_data[title[0]] = line_data[0]
		qoute_data[title[1]] = line_data[1] + ' ' + line_data[2] + ' ' +line_data[3]
		qoute_data[title[2]] = updatetime
		qoute_data[title[3]] = line_data[6]
		qoute_data[title[4]] = line_data[7]
		qoute_data[title[5]] = line_data[8]
		qoute_data[title[6]] = line_data[9]
		qoute_data[title[7]] = line_data[10]
		qoute_data[title[8]] = line_data[11] 
	except Exception as e:
		print e


def extract_qoute(title, open_file, Quo_collection):
	"""
		提取文件中的历史数据
	"""
	file_data = open_file.readlines()	
	iResult = 0
	history_qoute = []

	for line in file_data:
		qoute_data = {}
		line_data = line.strip('\r\n').split(' ')

		for count in range(line_data.count('')):
			line_data.remove('')
		save_qoute(qoute_data, line_data)	

		iResult += 1
		history_qoute.append(qoute_data)

		if iResult == 10:
			thr = Thread(target=thr_insert_quote,args=(Quo_collection, history_qoute))
			thr.daemon = True
			thr.start()
			iResult = 0
			history_qoute = []

	if iResult != 0:
		thr = Thread(target=thr_insert_quote,args=(Quo_collection, history_qoute))


if __name__ == '__main__':
	dbname = MongoClient(MONGO_HOST, MONGO_PORT)['yunsoft_qoute']
	col_list = []
	col_list.append(dbname['qoute_minute'])
	col_list.append(dbname['qoute_minute5'])
	col_list.append(dbname['qoute_hour'])
	col_list.append(dbname['qoute_day'])

	i = 0
	sys.argv = sys.argv[1:]
	for arg in sys.argv:
		title = []
		pf = file(arg, r'r')	
		get_title(title, arg, pf)

		print title
		extract_qoute(title, pf, col_list[i])
		i += 1
		pf.close()
