#! /usr/bin 
import os
import sys
import struct
import socket  
import time
import binascii
import pymongo
from pymongo import MongoClient
from threading import Thread
from datetime import datetime

HCODE_REQ_AUTH_KEY = 0x1001
HCODE_REQ_LOGIN	= 0x1002
HCODE_REQ_QUOTE = 0x1003
HCODE_REQ_QUOTE_LIST=0x1004	
HCODE_REQ_HISTORY=0x1005
HCODE_REQ_REAL_TIME_QOUTE=0x1006
HCODE_REQ_LOGOUT=0x1009

HCODE_RES_QUOTE=0x2013
HCODE_RES_REAL_TIME_QOUTE=0x2016


###########################################

client = MongoClient() 

dbname = client['yunsoft_qoute']
Quo_collection = dbname['quote__tick']
###########################################

def thr_insert_quote(*args,**kwargs):
	try:
		db, data = args
		db.insert_many(data)
	except Exception as e:
		print e

date = datetime.now().strftime('%Y-%m-%d ')
def thr_getdate():
	while 1:
		if (datetime.now().strftime('%H') == '23'):
			while 1:
				date = datetime.now().strftime('%Y-%m-%d ')
				time.sleep(60*5)
				#print date
				if (datetime.now().strftime('%H') == '00'):
					break
		else:
			time.sleep(3600*1)

time_thr = Thread(target=thr_getdate)
time_thr.daemon = True
time_thr.start()

address = ('120.25.93.6', 9999)  
#address = ('192.168.1.6', 9999)
connfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
connfd.connect(address)  

#Auth Key
key_id = '61oETzKXQAGaYdJFuYh7EQnp2XdTP1oV'
def auth_key(key_id):
	send_len = len(key_id)
	Format = 'ii%ds' % (send_len,)
	send_buf = struct.pack(Format, HCODE_REQ_AUTH_KEY, send_len, key_id)
	connfd.send(send_buf)

#login
def login(username, passwod):
	global sid
	send_buf = struct.pack('ii128s128s', HCODE_REQ_LOGIN, 128*2, username, passwod)
	print 'send login data'

	time.sleep(1)
	connfd.send(send_buf)

	re_data = connfd.recv(8)
	cmd, get_len = struct.unpack('ii', re_data)
	re_data = connfd.recv(get_len)

	Format = 'i39s' 
	cmd, sid = struct.unpack(Format, re_data)
	sid = sid[0:-1]
	return sid



def market_oper(Market, Code, sid, need):
	m_id_len = len(sid)
	Format = '=ii%dsi40s66s' % (m_id_len,)
	send_len = struct.calcsize(Format) - 8
	send_buf = struct.pack(Format, HCODE_REQ_QUOTE, send_len, sid, need, Market, Code)
	connfd.send(send_buf)

#Take Quote
def take_quote(Market, Code, sid):
	market_oper(Market, Code, sid, 1)

def cancel_take_quote(Market, Code, sid):
	market_oper(Market, Code, sid, 0)
	
def create_daemon():
  	pid = os.fork()  
  	if pid == 0:
   		os.setsid()
		if pid == 0:
			os.chdir('./daemon')
			os.umask(0)  
		else:
			os._exit(0)
	else:
		os._exit(0)

def un_pack_recv_data(quo_data):
	Format = 'ffffffffffff'
	Format2 = 'fffffifffffffffff'	
	Market, Code = struct.unpack('40s66s', quo_data[:106])
	YClose, YSettle, Open, High, Low, New, NetChg, Markup, Swing, Settle, Volume, Amount = struct.unpack(Format, quo_data[106:154])

	ASK = struct.unpack('10f', quo_data[154:194])
	AskVol = struct.unpack('10f', quo_data[194:234])
	Bid = struct.unpack('10f', quo_data[234:274])
	BidVol = struct.unpack('10f', quo_data[274:314])
	AvgPrice, LimitUp, LimitDown, HH, HL, YOPI, ZXSD, JXSD, CJJE, TCloes, Lastvol, status, updatetime, BestBPrice, BestBVol, BestSPrice, BestSVol = struct.unpack(Format2, quo_data[314:])

	insert_sql = {}
	#print repr(quo_data)
	insert_sql['Market'] = Market
	insert_sql['Code'] = Code 
	insert_sql['YClose'] = YClose 
	insert_sql['YSettle'] = YSettle 
	insert_sql['Open'] = Open 
	insert_sql['High'] = High 
	insert_sql['Low'] = Low 
	insert_sql['New'] = New 
	insert_sql['NetChg'] = NetChg
	insert_sql['Markup'] = Markup 
	insert_sql['Swing'] = Swing
	insert_sql['Settle'] = Settle
	insert_sql['Volume'] = Volume
	insert_sql['Amount'] = Amount
	insert_sql['AvgPrice'] = AvgPrice
	insert_sql['LimitUp'] = LimitUp
	insert_sql['LimitDown'] = LimitDown
	insert_sql['HistoryHigh'] = HH
	insert_sql['HistoryLow'] = HL
	insert_sql['YOPI'] = YOPI
	insert_sql['ZXSD'] = ZXSD
	insert_sql['JXSD'] = JXSD
	insert_sql['CJJE'] = CJJE
	insert_sql['TCloes'] = TCloes
	insert_sql['Lastvol'] = Lastvol
	insert_sql['status'] = status


	updatetime = int(updatetime)
	if updatetime < 0:
		return None
	updatetime = str(updatetime)
	if len(updatetime) < 6:
		updatetime = '0'+ updatetime
	updatetime = updatetime[:2]+':'+updatetime[2:4]+':'+updatetime[4:6]
	updatetime = date+updatetime
	updatetime = datetime.strptime(updatetime,'%Y-%m-%d %H:%M:%S')
	#print updatetime, Market, Code

	insert_sql['updatetime'] = updatetime
	insert_sql['BestBPrice'] = BestBPrice
	insert_sql['BestBVol'] = BestBVol
	insert_sql['BestSPrice'] = BestSPrice
	insert_sql['BestSVol'] = BestSVol
	
	return insert_sql

def _recv(get_len):
	try:
		data = connfd.recv(get_len)
	except Exception as e:
		print e
	return data

#Recv Quote
def recv_quote():
	insert_mary = [[],[],[],[],[],[],[],[],[],[]]
	index = 0
	i = 0
	while 1:
		re_data = connfd.recv(8)
		try:
			cmd, get_len = struct.unpack('ii', re_data)
		except Exception as e:
			print e
		
		if cmd == HCODE_RES_REAL_TIME_QOUTE:
			try :
				quo_data = connfd.recv(get_len)
				insert_sql = un_pack_recv_data(quo_data) 
				if (insert_sql == None):
					continue
			except Exception as e:
				print e
			
			insert_mary[index].append(insert_sql)

			i+=1
			if i == 1000:
				thr = Thread(target=thr_insert_quote,args=(Quo_collection, insert_mary[index]))
				thr.daemon = True
				thr.start()
				new_list = []
				insert_mary[index % 9] = new_list
				index += 1
				i = 0
			if index == 9:
				index = 0
		elif cmd == HCODE_RES_QUOTE:
			data = _recv(get_len)
		elif cmd == 8211:
			data = _recv(get_len)
		else :
			data = _recv(get_len)
			
#Onrecv_quote
if __name__ == '__main__':
	#create_daemon()
	auth_key(key_id)
	sid = ''
	m_id_len = 0
	sid = login('csqoute', 'Vz3023~F')
	m_id_len = len(sid)

	recv_thr = Thread(target=recv_quote)
	recv_thr.daemon = True

	take_quote("HKIF", "HKIF HSI 1603", sid)
	take_quote('HKIF', 'NYMEX CL 1604', sid)
	take_quote('HKIF', 'NYMEX CL 1603', sid)
	take_quote('HKIF', 'HKIF MCH 1603', sid)
	take_quote('LME', 'LME LAH 3M', sid)
	take_quote('NYMEX', 'NYMEX HO 1603', sid)

	recv_thr.start()
	while 1:
		take_quote("HKIF", "HKIF HSI 1603", sid)
		time.sleep(3600*2)
	connfd.close()  
