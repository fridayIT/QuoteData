#! /usr/bin 
#coding=utf8
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
try:
	from setting import MONGO_IP, MONGO_PORT, HCODE_REQ_AUTH_KEY, HCODE_REQ_LOGIN, HCODE_REQ_QUOTE, HCODE_REQ_QUOTE_LIST, HCODE_REQ_HISTORY, HCODE_REQ_REAL_TIME_QOUTE, HCODE_REQ_LOGOUT, HCODE_RES_QUOTE, HCODE_RES_REAL_TIME_QOUTE ,USERNAME, PASSWD, RCODE_RECONNECTION, QOUTE_YESTERDAY
except Exception:
	QOUTE_YESTERDAY = datetime.now().strftime('%Y-%m-%d ')




client = MongoClient(MONGO_IP, MONGO_PORT) 
client.auth()
dbname = client['yunsoft_qoute']
dbname.authenticate(USERNAME, PASSWD)
quo_collection = dbname['qoute_tick']


def thr_insert_quote(*args,**kwargs):
	'''
	将数据导入到mongo数据库中
	'''
	try:
		db, data = args
		#db.insert_many(data)
	except Exception as e:
		print e


date = datetime.now().strftime('%Y-%m-%d ')
def thr_getdate():
	'''
	在晚上11点时睡眠1小时以上后对QOUTE_YESTERDAY对象进行更新 
	'''
	global QOUTE_YESTERDAY
	while 1:
		time.sleep(60 * 30)
		if (datetime.now().strftime('%H') == '23'):
			time.sleep(60 * 60 + 3)
			QOUTE_YESTERDAY = datetime.now().strftime('%Y-%m-%d ')

time_thr = Thread(target=thr_getdate)
time_thr.daemon = True
time_thr.start()


def create_daemon():
	'''
	脱离会话·ID,转化为守护进程
	'''
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
		#重定向标准输入流、标准输出流、标准错误
		sys.stdout.flush()
		sys.stderr.flush()
		si = file("/dev/null", 'r')
		so = file("/dev/null", 'a+')
		se = file("/dev/null", 'a+', 0)
		os.dup2(si.fileno(), sys.stdin.fileno())
		os.dup2(so.fileno(), sys.stdout.fileno())
		os.dup2(se.fileno(), sys.stderr.fileno())

#备胎服务器
address = [('120.25.93.6', 9998),('120.25.93.6', 9999),('120.25.93.6', 8255), ('120.25.93.6', 9996)]

class Esqoutecli(object):
	def __init__(self,  key):
		self.connfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
		self.key_id = key
		self.sid = ''

	def connect_(self, address):
		for i in address:
			try:
				self.connfd.connect(i)
				break
			except Exception as e:
				print e

	def re_connect(self):
		"""
		    重新修复连接
		"""
		try:
			self.connfd.close()	    
			time.sleep(2)
			self.connfd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
			self.connfd.connect_(address)  
		except  Exception as e:
			print e

	def relogin(self):
		'''
		    重新登录
		'''
		self.re_connect()
		self.auth_key()
		self.login()

	#login
	def login(self, username='', passwd=''):
		if username == '':
			username = self.username
		else:
			self.username = username
		if passwd == '':
			passwd = self.passwd
		else:
			self.passwd = passwd
		send_buf = struct.pack('ii128s128s', HCODE_REQ_LOGIN, 128*2, username, passwd)
		#print 'send login data'

		time.sleep(1)
		self.connfd.send(send_buf)

		re_data = self.connfd.recv(8)
		cmd, get_len = struct.unpack('ii', re_data)
		re_data = self.connfd.recv(get_len)

		_format = 'i39s' 
		cmd, sid = struct.unpack(_format, re_data)
		self.sid = sid[0:-1]
		return self.sid

	#Auth Key
	def auth_key(self, sha_key=''):
		if sha_key != '':
			self.key_id = sha_key
		send_len = len(self.key_id)
		_format = 'ii%ds' % (send_len,)
		send_buf = struct.pack(_format, HCODE_REQ_AUTH_KEY, send_len, self.key_id)
		self.connfd.send(send_buf)
		
	def _market_oper(self, Market, Code, need):
		m_id_len = len(self.sid)
		_format = '=ii%dsi40s66s' % (m_id_len,)
		send_len = struct.calcsize(_format) - 8
		send_buf = struct.pack(_format, HCODE_REQ_QUOTE, send_len, self.sid, need, Market, Code)
		self.connfd.send(send_buf)

	#Take Quote
	def take_quote(self, Market, Code):
		self._market_oper(Market, Code,  1)

	def cancel_take_quote(self, Market, Code):
		self._market_oper(Market, Code, 0)

	def recv(self, get_len):
		try:
			data = self.connfd.recv(get_len)
			return data
		except Exception as e:
			print e
	

def un_pack_recv_data(quo_data):
	'''
		对接收到的行情进行数据解包，解包后拼接日期，与系统时间比较，正常情况下相差不超1小时(可调整)
	'''
	_format = 'ffffffffffff'
	_format2 = 'fffffifffffffffff'	
	Market, Code = struct.unpack('=40s66s', quo_data[:106])
	YClose, YSettle, Open, High, Low, New, NetChg, Markup, Swing, Settle, Volume, Amount = struct.unpack(_format, quo_data[106:154])
	#这些数据暂时不储存数据库
	APP = struct.unpack('40f', quo_data[154:314])

	#ASK, AskVol, Bid, BidVol = struct.unpack('10f10f10f10f', quo_data[154:314])
	#print repr(quo_data)
	AvgPrice, LimitUp, LimitDown, HH, HL, YOPI, ZXSD, JXSD, CJJE, TCloes, Lastvol, status, updatetime, BestBPrice, BestBVol, BestSPrice, BestSVol = struct.unpack(_format2, quo_data[314:])
	insert_sql = {}
	market = Market.strip('\0')
	code = Code.strip('\0')
	insert_sql['market'] = market
	insert_sql['code'] = code
	insert_sql['yclose'] = YClose 
	insert_sql['ysettle'] = YSettle 
	insert_sql['open'] = Open 
	insert_sql['high'] = High 
	insert_sql['low'] = Low 
	insert_sql['new'] = New 
	insert_sql['netchg'] = NetChg
	insert_sql['markup'] = Markup 
	insert_sql['swing'] = Swing
	insert_sql['settle'] = Settle
	insert_sql['volume'] = Volume
	insert_sql['amount'] = Amount
	insert_sql['avgprice'] = AvgPrice
	insert_sql['limitup'] = LimitUp
	insert_sql['limitdown'] = LimitDown
	insert_sql['historyhigh'] = HH
	insert_sql['historylow'] = HL
	insert_sql['YOPI'] = YOPI
	insert_sql['ZXSD'] = ZXSD
	insert_sql['JXSD'] = JXSD
	insert_sql['CJJE'] = CJJE
	insert_sql['tclose'] = TCloes
	insert_sql['lastvol'] = Lastvol
	insert_sql['status'] = status

	updatetime = int(updatetime)
	if updatetime < 0 or New < 0:
		return None
	updatetime = str(updatetime)
	while 1:
		if len(updatetime) < 6:
			updatetime = '0'+ updatetime
		else:
			break
	hour_min_sec = updatetime[:2]+':'+updatetime[2:4]+':'+updatetime[4:6]
	updatetime =  datetime.now().strftime('%Y-%m-%d ') + hour_min_sec
	updatetime = datetime.strptime(updatetime,'%Y-%m-%d %H:%M:%S')
	if updatetime.hour - datetime.now().hour > 1:
		updatetime = QOUTE_YESTERDAY + hour_min_sec
		updatetime = datetime.strptime(updatetime,'%Y-%m-%d %H:%M:%S')
	#print updatetime, Market, Code

	insert_sql['updatetime'] = updatetime
	insert_sql['bestbprice'] = BestBPrice
	insert_sql['bestbvol'] = BestBVo
	insert_sql['bestsprice'] = BestSPrice
	insert_sql['bestsvol'] = BestSVol
	
	return insert_sql



#Recv Quote
def recv_quote(*args, **kwargs):
	'''
	    实时接收数据，对接到的数据进行分类处理 
	'''
	cli = args[0]
	insert_mary = [[],[],[],[],[],[],[],[],[],[]]
	index = 0
	cmd , get_len = 0, 0
	i = 0
	while 1:
		re_data = cli.recv(8)
		try:
			cmd, get_len = struct.unpack('ii', re_data)
		except Exception as e:
			print e
		
		if cmd == HCODE_RES_REAL_TIME_QOUTE:
			try :
				quo_data = cli.recv(get_len)
				insert_sql = un_pack_recv_data(quo_data) 
				if (insert_sql == None):
					continue
			except Exception as e:
				print e
			
			insert_mary[index].append(insert_sql)

			i+=1
			if i == 5:
				thr = Thread(target=thr_insert_quote,args=(quo_collection, insert_mary[index]))
				thr.daemon = True
				thr.start()
				new_list = []

				insert_mary[index % 9] = new_list
				index += 1
				i = 0
			if index == 9:
				index = 0
		elif cmd == HCODE_RES_QUOTE:
			data = cli.recv(get_len)
		elif cmd == RCODE_RECONNECTION:
			cli.relogin(address)
			cli.take_quote("HKIF", "NYMEX CL 1605")
			cli.take_quote("HKIF", "HKIF HSI 1603")
			cli.take_quote("HKIF", "HKIF HSI 1604")
			time.sleep(3)
		else :
			data = cli.recv(get_len)
			
#Onrecv_quote
if __name__ == '__main__':
	create_daemon()
	key_id = '61oETzKXQAGaYdJFuYh7EQnp2XdTP1oV'
	escli = Esqoutecli(key_id)
	escli.connect_(address)
	escli.auth_key(key_id)
	if escli.login('csqoute', 'Vz3023~F') != '':
		print 'login succuess.'
	
	recv_thr = Thread(target=recv_quote, args=(escli,))
	recv_thr.daemon = True
	recv_thr.start()
	#	如果服务器存在定时服务,重复订阅是为了保持连接持久性
	while 1:
		escli.take_quote("HKIF", "NYMEX CL 1605")
		escli.take_quote("HKIF", "HKIF HSI 1603")
		escli.take_quote("HKIF", "HKIF HSI 1604")
		time.sleep(1*3600)
