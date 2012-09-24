#encoding: utf-8
import httplib
import urllib
import urllib2
import cookielib
import re
import time
import MySQLdb
#from BeautifulSoup import BeautifulSoup
import base64
import json
import types
import sys
import random
'''数据库weibo说明：rootId为0的是原创微博，其余转发微博的rootId为该原创微博的rid。parentId默认为0，即尚未赋值过。'''
DEBUG = True
def debug(*argv):
    if not DEBUG:return
    print argv

class CrawlWeibo:
    
    def __init__(self):
        self.initAccounts()
        self.accountIndex = 0
        self.switchUserAccount()
        
        self.createDatabaseTable()
        
        self.date=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        
        self.log = open("log.txt","a")
        self.log.write("=============start at %s=============" % self.date)
        
    def myUrlopen(self,urlTemplate,*argv):
        count = 0
        while count<5:
            self.checkRemain()
            url = self.getURL(urlTemplate,argv)#保证self.accessToken 变化的时候，url也跟着变化            
            try:
                result = urllib2.urlopen(url).read()
                return result
            except:
                print("in myUrlopen:urlopen error,url is ",url)
                self.log.write("in myUrlopen:urlopen error,url is"+url)
                time.sleep(60)
                count+= 1
        print("give up connection in myUrlopen,url is ",url)
        self.log.write("give up connection in myUrlopen,url is "+url)
        return False
    def getURL(self,urlTemplate,argv):
        '''argv is a tuple'''
        argv = list(argv)
        argv.append(self.accessToken)
        return urlTemplate.format(*argv)
    def getAccountLimit(self):
        try:
            debug('https://api.weibo.com/2/account/rate_limit_status.json?access_token=%s' % self.accessToken)
            data = urllib2.urlopen('https://api.weibo.com/2/account/rate_limit_status.json?access_token=%s' % self.accessToken).read()
            ##print data
            js = json.loads(data)
            print(js['remaining_ip_hits'],js['remaining_user_hits'],js['reset_time_in_seconds'])
            return (js['remaining_ip_hits'],js['remaining_user_hits'],js['reset_time_in_seconds'])
        except:
            print("getAccountLimit exception")
            time.sleep(60)
            self.getAccountLimit()
    def checkRemain(self):
        ipRemain,userRemain,timeRemain = self.getAccountLimit()

        if userRemain==0:
            
            self.log.write("userRemain is 0")
            print("userRemain is 0")
            result = self.switchUserAccount()
            self.log.write("switchUserAccount:"+str(result))
            print("switchUserAccount:"+str(result))
            time.sleep(60) #暂停60s，防止深入递归报错
            self.checkRemain()
        if ipRemain==0:
            self.log.write("ipRemain is 0")
            print("ipRemain is 0")           
            time.sleep(timeRemain+60)

    def crawlWeibo(self,wid):
        '''从原创微博抓起，获取微博转发树结构'''
        parentId = 0
        pageIndex = 1
        rootId = 0
        while True:
            urlTemplate = "https://api.weibo.com/2/statuses/repost_timeline.json?access_token={2}&id={0}&count=200&page={1}"
            isBreak = False
            whileCount = 0
            while True:
                page = self.myUrlopen(urlTemplate,wid,pageIndex)
                if page==False:
                    isBreak = True
                    break
                js = json.loads(page)
                
                if 'reposts' in js and len(js['reposts'])!=0:
                    reposts = js['reposts']
                    break
                if whileCount > 5:
                    isBreak = True
                    break
                
                whileCount += 1
                time.sleep(2*whileCount)
            if isBreak : break
#            page = self.myUrlopen(url)
#            if page!=False:
#                js = json.loads(page)
#                if 'reposts' not in js:
#                    break
#                
#                reposts = js['reposts']
#                ##print page
#                #print len(reposts)
#                if len(reposts)==0:
#                    break
            if rootId == 0:
                for repost in reposts:
                    if "retweeted_status" in repost:
                        retweeted_status = repost["retweeted_status"]
                        rootId = self.insertRetweet2DB(retweeted_status)
                        break
            
            for repost in reposts:
                results =  self.insertNewWeibo2DB(rootId,parentId,repost)
                    
            pageIndex += 1
    def createWeiboTree(self):
        '''读取数据库中的微博，更新其parentId'''
        #count = self.cursor.execute('select rid,rootId,text from weibo where rootId <> 0 order by id asc')
        count = self.cursor.execute('select rid,rootId,text from weibo where rootId <> 0 and parentId = 0 order by id asc')#parentId==0从未处理的数据开始
        for  (rid,rootId,text) in self.cursor.fetchall():
            if not text:continue
            nameTexts = self.weiboParse(text)
            currentId = rid
            #print nameTexts,rid,text
            if nameTexts==None or  len(nameTexts)==0:
                self.updateWeiboParentIdFromDb(currentId,rootId)
                continue
            for nameText in nameTexts:
            	parentId = self.getWidFromDb(nameText,rootId)
                #print parentId,currentId
            	self.updateWeiboParentIdFromDb(currentId,parentId)
            	currentId = parentId
            self.updateWeiboParentIdFromDb(currentId,rootId)

    
    def weiboParse(self,content):
        '''返回上层的作者名 和 微博内容text --- [(name,text),....]'''
    	#result = re.findall(r'//@(.*?)[:：](.*?)',content)#注意会存在用中文冒号：的情况
        index = content.find("//@")
        if index==-1:
            return None
        content = content[index:]
        result = []
        #print '##',content
        lines = content.split(r"//@")
        for line in lines:
            if not line.strip():continue
            match = re.match(r'(.*?)(:|：)(.*)',line)
            if match:
                result.append(match.group(1,3))
        return result
    def getWidFromDb(self,nameText,rootId):
    	self.cursor.execute("select weibo.rid,weibo.text,userinfo.rid from weibo,userinfo where userinfo.name = '%s' and weibo.uid = userinfo.rid" % nameText[0])
        results = self.cursor.fetchall()
        if not results:#用户名不存在
            #print "Nonnononono"
            return self.createDefaultWeibo4None(nameText[0],nameText[1],rootId)
        else:#用户名存在
            for rid,text,uId in results:
                if text.find(nameText[1])!= -1:#存在该微博 返回该微博的rid ，可能更精确的使用text.find(nameText[1])==0来判断
                    return rid
            #如果不存在该微博，但用户名存在
            self.cursor.execute("insert into weibo (rootId,uId,deleted,date,text) values(%s,%s,%s,%s,%s)",(rootId,uId,'default',self.date,nameText[1]))
            self.conn.commit()
            return self.cursor.lastrowid
    def createDefaultWeibo4None(self,screenName,text,rootId):
        url = "https://api.weibo.com/2/statuses/user_timeline.json?access_token=%s&screen_name=%s" %(self.accessToken,screenName)
        page = self.myUrlopen(url)
        js = json.loads(page)
        data = js['statuses']
        if not data:#当该用户名不存在（可能是改名了），设置uId为0
            self.cursor.execute("insert into weibo (rootId,uId,deleted,date,text) values(%s,%s,%s,%s,%s)",(rootId,0,'default',self.date,text))
            self.conn.commit()
            return self.cursor.lastrowid
        else:
            data = js['statuses'][0]['user']
            rid,id = self.insetUserId2DB(data)
            self.cursor.execute("insert into weibo (rootId,uId,deleted,date,text) values(%s,%s,%s,%s,%s)",(rootId,rid,'default',self.date,text))
            self.conn.commit()
            return self.cursor.lastrowid
        
    def updateWeiboParentIdFromDb(self,currentId,parentId):
    	self.cursor.execute("update weibo set parentId = %s where rid = %s",(parentId,currentId))
    	self.conn.commit()
    

    def insertRetweet2DB(self,jsData):
        '''将微博转发的原微博插入数据库'''
        id = jsData['idstr']
        count = self.cursor.execute("select rid from weibo where id = %s",id)
        if count!=0:#原微博存在
            return self.cursor.fetchone()[0]
        
        if 'deleted' in jsData:#原微博已删除
            self.cursor.execute("insert into weibo (rootId,id,mid,text,deleted,created_at,date) values(%s,%s,%s,%s,%s,%s,%s)",(0,jsData['idstr'],jsData['mid'],jsData['text'],jsData['deleted'],jsData['created_at'],self.date))
            self.conn.commit()
            return self.cursor.lastrowid
        
        user = jsData['user']
        rid,uid = self.insetUserId2DB(user)#若该原微博的用户不存在，插入数据库

        
        if 'original_pic' not in jsData:
            jsData['original_pic']=None
        self.cursor.execute("insert into weibo values(0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)",(rid,0,jsData['idstr'],jsData['mid'],jsData['text'],jsData['source'],jsData['favorited'],jsData['truncated'],jsData['original_pic'],jsData['mlevel'],jsData['reposts_count'],jsData['comments_count'],jsData['created_at'],None,self.date))
        self.conn.commit()
        return self.cursor.lastrowid
    def insertNewWeibo2DB(self,rootId,parentId,jsData):
        '''将微博插入数据库,返回该微博数据库的行rid以及微博id'''
        id = jsData['idstr']
        count = self.cursor.execute("select rid,id,parentId from weibo where id = %s",id)
        if count!=0:#原微博存在
            result = self.cursor.fetchone()
            ##print result
            if result[2]!=parentId:#如果已存在的微博parentId与实际不相同（可能是从用户抓取的），则更新parentId
                ##print result
                self.cursor.execute("update weibo set parentId = %s where id = %s",(parentId,id))
                self.conn.commit()
            return result[:2]
            
        
        if 'deleted' in jsData:#原微博已删除
            ##print jsData
            self.cursor.execute("insert into weibo (rootId,id,mid,text,deleted,created_at,date,parentId) values(%s,%s,%s,%s,%s,%s,%s,%s)",(rootId,jsData['idstr'],jsData['mid'],jsData['text'],jsData['deleted'],jsData['created_at'],self.date,parentId))
            self.conn.commit()
            return [self.cursor.lastrowid,id]
        
        user = jsData['user']
        rid,uid = self.insetUserId2DB(user)#若该原微博的用户不存在，插入数据库

        
        if 'original_pic' not in jsData:
            jsData['original_pic']=None
        self.cursor.execute("insert into weibo values(0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(rid,rootId,jsData['idstr'],jsData['mid'],jsData['text'],jsData['source'],jsData['favorited'],jsData['truncated'],jsData['original_pic'],jsData['mlevel'],jsData['reposts_count'],jsData['comments_count'],jsData['created_at'],None,self.date,parentId))
        self.conn.commit()
        return [self.cursor.lastrowid,id]
    def getSql4insertUserInfo2DB(self,arg):
        columns = []
        values = []
        for e in arg:
            if e == "status":
                continue
            columns.append(e)
            
            if type(arg[e]) != types.UnicodeType and type(arg[e]) != types.StringType:
                ##print e,":",arg[e],":",type(arg[e])
                arg[e] = str(arg[e])
            else:
                arg[e] = MySQLdb.escape_string(arg[e].encode('utf-8')).decode('utf-8')
            values.append(arg[e])
        #加入日期
        columns.append('date')
        values.append(self.date)
        #确保columns和vaules内的值都是unicode
        #这里把description的字段设为了mediumblob，用于存放4字节的utf8，取出时需要SELECT CAST(Content AS CHAR CHARACTER SET utf8) AS Content
        sql = "insert into userinfo ("+",".join(columns)+") values('"+"','".join(values)+"')"
        return sql

    def insetUserId2DB(self,data):
        id = data['idstr']
        count = self.cursor.execute("select rid,id from userinfo where id = %s",id)
        if count == 0:#如果不存在，则插入
            sql = self.getSql4insertUserInfo2DB(data)
            ##print sql
            self.cursor.execute(sql)
            self.conn.commit()
            
            
#            #print sql
#            try:
#                self.cursor.execute(sql)
#                self.conn.commit()
#            except MySQLdb.Error, e:
#                #print "Error %d: %s" % (e.args[0], e.args[1]) 
#                
                
            return [self.cursor.lastrowid,id]
        else:
            result = self.cursor.fetchone()
            return result
    def isValidAccount(self):
        validAccounts = []
        for account in self.userAccounts:
            try:
                data = urllib2.urlopen('https://api.weibo.com/2/account/rate_limit_status.json?access_token=%s' % account[3]).read()
                js = json.loads(data)
                print js
                if 'error' in js:
                    print 'account is invalid:  ',account
                    index = self.userAccounts.index(account)
                    del self.userAccounts[index]
                else:
                    debug('account valid:',account)
                    validAccounts.append(account)
            except:
                debug('account is invalid:  ',account)
                
        self.userAccounts = validAccounts
    def initAccounts(self):
        self.userAccounts = []
        f = open(r'userAccount.txt', 'r')
        for line in f:
            line = line.strip()
            if line[0]=='#':
                continue
            result = line.split()
            self.userAccounts.append(result)
        self.isValidAccount()
        print self.userAccounts
    def switchUserAccount(self):
        self.accountIndex = self.accountIndex % len(self.userAccounts)
        username = self.userAccounts[self.accountIndex][0]
        password = self.userAccounts[self.accountIndex][1]
        self.appKey = self.userAccounts[self.accountIndex][2]
        self.accessToken = self.userAccounts[self.accountIndex][3]
        self.accountIndex += 1
        return username,password,self.accessToken
    
    def createDatabaseTable(self):
        self.conn=MySQLdb.connect(host="127.0.0.1",user="root",
                          passwd="g",db="nWeibo",charset='utf8')
    
        self.cursor = self.conn.cursor()
#        self.cursor.execute('''CREATE TABLE if not exists userinfo_temp  (
#        `id` bigint unsigned  not null  auto_increment PRIMARY KEY,
#        `userId` text,
#        `date` text
#        )
#        ''')
        self.cursor.execute('''create table if not exists userinfo (
            `rid` bigint unsigned  not null  auto_increment PRIMARY KEY,
            id text,
            idstr text,
            screen_name text,
            name text,
            province text,
            city text,
            location text,
            description MEDIUMBLOB,
            url text,
            profile_image_url text,
            profile_url text,
            weihao text,
            domain text,
            gender text,
            followers_count text,
            friends_count text,
            statuses_count text,
            favourites_count text,
            created_at text,
            following text,
            allow_all_act_msg text,
            geo_enabled text,
            verified text,
            verified_type text,
            remark text,
            status_id text,
            allow_all_comment text,
            avatar_large text,
            verified_reason text,
            follow_me text,
            online_status text,
            bi_followers_count text,
            lang text,
            cover_image text,
            date text
            
        )ENGINE=MyISAM ''')
        self.cursor.execute('''CREATE TABLE if not exists followers  (
        `id` bigint unsigned  not null  auto_increment PRIMARY KEY,
        `uId` bigint,
        `targetId` bigint
        )ENGINE=MyISAM
        ''')
        self.cursor.execute('''CREATE TABLE if not exists friends  (
        `id` bigint unsigned  not null  auto_increment PRIMARY KEY,
        `uId` bigint,
        `targetId` bigint
        )ENGINE=MyISAM
        ''')
        self.cursor.execute('''CREATE TABLE if not exists weibo  (
        `rid` bigint unsigned  not null  auto_increment PRIMARY KEY,
        `uId` bigint,
        `rootId` bigint default 0,
        `id` bigint unsigned,
        `mid` text,
        `text` MEDIUMBLOB,
        `source` text,
        `favorited` text,
        `truncated` text,
        `original_pic` text,
        `mlevel` text,
        `reposts_count` int,
        `comments_count` int,
        `created_at` text,
        `deleted` text,
        `date` text,
        `parentId` bigint default 0
        )ENGINE=MyISAM
        ''')

        
        
        self.conn.commit()
    def test(self):
        self.cursor.execute("SELECT text FROM `weibo` where id = 3492828741371910")
        text = self.cursor.fetchone()[0]
        #print text
        #print type(text)
        #print self.weiboParse(text)
        
if __name__=="__main__":
    crawl = CrawlWeibo()
    file = open('hotweibos.txt')
    for line in file:
        line = line.strip()
        if line[0]=='#':continue
        crawl.crawlWeibo(line)
    print 'createWeiboTree'    
    crawl.createWeiboTree()
    #crawl.test()

        
