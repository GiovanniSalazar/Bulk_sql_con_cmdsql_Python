import os
import fnmatch
import logging
import pyodbc
import configparser
from subprocess import Popen, PIPE, STDOUT

#logging.basicConfig(filename='cmdsql.log', level=logging.INFO, format='[%(asctime)s] - [%(levelname)s] - %(message)s')
              
class DBHelper:

    def __init__(self,db,host,user,password):

        self.db = db
        self.host = host
        self.user = user
        self.password = password        

    def __connect__(self):
        self.con = cnxn = pyodbc.connect('driver={SQL Server};server=%s;uid=%s;pwd=%s'%(self.host, self.user, self.password),autocommit=True)       
        self.cur = self.con.cursor()

    def __disconnect__(self):
        self.con.close()

    def fetch(self, sql):
        self.__connect__()
        self.cur.execute(sql)
        result = self.cur.fetchall()
        self.__disconnect__()
        return result

    def execute(self,sql):
    
        try:
            self.__connect__()
            self.cur.execute(sql)
            self.__disconnect__()
            return True
        except Exception as e:
            #print ("Error: " + str(e))
            return str(e)
    
    def executefile(self,file):
        
        connection_string = 'sqlcmd -S %s -U %s -P %s -d %s -i %s -r0' %(self.host,self.user,self.password,self.db,file)
        try:

            session = Popen(connection_string, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            stdout, stderr = session.communicate()
            if(len(stderr)==0):
                return True
            else:
                return stderr   
            
        except Exception:
            return str(e)
            
                               
class Logger(object):
    #logging.basicConfig(filename='cmdsql.log', level=logging.INFO, format='[%(asctime)s] - [%(levelname)s] - %(message)s')
    def __init__(self, name='cmdsql',level=logging.DEBUG):
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        file_handler = logging.FileHandler('%s.log' % name, 'w')
        formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)        
        
    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)
        
def Main():

    log = Logger()
    
    cp = configparser.ConfigParser()
    cp.read("config.conf")
    
    folder      = eval(cp.get("common", "folder"),{},{})
    logfile     = eval(cp.get("common", "logfile"),{},{})
    database    = eval(cp.get("common", "db_name"),{},{})
    ip_addr     = eval(cp.get("common", "db_ip"),{},{})
    user        = eval(cp.get("common", "db_user"),{},{})
    password    = eval(cp.get("common", "db_pass"),{},{})
    
    
    objDB       = DBHelper(database,ip_addr,user,password)
    
    sqlcommand = ('create database %s' % (database))   
    resultExec=objDB.execute(sqlcommand)
    
    if(resultExec==True):
        for x in folder:
            files = []
            for r, d, f in os.walk(str(os.getcwd())+str(x)):
                for file in f:
                    if fnmatch.fnmatch(file, '*.sql'):
                        files.append(os.path.join(r, file))
            for f in files:
                log.info("Ejecutando "+str(f))
                resultcmd=objDB.executefile(f)
                if(resultcmd == True):
                    print("Se ejecutó "+str(f))
                    log.info("Se ejecutó "+str(f))
                else:
                    print(resultcmd)
                    log.error(resultcmd)
                    return
                    
    else:
        print(resultExec)
        log.error(resultExec)
                        

Main()