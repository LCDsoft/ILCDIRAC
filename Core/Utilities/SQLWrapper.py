# $HeadURL$
# $Id$
'''
Wrapper script to run mokka with a local database setup 
Based on Jan Engels bash script

Created on Feb 1, 2010

@author: pmajewsk
'''

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, List
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall, Subprocess
import DIRAC
import os,sys,re, tempfile, threading, time

EXECUTION_RESULT = {}

class SQLWrapper:
  def __init__(self,dumpfile='',softwareDir='./',mokkaDBroot=''):
    """Set initial variables"""
    self.MokkaDumpFile = ""
    if(len(dumpfile)<1):
      dumpfile= 'CLICMokkaDB.sql'
      self.MokkaDumpFile = "%s/%s"%(softwareDir,dumpfile)
    else:
      self.MokkaDumpFile = "./%s"%(os.path.basename(dumpfile))
    if not os.environ.has_key('MOKKA_DUMP_FILE'):
      os.environ['MOKKA_DUMP_FILE']=self.MokkaDumpFile
      
    self.MokkaTMPDir = ''
    self.applicationLog = '%s/mysql.log'%(os.getcwd())
         
    self.stdError = '%s/mysql_err.log'%(os.getcwd())
 
    self.softDir = softwareDir
    self.rootpass = "rootpass"
    self.mokkaDBroot = mokkaDBroot
    
    self.initialDir= os.getcwd()
    #os.chdir(self.softDir)

    """create tmp dir and track it"""
    try :
      os.makedirs(self.mokkaDBroot)
    except :
      pass
    try:
        self.MokkaTMPDir = tempfile.mkdtemp('','TMP',mokkaDBroot)
    except IOError, (errno,strerror):
        DIRAC.gLogger.exception("I/O error({0}): {1}".format(errno, strerror))   
        
       
    self.log = gLogger.getSubLogger( "SQL-wrapper" )
        
    self.mysqlInstalDir = ''  
    
    #mysqld threading
    self.bufferLimit = 10485760   
    self.maxPeekLines = 20      

    #os.chdir(self.initialDir)
  def getMokkaTMPDIR(self):
    #return os.path.join(self.initialDir,self.MokkaTMPDir)
    return os.path.join(self.MokkaTMPDir)
      
  def mysqlSetup(self):
    """Setup mysql locally in local tmp dir """
    #initialDir= os.getcwd()
    os.chdir(self.softDir)
    DIRAC.gLogger.verbose('setup local mokka database')
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH']='%s/mysql4grid/lib64/mysql:%s/mysql4grid/lib64:%s'%(self.softDir,self.softDir,os.environ['LD_LIBRARY_PATH'])
    else:
      os.environ['LD_LIBRARY_PATH']='%s/mysql4grid/lib64/mysql:%s/mysql4grid/lib64'%(self.softDir,self.softDir)
    os.environ['PATH']='%s/mysql4grid/bin:%s'%(self.softDir,os.environ['PATH'])
    self.exeEnv = dict( os.environ )
    
    safe_options =  "--no-defaults --skip-networking --socket=%s/mysql.sock --datadir=%s/data --basedir=%s/mysql4grid --pid-file=%s/mysql.pid --log-error=%s --log=%s"%(self.MokkaTMPDir,self.MokkaTMPDir,self.softDir,self.MokkaTMPDir,self.stdError,self.applicationLog)
    comm = "mysql_install_db %s"%(safe_options) 
    self.log.verbose("Running %s"%comm)
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
        
    resultTuple = self.result['Value']

    status = resultTuple[0]
    self.log.info( "Status after the mysql_install_db execution is %s" % str( status ) )
    failed = False
    if status != 0:
      self.log.error( "mysql_install_db execution completed with errors:" )
      failed = True
    else:
      self.log.info( "mysql_install_db execution completed successfully")

    if failed:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.log.error('SQLwrapper Exited With Status %s' %(status))
 
    ###Now run mysqld in thread
    os.chdir("%s/mysql4grid"%(self.softDir))
    
    
    self.log.verbose("Running mysqld_safe %s"%safe_options)

    spObject = Subprocess( timeout = False, bufferLimit = int( self.bufferLimit ) )
    command = '%s/mysql4grid/bin/mysqld_safe %s'%(self.softDir,safe_options)
    self.log.verbose( 'Execution command: %s' % ( command ) )
        
    exeThread = ExecutionThread( spObject, command, self.maxPeekLines, self.applicationLog, self.stdError, self.exeEnv )
    exeThread.start()
    time.sleep( 5 )
    self.mysqldPID = spObject.getChildPID()
       
    #if not self.mysqldPID:
        #return S_ERROR( 'MySQLd process could not start after 5 seconds' )
    
    self.log.verbose("MySQLd run with pid: %s"%self.mysqldPID)


    ####Have to sleep for a while to let time for the socket to wake up
    sleepComm = """
while [ -z "$socket_grep" ] ; do
    socket_grep=$(netstat -ln 2>/dev/null | grep "%s/mysql.sock")
    echo -n .
    sleep 1
done 
"""%(self.MokkaTMPDir)
    self.result = shellCall(0,sleepComm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    ###changing root pass
    mysqladmincomm = "mysqladmin --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot password '%s'"%(self.MokkaTMPDir,self.rootpass)
    self.log.verbose("Running %s"%mysqladmincomm)
    self.result = shellCall(0,mysqladmincomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    self.log.info( "Status after the mysqladmin execution is %s" % str( status ) )
    
    ###taken from https://svnsrv.desy.de/viewvc/ilctools/gridtools/trunk/MokkaGridScripts/runjob.sh?revision=268&view=markup
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'GRANT ALL PRIVILEGES ON *.* TO root;' "%(self.MokkaTMPDir,self.rootpass)
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'GRANT ALL PRIVILEGES ON *.* TO consult IDENTIFIED BY \"consult\";' "%(self.MokkaTMPDir,self.rootpass)
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'DELETE FROM mysql.user WHERE User = \"\"; FLUSH PRIVILEGES;' "%(self.MokkaTMPDir,self.rootpass)
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'DELETE FROM mysql.user WHERE Host != \"%\"; FLUSH PRIVILEGES;' "%(self.MokkaTMPDir,self.rootpass)
    #self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    
    ###calling mysql
    mysqlcomm = "mysql  --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot -p%s < %s"%(self.MokkaTMPDir,self.rootpass,self.MokkaDumpFile)
    self.log.verbose("running %s"%mysqlcomm)
    self.result = shellCall(0,mysqlcomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    self.log.info( "Status after the mysql execution is %s" % str( status ) )
    ### now test
    comm = "mysql --no-defaults -uconsult -pconsult -hlocalhost --socket=%s/mysql.sock <<< 'SHOW VARIABLES;' "%(self.MokkaTMPDir)
    self.result = shellCall(0,mysqlcomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)

    ##get the intial DB
#    init_db_file_name = "%s/setup.sql"%self.MokkaTMPDir
#    init_db_file = file(init_db_file_name,"w")
#    init_db_file.write("-- DEFAULT DATABASE SETUP SCRIPT -------------------------------------\n")
#    init_db_file.write("GRANT ALL PRIVILEGES ON *.* TO '$USER'@'localhost' WITH GRANT OPTION;\n")
#    init_db_file.write("GRANT SELECT ON *.* TO '$read_user'@'localhost';\n")
#    init_db_file.write("GRANT ALL PRIVILEGES ON *.* TO '$write_user'@'localhost';\n")
#    init_db_file.write("FLUSH PRIVILEGES;\n")
#    init_db_file.write("-- -------------------------------------------------------------------")
#    init_db_file.close
#
#    lastmysqlcomm = "mysql --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot -p%s < %s"%(self.MokkaTMPDir,self.rootpass,init_db_file_name)
#    print "running %s"%lastmysqlcomm
#    self.result = shellCall(0,lastmysqlcomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
#    resultTuple = self.result['Value']
#    status = resultTuple[0]
#    #self.log.info( "Status after the mysql-local-db-setup execution is %s" % str( status ) )
#    self.log.info( "Status after the last mysql execution is %s" % str( status ) )
#   
    
#    if str(os.environ.get('UID_TMP')) == 'None':
#      DIRAC.gLogger.error('No UID_TMP known')
#      self.UID_TMP = ''
#    else:
#      self.UID_TMP = str(os.environ.get('UID_TMP'))
#    
#        
#    if not self.MokkaTMPDir[-1] == '/':
#      self.MokkaTMPDir += '/'
#                
#    self.mysqlInstalDir = self.MokkaTMPDir + self.UID_TMP 
#        
#    #init db
#    DIRAC.gLogger.verbose('add all privileges for user consult')
#    #mysql command:
#    #MySQLcomm = 'mysql'
#    MySQLcomm = 'mysql --socket ' + self.mysqlInstalDir + '/' + '/mysql.sock' + ' -e "GRANT ALL PRIVILEGES ON *.* TO \'consult\'@\'localhost\' IDENTIFIED BY \'consult\';"'
#    
#    comm = MySQLcomm
#    print "MySQLcomm %s"%comm
#    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
#        
#    resultTuple = self.result['Value']
#
#    status = resultTuple[0]
#    self.log.info( "Status after the application execution is %s" % str( status ) )
#    failed = False
#    if status != 0:
#      self.log.error( "mysql execution completed with errors:" )
#      failed = True
#    else:
#      self.log.info( "mysql execution completed successfully")
#
#    if failed:
#      self.log.error( "==================================\n StdError:\n" )
#      self.log.error( self.stdError )
#      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
#      self.log.error('mysql Exited With Status %s' %(status))
#      return S_ERROR('mysql Exited With Status %s' %(status))
#    # Still have to set the application status e.g. user job case.
#    #self.setApplicationStatus('mokka-wrapper %s Successful' %(self.applicationVersion))
#        
#    #test query
#    DIRAC.gLogger.verbose('test query to mysql')
#    MySQLcomm = 'mysql'
#    MySQLparams = ' --socket ' + self.mysqlInstalDir + '/' + '/mysql.sock' + ' -uconsult -pconsult -e "show databases;"'
#    comm = MySQLcomm + MySQLparams
#    print "MySQLcomm %s"%comm
#         
#    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
#        
#    resultTuple = self.result['Value']
#
#    status = resultTuple[0]
    self.log.info( "Status after the application execution is %s" % str( status ) )
    failed = False
    if status != 0:
      self.log.error( "mysql client execution completed with errors:" )
      failed = True
    else:
      self.log.info( "mysql client execution completed successfully")
   
    if failed:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.log.error('MySQL setup Exited With Status %s' %(status))
      os.chdir(self.initialDir)
      return S_ERROR('MySQL setup Exited With Status %s' %(status))
    # Still have to set the application status e.g. user job case.
    #self.setApplicationStatus('mysql client %s Successful' %(self.applicationVersion))
    os.chdir(self.initialDir)
    #return S_OK('Mokka-wrapper %s Successful' %(self.applicationVersion))
    return S_OK('OK')
    
    #############################################################################
  def mysqlCleanUp(self):
    """Does mysql cleanup command. Remove socket and tmpdir with mysql db."""
    currentdir = os.getcwd()
    os.chdir(os.path.join(self.softDir,"mysql4grid"))
    DIRAC.gLogger.verbose('clean up db')
    MySQLcleanUpComm = "mysqladmin --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot -p%s shutdown"%(self.MokkaTMPDir,self.rootpass)
            
    self.result = shellCall(0,MySQLcleanUpComm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    
    resultTuple = self.result['Value']

    status = resultTuple[0]
    self.log.info( "Status after the shutdown execution is %s" % str( status ) )
    ##kill mysql
    #mysqlkillcomm = "cat mysql.pid | kill -9 "#%(self.MokkaTMPDir)
    #mysqlkillcomm = "kill -9 %s"%(self.mysqldPID)
    #self.result = shellCall(0,mysqlkillcomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    
    #resultTuple = self.result['Value']

    ####Have to sleep for a while to let time for the socket to go away
    sleepComm = """
while [ -n "$socket_grep" ] ; do
    socket_grep=$(netstat -ln 2>/dev/null | grep "%s/mysql.sock")
    echo -n .
    sleep 1
done 
"""%(self.MokkaTMPDir)
    self.result = shellCall(0,sleepComm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    status = resultTuple[0]
    #self.log.info( "Status after the application execution is %s" % str( status ) )    
    failed = False
    if status != 0:
      self.log.error( "MySQL-cleanup execution completed with errors:" )
      failed = True
    else:
      self.log.info( "MySQL-cleanup execution completed successfully")

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.log.error('MySQL-cleanup Exited With Status %s' %(status))
      return S_ERROR('MySQL-cleanup Exited With Status %s' %(status))

    #cleanup script also removes tmp
    if (os.path.exists(self.MokkaTMPDir)):
      try:
        DIRAC.gLogger.verbose('Removing tmp dir')
        os.removedirs(self.mokkaDBroot)
      except IOError, (errno,strerror):
        DIRAC.gLogger.error("I/O error(%s): %s"%(errno, strerror))
        #return S_ERROR('Removing tmp dir failed')
      os.chdir(currentdir)
      return S_OK('OK')
    #############################################################################
        
    #############################################################################
  def redirectLogOutput(self, fd, message):
    """Redirecting logging output to file specified."""
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
    if self.applicationLog:
      log = open(self.applicationLog,'a')
      log.write(message+'\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
    #############################################################################

class ExecutionThread( threading.Thread ):

  #############################################################################
  def __init__( self, spObject, cmd, maxPeekLines, stdoutFile, stderrFile, exeEnv ):
    threading.Thread.__init__( self )
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines
    self.stdout = stdoutFile
    self.stderr = stderrFile
    self.exeEnv = exeEnv

  #############################################################################
  def run( self ):
    # FIXME: why local intances of object variables are created?
    cmd = self.cmd
    spObject = self.spObject
    start = time.time()
    initialStat = os.times()
    output = spObject.systemCall( cmd, env = self.exeEnv, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['Timing'] = timing
    finalStat = os.times()
    EXECUTION_RESULT['CPU'] = []
    for i in range( len( finalStat ) ):
      EXECUTION_RESULT['CPU'].append( finalStat[i] - initialStat[i] )

  #############################################################################
  def getCurrentPID( self ):
    return self.spObject.getChildPID()

  #############################################################################
  def sendOutput( self, stdid, line ):
    if stdid == 0 and self.stdout:
      outputFile = open( self.stdout, 'a+' )
      print >> outputFile, line
      outputFile.close()
    elif stdid == 1 and self.stderr:
      errorFile = open( self.stderr, 'a+' )
      print >> errorFile, line
      errorFile.close()
    self.outputLines.append( line )
    size = len( self.outputLines )
    if size > self.maxPeekLines:
      # reduce max size of output peeking
      self.outputLines.pop( 0 )

  #############################################################################
  def getOutput( self, lines = 0 ):
    if self.outputLines:
      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      # FIXME: this is multithread, thus single line would be better
      if lines:
        size = len( self.outputLines )
        cut = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR( 'No Job output found' )

    return result
