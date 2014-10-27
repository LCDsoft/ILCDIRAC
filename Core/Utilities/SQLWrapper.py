'''
Wrapper script to run mokka with a local database setup 

Based on Jan Engels bash script

Called from ILCDIRAC.Workflow.Modules.MokkaAnalysis

@author: Przemyslaw Majewski and Stephane Poss
@since: Feb 1, 2010
'''

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Subprocess import shellCall, Subprocess
from ILCDIRAC.Core.Utilities.PrepareLibs import removeLibc
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation  import LocalArea, SharedArea

import os, sys, tempfile, threading, time, shutil

EXECUTION_RESULT = {}

class SQLWrapper:
  """Wrapper around the mokka DB"""
  def __init__(self, softwareDir = './', mokkaDBroot = ''):
    """Set initial variables
    
    @param softwareDir: path to the location of the software installation
    @type softwareDir: string
    @param mokkaDBroot: path to the place where the DB will live
    @type mokkaDBroot: string
    
    """
    self.MokkaDumpFile = ""
      
    self.MokkaTMPDir = ''
    self.MokkaDataDir = ''
    self.applicationLog = '%s/mysqllog' % (os.getcwd())
         
    self.stdError = '%s/mysql_errlog' % (os.getcwd())
 
    self.softDir = softwareDir
    self.rootpass = "rootpass"
    self.mokkaDBroot = mokkaDBroot
    
    self.initialDir = os.getcwd()
    self.log = gLogger.getSubLogger( "SQL-wrapper" )
    
    self.mysqlInstalDir = ''  
    
    #mysqld threading
    self.bufferLimit = 10485760   
    self.maxPeekLines = 20

    self.exeEnv=''
    self.mysqldPID = -1
    
  def setDBpath(self, dbpath, dumpfile = ''):
    """ Look for the DB to use.
    """
    if len(dumpfile) < 1:
      dumpfile = 'CLICMokkaDB.sql'
      path = "%s/%s" % (dbpath, dumpfile)
      self.MokkaDumpFile = path
    else:
      self.MokkaDumpFile = "%s/%s" % (os.getcwd(), os.path.basename(dumpfile))
      
    if not os.path.exists(self.MokkaDumpFile):
      return S_ERROR("Default DB was not found")
    if not os.environ.has_key('MOKKA_DUMP_FILE'):
      os.environ['MOKKA_DUMP_FILE'] = self.MokkaDumpFile
    return S_OK()  
      
  def makedirs(self):
    """Method to create all necessary directories for MySQL
    """
    #os.chdir(self.softDir)

    #"""create tmp dir and track it"""
    if not os.path.exists(self.mokkaDBroot):
      try :
        os.makedirs(self.mokkaDBroot)
      except OSError as x:
        self.log.error("Could not create mokkaDBroot, exception %s." % (x))
        return S_ERROR("Could not create mokkaDBroot, exception %s." % (x))
    try:
      self.MokkaTMPDir = tempfile.mkdtemp('', 'TMP', self.mokkaDBroot)
    except OSError as x:
      self.log.error("Exception error: %s" % (x))
      return S_ERROR("Exception error: %s" % (x))
    self.MokkaDataDir = os.path.join(self.initialDir, "data")
    try:
      os.mkdir(self.MokkaDataDir)
    except OSError as x:
      self.log.error("Could not create data dir, exception %s" % (x))
      return S_ERROR("Could not create data dir, exception %s" % (x))  
    return S_OK()

    #os.chdir(self.initialDir)
  def getMokkaTMPDIR(self):
    """ Get the location of the TMPDIR, where the socket is.
    """
    #return os.path.join(self.initialDir,self.MokkaTMPDir)
    return os.path.join(self.MokkaTMPDir)
      
  def mysqlSetup(self):
    """Setup mysql locally in local tmp dir 
    """
    #initialDir= os.getcwd()
    if not os.path.exists(self.MokkaTMPDir):
      return S_ERROR("MokkaTMP dir is not available")
    if not os.path.exists(self.MokkaDataDir):
      return S_ERROR("Mokka Data dir is not available")  
    
    ##Because it's possibly installed in the shared area, where one regular user cannot write, it's needed to get it back to the LocalArea
    if self.softDir == SharedArea():
      localarea = LocalArea()
      if not os.path.isdir(os.path.join(localarea, "mysql4grid")):
        try:
          shutil.copytree(os.path.join(self.softDir, "mysql4grid"), os.path.join(localarea, "mysql4grid"), False)
        except Exception, x :
          return S_ERROR("Could not copy back to LocalArea the mysql install dir: %s" % (str(x)))
      self.softDir = localarea

    ### So here lies the dragon: Beware of what you do!
    ###
    #                           / \  //\
    #            |\___/|      /   \//  \\
    #            /0  0  \__  /    //  | \ \    
    #           /     /  \/_/    //   |  \  \  
    #           @_^_@'/   \/_   //    |   \   \ 
    #           //_^_/     \/_ //     |    \    \
    #        ( //) |        \///      |     \     \
    #      ( / /) _|_ /   )  //       |      \     _\
    #    ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
    #  (( / / )) ,-{        _      `-.|.-~-.           .~         `.
    # (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
    # (( /// ))      `.   {            }                   /      \  \
    #  (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
    #             ///.----..>        \             _ -~             `.  ^-`  ^-_
    #               ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
    #                                                                  /.-~
    ######
    #### Hell is the maintenance of the crap below!  
    os.chdir(self.softDir)
    self.log.verbose('setup local mokka database')
    removeLibc(self.softDir + "/mysql4grid/lib64/mysql")
    if os.environ.has_key('LD_LIBRARY_PATH'):
      os.environ['LD_LIBRARY_PATH'] = '%s/mysql4grid/lib64/mysql:%s/mysql4grid/lib64:%s' % (self.softDir,
                                                                                            self.softDir,
                                                                                            os.environ['LD_LIBRARY_PATH'])
    else:
      os.environ['LD_LIBRARY_PATH'] = '%s/mysql4grid/lib64/mysql:%s/mysql4grid/lib64' % (self.softDir, self.softDir)
    os.environ['PATH'] = '%s/mysql4grid/bin:%s' % (self.softDir, os.environ['PATH'])
    self.exeEnv = dict( os.environ )
    
    safe_options = "--no-defaults --skip-networking --socket=%s/mysql.sock --datadir=%s --basedir=%s/mysql4grid --pid-file=%s/mysql.pid --log-error=%s --log=%s" % (self.MokkaTMPDir, 
                                                                                                                                                                    self.MokkaDataDir,
                                                                                                                                                                    self.softDir,
                                                                                                                                                                    self.MokkaTMPDir,
                                                                                                                                                                    self.stdError,
                                                                                                                                                                    self.applicationLog)
    comm = "mysql_install_db %s" % (safe_options) 
    self.log.verbose("Running %s" % comm)
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
        
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
      self.log.error('SQLwrapper Exited With Status %s' % (status))
 
    ###Now run mysqld in thread
    os.chdir("%s/mysql4grid" % (self.softDir))
    
    
    self.log.verbose("Running mysqld_safe %s" % safe_options)

    spObject = Subprocess( timeout = False, bufferLimit = int( self.bufferLimit ) )
    command = '%s/mysql4grid/bin/mysqld_safe %s' % (self.softDir, safe_options)
    self.log.verbose( 'Execution command: %s' % ( command ) )
        
    exeThread = ExecutionThread( spObject, command, self.maxPeekLines, self.applicationLog, self.stdError, self.exeEnv )
    exeThread.start()
    time.sleep( 5 )
    self.mysqldPID = spObject.getChildPID()
       
    #if not self.mysqldPID:
        #return S_ERROR( 'MySQLd process could not start after 5 seconds' )
    
    self.log.verbose("MySQLd run with pid: %s" % self.mysqldPID)


    ####Have to sleep for a while to let time for the socket to wake up
    sleepComm = """
while [ -z "$socket_grep" ] ; do
    socket_grep=$(netstat -ln 2>/dev/null | grep "%s/mysql.sock")
    echo -n .
    sleep 1
done 
""" % (self.MokkaTMPDir)
    self.result = shellCall(0, sleepComm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    ###changing root pass
    mysqladmincomm = "mysqladmin --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot password '%s'" % (self.MokkaTMPDir,
                                                                                                           self.rootpass)
    self.log.verbose("Running %s" % mysqladmincomm)
    self.result = shellCall(0, mysqladmincomm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    self.log.info( "Status after the mysqladmin execution is %s" % str( status ) )
    
    ###taken from https://svnsrv.desy.de/viewvc/ilctools/gridtools/trunk/MokkaGridScripts/runjob.sh?revision=268&view=markup
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'GRANT ALL PRIVILEGES ON *.* TO root;' " % (self.MokkaTMPDir, self.rootpass)
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'GRANT ALL PRIVILEGES ON *.* TO consult IDENTIFIED BY \"consult\";' " % (self.MokkaTMPDir, self.rootpass)
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'DELETE FROM mysql.user WHERE User = \"\"; FLUSH PRIVILEGES;' " % (self.MokkaTMPDir, self.rootpass)
    self.result = shellCall(0, comm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    #comm = "mysql --no-defaults -uroot -hlocalhost --socket=%s/mysql.sock -p%s <<< 'DELETE FROM mysql.user WHERE Host != \"%\"; FLUSH PRIVILEGES;' "%(self.MokkaTMPDir,self.rootpass)
    #self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    
    ###calling mysql
    mysqlcomm = "mysql  --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot -p%s < %s" % (self.MokkaTMPDir, 
                                                                                              self.rootpass, 
                                                                                              self.MokkaDumpFile)
    self.log.verbose("running %s" % mysqlcomm)
    self.result = shellCall(0, mysqlcomm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = self.result['Value']
    status = resultTuple[0]
    self.log.info( "Status after the mysql execution is %s" % str( status ) )
    ### now test
    #comm = "mysql --no-defaults -uconsult -pconsult -hlocalhost --socket=%s/mysql.sock <<< 'SHOW VARIABLES;' "%(self.MokkaTMPDir)
    #self.result = shellCall(0,mysqlcomm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)


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
      self.log.error('MySQL setup Exited With Status %s' % (status))
      os.chdir(self.initialDir)
      return S_ERROR('MySQL setup Exited With Status %s' % (status))
    # Still have to set the application status e.g. user job case.
    #self.setApplicationStatus('mysql client %s Successful' %(self.applicationVersion))
    os.chdir(self.initialDir)
    #return S_OK('Mokka-wrapper %s Successful' %(self.applicationVersion))
    return S_OK('OK')
    
    #############################################################################
  def mysqlCleanUp(self):
    """Does mysql cleanup. Remove socket and tmpdir with mysql db.
    
    Called at the end of Mokka execution, whatever the status is.
    """
    currentdir = os.getcwd()
    os.chdir(os.path.join(self.softDir, "mysql4grid"))
    self.log.verbose('clean up db')
    mySQLcleanUpComm = "mysqladmin --no-defaults -hlocalhost --socket=%s/mysql.sock -uroot -p%s shutdown" % (self.MokkaTMPDir, self.rootpass)
            
    result = shellCall(0, mySQLcleanUpComm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = result['Value']
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
""" % (self.MokkaTMPDir)
    
    result = shellCall(0, sleepComm, callbackFunction = self.redirectLogOutput, bufferLimit = 20971520)
    resultTuple = result['Value']

    os.chdir(currentdir)

    status = resultTuple[0]
    #self.log.info( "Status after the application execution is %s" % str( status ) )    
    failed = False
    if status != 0:
      self.log.error( "MySQL-cleanup execution completed with errors:" )
      failed = True
    else:
      self.log.info( "MySQL-cleanup execution completed successfully")

    #cleanup script also removes tmp
    if os.path.exists(self.MokkaTMPDir):
      try:
        self.log.verbose('Removing tmp dir')
        shutil.rmtree(self.mokkaDBroot, True)
        #shutil.rmtree(self.MokkaTMPDir,True)
        #shutil.rmtree(self.MokkaDataDir,True)
      except OSError as err:
        errno, strerror = err
        self.log.error("I/O error(%s): %s" % (errno, strerror))
        #return S_ERROR('Removing tmp dir failed')

    if failed:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      self.log.error('MySQL-cleanup Exited With Status %s' % (status))
      return S_ERROR('MySQL-cleanup Exited With Status %s' % (status))

    return S_OK('OK')
    #############################################################################
        
    #############################################################################
  def redirectLogOutput(self, fd, message):
    """Redirecting logging output to file specified.
    
    Should not be accessed from somewhere else.
    """
    sys.stdout.flush()
    if self.applicationLog:
      log = open(self.applicationLog, 'a')
      log.write(message + '\n')
      log.close()
    else:
      self.log.error("Application Log file not defined")
    if fd == 1:
      self.stdError += message
    #############################################################################

class ExecutionThread( threading.Thread ):
  """ Threading class used to run the mysqld server
  """
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
    start = time.time()
    initialStat = os.times()
    output = self.spObject.systemCall( self.cmd, env = self.exeEnv, callbackFunction = self.sendOutput, shell = True )
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
    """sends the Output"""
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
