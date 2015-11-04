"""Restart the an agent in case it gets stuck

This agent was intially designed to supervise the RequestExecutingAgent and
retart it in case it gets stuck.  At the moment the RequestExecutingAgent is the
only agent to really get stuck on a normal basis.  Can be extend to restart any
given agent of so desired.

The agent checks the age of the log file and if it is deemed too old will kill
the agent so that it is restarted automatically.

+----------------------------------------+-----------------------------------------+---------------------------------------+
|  **Option**                            |    **Description**                      |  **Example**                          |
+----------------------------------------+-----------------------------------------+---------------------------------------+
|  MaxLogAge                             | maximum Age of the log file in minues   | MaxLogAge = 60                        |
|                                        |                                         |                                       |
+----------------------------------------+-----------------------------------------+---------------------------------------+
|  AgentNames                            | name of the agent to monitor            | AgentNames=RequestExecutingAgent      |
|                                        |                                         |                                       |
|                                        |                                         |                                       |
+----------------------------------------+-----------------------------------------+---------------------------------------+


"""
__RCSID__ = "$Id$"

# # imports
import datetime
import os
import signal

# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Subprocess import systemCall

AGENT_NAME = "RequestManagement/RestartReqExeAgent"

#Define units
MINUTES = 60
SECONDS = 1

########################################################################
class RestartReqExeAgent( AgentModule ):
  """
  .. class:: RestartReqExeAgent

  """

  def initialize( self ):
    """ initialization """

    self.maxLogAge = int(self.am_getOption( "MaxLogAge", 60 ))*MINUTES
    self.agentNames = self.am_getOption( "AgentNames", ['RequestExecutingAgent'] )
    self.enabled = self.am_getOption( "Enabled" )
    return S_OK()

  def execute( self ):
    """ execution in one cycle """
    ok = True
    for agentName in self.agentNames:
      res = self._checkAgent( agentName )
      if not res['OK']:
        self.log.error( "Failure when checking agent", "%s, %s" %( agentName, res['Message'] ) )
        ok = False

    if not ok:
      return S_ERROR( "Error during this cycle, check log" )
    return S_OK()

  def _checkAgent( self, agentName ):
    """Check if the given agent is still running
    we are assuming this is an agent in the RequestManagementSystem
    """
    diracLocation = os.environ.get( "DIRAC", "/opt/dirac/pro" )
    currentLogLocation = os.path.join( diracLocation, 'runit', 'RequestManagement', agentName, 'log', 'current' )
    self.log.verbose( "Current Log File location: %s " % currentLogLocation )

    ## get the age of the current log file
    lastAccessTime = 0
    try:
      lastAccessTime = os.path.getmtime( currentLogLocation )
      lastAccessTime = datetime.datetime.fromtimestamp( lastAccessTime )
    except OSError as e:
      self.log.error( "Failed to access current log file", str(e) )
      return S_ERROR( "Failed to access current log file" )

    now = datetime.datetime.now()
    age = now - lastAccessTime

    self.log.info( "Current log file for %s is %d minutes old" % ( agentName, ( age.seconds / MINUTES ) ) )

    if age.seconds > self.maxLogAge:
      self.log.info( "Current log file is too old!" )
      res = self.__getPIDs( agentName )
      if not res['OK']:
        return res
      pids = res['Value']

      self.log.info( "Found PIDs for %s: %s" % ( agentName, pids ) )
      ## kill the agent
      if self.enabled:
        for pid in pids:
          os.kill( pid, signal.SIGTERM )
          self.log.info( "Killed the %s Agent with PID %s" % (agentName, pid) )
      else:
        self.log.info( "Would have killed the %s Agent" % agentName )


    return S_OK()


  def __getPIDs( self, agentName ):
    """return PID for agentName"""

    ## Whitespaces around third argument are mandatory to only match the given agentName
    pidRes = systemCall( 10, [ 'pgrep', '-f', ' RequestManagement/%s ' % agentName ] )
    if not pidRes['OK']:
      return pidRes
    pid = pidRes['Value'][1].strip()
    pid = pid.split("\n")
    pids = []
    try:
      pids.append( int( pid[0] ) )
    except ValueError as e:
      self.log.error( "Could not create int from PID: ", "PID %s: %s" (pid, e) )
      return S_ERROR( "Could not create int from PID" )

    return S_OK( pids )
