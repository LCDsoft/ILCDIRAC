'''
Several file utilities

@site: Mar 13, 2013
@author: sposs
'''
import os, shutil
from DIRAC import S_OK, S_ERROR

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient    import RequestClient

def upload(path, appTar):
  """ Upload to storage
  """
  rm = ReplicaManager()

  if not os.path.exists(appTar):
    print "Tar ball %s does not exists, cannot continue." % appTar
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data") > -1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar,"%s%s" % (final_path, appTar))
    except Exception, x:
      print "Could not copy because %s" % x
      return S_ERROR("Could not copy because %s" % x)
  elif path.find("http://") > -1:
    print "path %s was not forseen, location not known, upload to location yourself, and publish in CS manually" % path
    return S_ERROR()
  else:
    lfnpath = "%s%s" % (path, appTar)
    res = rm.putAndRegister(lfnpath, appTar, "CERN-SRM")
    if not res['OK']:
      return res
    request = RequestContainer()
    request.setCreationTime()
    requestClient = RequestClient()
    request.setRequestName('default_request.xml')
    request.setSourceComponent('ReplicateILCSoft')
    res = request.addSubRequest({'Attributes':{'Operation' : 'replicateAndRegister',
                                               'TargetSE' : 'IN2P3-SRM'},
                                 'Files':[{'LFN':lfnpath}]},
                                 'transfer')
    #res = rm.replicateAndRegister("%s%s"%(path,appTar),"IN2P3-SRM")
    if not res['OK']:
      return res
    requestName = os.path.basename(appTar).replace('.tgz', '')
    request.setRequestAttributes({'RequestName' : requestName})
    requestxml = request.toXML()['Value']
    res = requestClient.setRequest(requestName, requestxml)
    if not res['OK']:
      print 'Could not set replication request %s' % res['Message']
    return S_OK('Application uploaded')
  return S_OK()

