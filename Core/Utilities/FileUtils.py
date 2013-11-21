'''
Several file utilities

@site: Mar 13, 2013
@author: sposs
'''
__RCSID__ = "$Id$"

import os, shutil, glob, re
from distutils import dir_util, errors

from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.RequestClient    import RequestClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations   import Operations 

def upload(path, appTar):
  """ Upload software tar ball to storage
  """
  rm = ReplicaManager()
  ops = Operations()
  if path[-1] != "/":
    path += "/"
  if not os.path.exists(appTar):
    gLogger.error("Tar ball %s does not exists, cannot continue." % appTar)
    return S_ERROR()
  if path.find("http://www.cern.ch/lcd-data") > -1:
    final_path = "/afs/cern.ch/eng/clic/data/software/"
    try:
      shutil.copy(appTar,"%s%s" % (final_path, os.path.basename(appTar)))
    except EnvironmentError, x:
      gLogger.error("Could not copy because %s" % x)
      return S_ERROR("Could not copy because %s" % x)
  elif path.find("http://") > -1:
    gLogger.error("Path %s was not foreseen!" % path)
    gLogger.error("Location not known, upload to location yourself, and publish in CS manually")
    return S_ERROR()
  else:
    lfnpath = "%s%s" % (path, os.path.basename(appTar))
    res = rm.putAndRegister(lfnpath, appTar, ops.getValue('Software/BaseStorageElement', "CERN-SRM"))
    if not res['OK']:
      return res
    request = RequestContainer()
    request.setCreationTime()
    requestClient = RequestClient()
    request.setRequestName('copy_%s' % os.path.basename(appTar).replace(".tgz", "").replace(".tar.gz", ""))
    request.setSourceComponent('ReplicateILCSoft')
    copies_at = ops.getValue('Software/CopiesAt', [])
    index_copy = 0
    for copies in copies_at:
      res = request.addSubRequest({'Attributes':{'Operation' : 'replicateAndRegister',
                                                 'TargetSE' : copies,
                                                 'ExecutionOrder' : index_copy},
                                   'Files':[{'LFN':lfnpath}]},
                                   'transfer')
      #res = rm.replicateAndRegister("%s%s"%(path,appTar),"IN2P3-SRM")
      if not res['OK']:
        return res
      index_copy += 1
    requestxml = request.toXML()['Value']
    if copies_at:
      res = requestClient.setRequest(request.getRequestName()['Value'], requestxml)
      if not res['OK']:
        gLogger.error('Could not set replication request %s' % res['Message'])
      return S_OK('Application uploaded')
  return S_OK()

def fullCopy(srcdir, dstdir, item):
  """ Copy the item from srcdir to dstdir, creates missing directories if needed
  """
  item = item.rstrip().lstrip().lstrip("./").rstrip("/")
  srcdir = srcdir.rstrip("/")
  dstdir = dstdir.rstrip("/")
  if not re.match(r"(.*)[a-zA-Z0-9]+(.*)", item):#we want to have explicit elements
    gLogger.error("You try to get all files, that cannot happen")
    return S_OK()
  src = os.path.join(srcdir, item)
  items = glob.glob(src)
  if not items:
    gLogger.error("No items found matching", src)
    return S_ERROR("No items found!")
  
  for item in items:
    item = item.replace(srcdir,"").lstrip("/")
    dst = os.path.join(dstdir, item)

    try:
      dir_util.create_tree(dstdir, [item])
    except errors.DistutilsFileError, why:
      return S_ERROR(str(why))
    
    if os.path.isfile(os.path.join(srcdir, item)):
      try:
        shutil.copy2(os.path.join(srcdir, item), dst)
      except EnvironmentError, why:
        return S_ERROR(str(why))
    else:
      try:
        shutil.copytree(os.path.join(srcdir, item), dst)
      except EnvironmentError, why:
        return S_ERROR(str(why))
  return S_OK()

