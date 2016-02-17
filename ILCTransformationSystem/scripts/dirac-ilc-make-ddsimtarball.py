#!/usr/bin/env python
"""
This program will create a tar ball suitable for running the program on the grid with ILCDIRAC
Needs the chrpath and readelf utilities
"""

import sys
import os
import tarfile

try:
  import hashlib as md5
except ImportError:
  import md5

from pprint import pprint


from DIRAC.Core.Base import Script
from DIRAC import gLogger, S_OK

Script.parseCommandLine( ignoreErrors = False )

from ILCDIRAC.ILCTransformationSystem.Utilities.ReleaseHelper import copyFolder, getLibraryPath, getFiles, getDependentLibraries, copyLibraries, getPythonStuff, killRPath, resolveLinks, removeSystemLibraries
from ILCDIRAC.Core.Utilities.CheckAndGetProdProxy import checkOrGetGroupProxy

class DDSimTarMaker( object ):
  """ create a tarball of the DDSim release """
  def __init__( self ):
    self.detmodels = {}
    self.lcgeo_env="lcgeo_DIR"
    self.ddhep_env="DD4HEP"
    self.softSec = "/Operations/Defaults/AvailableTarBalls"
    self.version = ''
    self.platform = 'x86_64-slc5-gcc43-opt'
    self.comment = ""
    self.name = "ddsim"
    self.csapi = None
    self.tarBallName = None
    self.md5sum = None

  def copyDetectorModels( self, basePath, folder, targetFolder ):
    """copy the compact folders to the targetFolder """
    for root,dirs,_files in os.walk( os.path.join(basePath, folder) ):
      for direct in dirs:
        if root.endswith("compact"):
          ## the main xml file must have the same name as the folder, ILD and CLIC follow this convention already
          xmlPath = os.path.join( root, direct, direct+".xml")
          if os.path.exists( xmlPath ):
            self.detmodels[direct] = "detectors/"+direct+"/"+direct+".xml"
            copyFolder( os.path.join(root, direct), targetFolder )

  def createTarBall( self, folder ):
    """create a tar ball from the folder
      tar zcf $TARBALLNAME $LIBFOLDER/*
    """
    ##Create the Tarball
    if os.path.exists(self.tarBallName):
      os.shutil.rm(self.tarBallName)
    gLogger.notice("Creating Tarball...")
    myappTar = tarfile.open(self.tarBallName, "w:gz")
    myappTar.add(folder)
    myappTar.close()

    self.md5sum = md5.md5(open( self.tarBallName, 'r' ).read()).hexdigest()

    gLogger.notice("...Done")
    return S_OK( "Created Tarball")

  def parseArgs( self ):
    """ parse the command line arguments"""
    # if len(sys.argv) != 3:
    #   raise RuntimeError( "Wrong number of arguments in call: '%s'" % " ".join(sys.argv) )
    self.name = sys.argv[1]
    self.version = sys.argv[2]
    self.tarBallName = "%s%s.tgz" % (self.name, self.version)


  def checkEnvironment( self ):
    """ check if dd4hep and lcgeo are in the environment """
    for var in [ self.ddhep_env, self.lcgeo_env , 'ROOTSYS' ]:
      if var not in os.environ:
        raise RuntimeError( "%s is not set" % var )
    return os.environ[self.ddhep_env], os.environ[self.lcgeo_env], os.environ['ROOTSYS']

  def createCSEntry( self ):
    """add the entries for this version into the Configuration System
    <version>
            {
              TarBall = ddsim<version>.tgz
              DetectorModels
              {
                CLIC_o2_v03 = detectors/CLIC_o2_v03/CLIC_o2_v03.xml
                ...
              }
              AdditionalEnvVar
              {
                ROOTSYS = /cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/root/5.34.30
                G4INSTALL = /cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/geant4/10.01
                G4DATA = /cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/geant4/10.01/share/Geant4-10.1.0/data
              }
              Overwrite = True
            }
    """
    #FIXME: Get root and geant4 location from environment, make sure it is cvmfs
    csParameter = { "TarBall": self.tarBallName,
                    "DetectorModels": self.detmodels,
                    "AdditionalEnvVar": {
                      "ROOTSYS" : "/cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/root/5.34.30",
                      "G4INSTALL" : "/cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/geant4/10.01",
                      "G4DATA" : "/cvmfs/ilc.desy.de/sw/x86_64_gcc44_sl6/geant4/10.01/share/Geant4-10.1.0/data",
                    }
                  }


    pars = dict( platform=self.platform,
                 name="ddsim",
                 version=self.version
               )


    csPath = os.path.join( self.softSec , "%(platform)s/%(name)s/%(version)s/" % pars )
    result = self.insertCSSection( csPath, csParameter )

    if self.csapi is not None:
      resProxy = checkOrGetGroupProxy( "diracAdmin" )
      if not resProxy['OK']:
        gLogger.error( "Failed to get AdminProxy", resProxy['Message'] )
        raise RuntimeError( "Failed to get diracAdminProxy" )
      self.csapi.commit()

    if not result['OK']:
      gLogger.error( "Failed to create CS Section", result['Message'] )
      raise RuntimeError( "Failed to create CS Section" )

  def insertCSSection( self, path, pardict ):
    """ insert a section and values (or subsections) into the CS

    :param str path: full path of the new section
    :param str pardict: dictionary of key values in the new section, values can also be dictionaries
    :return: S_OK(), S_ERROR()
    """
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
    self.csapi = CSAPI()

    for key, value in pardict.iteritems():
      newSectionPath = os.path.join(path,key)
      gLogger.debug( "Adding to cs %s : %s " % ( newSectionPath, value ) )
      self.csapi.createSection( path )
      if isinstance( value, dict ):
        res = self.insertCSSection( newSectionPath, value )
      else:
        res = self.csapi.setOption( newSectionPath, value )

      if not res['OK']:
        return res
      else:
        gLogger.notice( "Added to CS: %s " % res['Value'] )

    return S_OK("Added all things to cs")

  def createDDSimTarBall( self ):
    """ do everything to create the DDSim tarball"""
    self.parseArgs()
    ddBase, lcgeoBase, _rootsys = self.checkEnvironment()

    realTargetFolder = os.path.join( os.getcwd(), self.name+self.version )
    targetFolder = os.path.join( os.getcwd(), "temp", self.name+self.version )
    for folder in (targetFolder, targetFolder+"/lib"):
      try:
        os.makedirs( folder )
      except OSError:
        pass

    libraries = set()
    rootmaps = set()

    dd4hepLibPath = getLibraryPath( ddBase )
    lcgeoPath = getLibraryPath( lcgeoBase )


    self.copyDetectorModels( lcgeoBase, "CLIC" , targetFolder+"/detectors" )
    self.copyDetectorModels( lcgeoBase, "ILD"  , targetFolder+"/detectors" )


    libraries.update( getFiles( dd4hepLibPath, ".so") )
    libraries.update( getFiles( lcgeoPath, ".so" ) )

    rootmaps.update( getFiles( dd4hepLibPath, ".rootmap") )
    rootmaps.update( getFiles( lcgeoPath, ".rootmap" ) )

    pprint( libraries )
    pprint( rootmaps )


    allLibs = set()
    for lib in libraries:
      allLibs.update( getDependentLibraries(lib) )
    ### remote root and geant4 libraries, we pick them up from
    allLibs = set( [ lib for lib in allLibs if not ( "/geant4/" in lib.lower() or "/root/" in lib.lower()) ] )

    print allLibs

    copyLibraries( libraries, targetFolder+"/lib" )
    copyLibraries( allLibs, targetFolder+"/lib" )
    copyLibraries( rootmaps, targetFolder+"/lib" )

    getPythonStuff( ddBase+"/python"       , targetFolder+"/lib/")
    getPythonStuff( lcgeoBase+"/lib/python", targetFolder+"/lib/" )
    getPythonStuff( lcgeoBase+"/bin/ddsim", targetFolder+"/bin/" )


    ##Should get this from CVMFS
    #getRootStuff( rootsys, targetFolder+"/ROOT" )

    copyFolder( targetFolder+"/", realTargetFolder.rstrip("/") )

    killRPath( realTargetFolder )
    resolveLinks( realTargetFolder+"/lib" )
    removeSystemLibraries( realTargetFolder+"/lib" )
    #removeSystemLibraries( realTargetFolder+"/ROOT/lib" )
    print self.detmodels


    self.createTarBall( realTargetFolder )

    #self.createCSEntry()

if __name__=="__main__":
  print "Creating Tarball for DDSim"
  try:
    DDSIMMAKER = DDSimTarMaker()
    DDSIMMAKER.createDDSimTarBall()
  except RuntimeError as e:
    print "ERROR during tarball creation: %s " % e
    exit(1)
  exit(0)
