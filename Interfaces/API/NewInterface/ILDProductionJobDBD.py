"""
ILD DBD specific  job utility

:author: S. Poss, A. Sailer, C. Calancha
:since: Jul 01, 2012
"""

import string
import pprint
from decimal import Decimal


from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from ILCDIRAC.Interfaces.Utilities import JobHelpers
from ILCDIRAC.Core.Utilities.LFNPathUtilities import joinPathForMetaData

__RCSID__ = "$Id$"

#pylint: disable=W0311
#pylint: disable=R0902
#pylint: disable=R0904

class ILDProductionJobDBD( ProductionJob ):
    """ILD Production Jobs definition"""
    def __init__( self ):
        super( ILDProductionJobDBD, self ).__init__()
        self.machine = 'ilc'
        self.experiment = 'ILC_ILD'
        self.basepath = self.ops.getValue( '/Production/%s/BasePath' % self.experiment, '/ilc/prod/ilc/mc-dbd/ild/' )
        self.polarization = ""
        self.machineparams = ''
        self.detector = ''
        self.compatmeta = {}
        self.processID = 0
        self.evtclass = ''
        self.evttype = ''
        self.genprocname = ''
        self.matchToInput=''


    def setMatchToInput( self, matchToInput ):
        """ Help to find faster the input directory
        """
        self.matchToInput = matchToInput

    def setProcessIDInFinalPath( self ):
        """ ILD name convention dont include ProcessID in the path name
            ProcessID is needed in the metadata: we add in the last path
        """
        for finalpaths in self.finalpaths:
            if self.processID:
                self.finalMetaDict[finalpaths].update({"ProcessID":self.processID})

        return S_OK()
    
    def setEvtClass( self, evtclass ):
        """ Sometimes we used different evtclass in the sim/reco files than the original in the stdhep
        """
        self.evtclass = evtclass

    def setEvtType( self, evttype ):
        """ EvtType missing in input lfn: i have no privileges to set metadata on those files
        """
        self.evttype = evttype
        print '[debug tino] Set self.evttype to %s '%self.evttype

    def setGenProcName( self, genprocname ):
        """ ILD convention add gen process name in the basename of LFN's
        """
        self.genprocname = genprocname

    # def __swapGenProcNameEvtType( self ):
    #     """ ILD has swapped these two meta fields at different energies
    #         GenProcessName @ 350 <----> EvtType @ 1000
    #     """
    #     tmpval           = self.evttype
    #     self.evttype     = self.genprocname
    #     self.genprocname = tmpval

        
    def setILDConfig( self, version ):
        """ This is because in the ProductionJob, it's called Config
        """
        return self.setConfig( version )
        
    def setInputDataQuery( self, metadata ):
        """ Define the input data query needed, also get from the data the meta info requested to build the path
        """

        for key,val in metadata.iteritems():
            print "[0] meta[%s] %s"%(key,val)

        retMetaKey = self._checkMetaKeys( metadata.keys(), extendFileMeta = True)
        if not retMetaKey['OK']:
            return retMetaKey

        # do i need this?
        tmp_metadata = {}
        tmp_metadata.update(metadata)
        # for kk in ['SoftwareTag', 'ILDConfig']:
        for kk in ['SoftwareTag', 'ILDConfig','ProcessID']:
            tmp_metadata.pop(kk, None) # why i was dropping ProdID?

        retDirs = self._checkFindDirectories( tmp_metadata )
        if not retDirs['OK']:
            return retDirs
        dirs = retDirs['Value'].values()

        compatmeta = {}

        print 'dirs found: %d' %len(dirs)
        # for d in dirs:
        #     print '%s'%d
        dir_found = False
        if self.matchToInput:
            print 'Will try to match dir with %s' %self.matchToInput
        for mdir in dirs:
            if self.matchToInput:
                if self.matchToInput not in mdir:
                    continue
            if 'ProdID' in metadata:
                val = '/' + str(metadata['ProdID']).zfill(8)
                if val not in mdir:
                    continue

            dir_found = True
            print '[debug tino] Found mdir %s' %mdir
            res = self.fc.getDirectoryUserMetadata( mdir )
            if not res['OK']:
                return self._reportError( "Error looking up the catalog for directory metadata: %s" % res['Message'] )
            compatmeta = res['Value'] # this reset compatmeta for each iteration (do we want this?)
            compatmeta.update( metadata )
            print '[tino debug] Updated compatmeta to: %s' %compatmeta
                
           
        if not dir_found:
            if self.dryrun:
                print 'We could not find our target dir: please try w/o dryrun (maybe target dir still not registered)'
            else:
                print 'We could not find our target dir and this is not a dryrun: please check'
            
        print 'self.fc.findFilesByMetadata( %s, "/ilc/prod/ilc" ) '%metadata
        # get all the files available, if any
        res = self.fc.findFilesByMetadata( metadata, '/ilc/prod/ilc' )
        # res = self.fc.findFilesByMetadata( metadata, '/ilc/user/c/calanchac/stdhep' )
        if not res['OK']:
            return self._reportError( "Could not find the files with this metadata" )

        if len( res['Value'] ):
            my_lfn = res['Value'][0]
            # #Get the meta data of the first one as it should be enough is the registration was 
            # # done right

            res = self.fc.getFileUserMetadata( my_lfn )
            if not res['OK']:
                return self._reportError( 'Failed to get file metadata, cannot build filename: %s' % res['Message'] )
            compatmeta.update( res['Value'] )
            print '[tino debug] Updated compatmeta to: %s' %compatmeta

            for key,val in self.compatmeta.iteritems():
                print "my_lfn %s compatmeta[%s] %s"%(my_lfn, key, val)

        else:
            if not self.dryrun:
                print res
                self._reportError( "No files matching the metadata: Metadata is wrong or files are not under "
                                   "/ilc/prod/ilc directory" )
                

        if not len(compatmeta):
            print 'ERROR, compatmeta is empty: this is expected when dryrun = True'

        print 'compatmeta contains ProcessID? (%s) See below:'%compatmeta
        pprint.pprint(self.compatmeta)

        self.log.verbose( "Using %s to build path" % str( compatmeta ) )
        if 'EvtClass' in compatmeta and not self.evtclass:
            self.evtclass = JobHelpers.getValue( compatmeta['EvtClass'], str, basestring )

        if 'EvtType' in compatmeta and not self.evttype:
            self.evttype = JobHelpers.getValue( compatmeta['EvtType'], str, basestring )
        elif 'GenProcessType' in compatmeta and not self.evttype:
            self.evttype = JobHelpers.getValue( compatmeta['GenProcessType'], str, basestring )
        elif not self.evttype:
            return self._reportError( "Neither EvtType nor GenProcessType are "
                                      "in the metadata: if you dont set app "
                                      "evttype with setEvtType at least one "
                                      "should be." )

        if 'GenProcessName' in compatmeta:
            self.genprocname = compatmeta['GenProcessName']

        if not self.genprocname:
            return self._reportError( "GenProcessName is missing! It should appear in the basename")

        if 'GenProcessID' in compatmeta:
            self.processID = JobHelpers.getValue( compatmeta['GenProcessID'], int, (int, long) )
        elif 'ProcessID' in compatmeta:
            self.processID = JobHelpers.getValue( compatmeta['ProcessID'], int, (int, long) )
        else:
            return self._reportError( "Cannot find ProcessID, it's mandatory for path definition" )

        if 'Energy' in compatmeta:
            self.energycat = JobHelpers.getValue( compatmeta['Energy'], str, (int, long, basestring) )

        if 'MachineParams' in compatmeta:
            self.machineparams = JobHelpers.getValue( compatmeta['MachineParams'], str, basestring )
        if not self.machineparams:
            return self._reportError( "MachineParams should part of the metadata" )
        gendata = False
        if 'Datatype' in compatmeta:
            self.datatype = JobHelpers.getValue( compatmeta['Datatype'], str, basestring )
            if self.datatype.lower() == 'gen':
                gendata = True

        if 'DetectorModel' in compatmeta and not gendata:
            self.detector = JobHelpers.getValue( compatmeta['DetectorModel'], str, basestring )

        self.compatmeta = compatmeta
        self.basename = ''
#
        if not self.energycat:# FIXME
            print "Printing metadata before exit:"
            pprint.pprint( self.compatmeta )
            return self._reportError("ERROR::ILDProductionJobDBD.py: self.energycat is null")

        self.energy = Decimal( self.energycat )    
        
        self.inputBKSelection = metadata
        self.prodparameters["FCInputQuery"] = self.inputBKSelection

        self.inputdataquery = True
        return S_OK()        
        
    def _addRealFinalization( self ):
        """ See :mod:`~ILCDIRAC.Interfaces.API.NewInterface.ProductionJob` for definition
        """
        importLine = 'from ILCDIRAC.Workflow.Modules.<MODULE> import <MODULE>'
        dataUpload = ModuleDefinition( 'UploadOutputData' )
        dataUpload.setDescription( 'Uploads the output data' )
        self._addParameter( dataUpload, 'enable', 'bool', False, 'EnableFlag' )
        body = string.replace( importLine, '<MODULE>', 'UploadOutputData' )
        dataUpload.setBody( body )

        failoverRequest = ModuleDefinition( 'FailoverRequest' )
        failoverRequest.setDescription( 'Sends any failover requests' )
        self._addParameter( failoverRequest, 'enable', 'bool', False, 'EnableFlag' )
        body = string.replace( importLine, '<MODULE>', 'FailoverRequest' )
        failoverRequest.setBody( body )

        registerdata = ModuleDefinition( 'ILDRegisterOutputData' )
        registerdata.setDescription( 'Module to add in the metadata catalog the relevant info about the files' )
        self._addParameter( registerdata, 'enable', 'bool', False, 'EnableFlag' )
        body = string.replace( importLine, '<MODULE>', 'ILDRegisterOutputData' )
        registerdata.setBody( body )

        logUpload = ModuleDefinition( 'UploadLogFile' )
        logUpload.setDescription( 'Uploads the output log files' )
        self._addParameter( logUpload, 'enable', 'bool', False, 'EnableFlag' )
        body = string.replace( importLine, '<MODULE>', 'UploadLogFile' )
        logUpload.setBody( body )

        errorReport = ModuleDefinition('ReportErrors')
        errorReport.setDescription('Reports errors at the end')
        body = importLine.replace('<MODULE>', 'ReportErrors')
        errorReport.setBody(body)

        finalization = StepDefinition( 'Job_Finalization' )
        finalization.addModule( dataUpload )
        up = finalization.createModuleInstance( 'UploadOutputData', 'dataUpload' )
        up.setValue( "enable", self.finalsdict['uploadData'] )

        finalization.addModule( registerdata )
        # TODO: create ILDRegisterOutputData
        ro = finalization.createModuleInstance( 'ILDRegisterOutputData', 'ILDRegisterOutputData' )
        ro.setValue( "enable", self.finalsdict['registerData'] )

        finalization.addModule( logUpload )
        ul = finalization.createModuleInstance( 'UploadLogFile', 'logUpload' )
        ul.setValue( "enable", self.finalsdict['uploadLog'] )

        finalization.addModule( failoverRequest )
        fr = finalization.createModuleInstance( 'FailoverRequest', 'failoverRequest' )
        fr.setValue( "enable", self.finalsdict['sendFailover'] )

        finalization.addModule(errorReport)
        fr = finalization.createModuleInstance('ReportErrors', 'reportErrors')

        self.workflow.addStep( finalization )
        self.workflow.createStepInstance( 'Job_Finalization', 'finalization' )

        return S_OK() 
    
    def _jobSpecificParams( self, application ):
        """ For production additional checks are needed: ask the user
        """
        retCheck = self.__checkParameters( application )
        if not retCheck['OK']:
            return retCheck

        if retCheck.get('Value') is not None and "ShortCut" in retCheck.get('Value'):
            ##Short cut the function because of overlay
            return S_OK()

        if self.energy: ##APS: Is it possible for there no being any energy?
            self._setParameter( "Energy", "float", float( self.energy ), "Energy used" )
            self.prodparameters["Energy"] = float( self.energy )

        energypath = "%s-%s" % ( self.energy, self.machineparams ) # e.g.: 1000-B1s_ws
 
        retFileName = self.__createFileName( application )
        if not retFileName['OK']:
            self.log.error( "Filename creation Failed", retFileName['Message'] )
            return retFileName

        ##Always use ILDConfig for the path
        if 'ILDConfigVersion' not in self.prodparameters and application.datatype.lower() != "gen":
            return S_ERROR( "ILDConfig not set, it is mandatory for the path "
                            "definition, please use p.setILDConfig() before"
                            "appending applications" )
        ildConfigPath = self.prodparameters.get( "ILDConfigVersion", "" ) + "/"

        path = self.basepath

        if not self._recBasePaths:
          self.setReconstructionBasePaths( self.basepath, self.basepath )

        # ##Need to resolve file names and paths
        # TODO: change basepath for ILD Don't forget LOG PATH in ProductionOutpuData module
        if hasattr( application, "setOutputRecFile" ) and not application.willBeCut:

            for outType in ( 'REC', 'DST' ):
                metaPath = joinPathForMetaData( self._recBasePaths[ outType ], outType.lower() )
                self.finalMetaDict[ metaPath ] = { 'Datatype': outType }

                metaPath = joinPathForMetaData( metaPath, energypath )
                self.finalMetaDict[ metaPath ] = { 'Energy': str(self.energy),
                                                   'MachineParams':self.machineparams }

                metaPath = joinPathForMetaData( metaPath, self.evttype )
                self.finalMetaDict[ metaPath ] = {'EvtType' : self.evttype.strip('/') }

                metaPath = joinPathForMetaData( metaPath, self.detector )
                self.finalMetaDict[ metaPath ] = { 'DetectorModel' : self.detector.strip('/') }

                metaPath = joinPathForMetaData( metaPath, ildConfigPath )
                self.finalMetaDict[ metaPath ] = { 'ILDConfig': self.prodparameters['ILDConfigVersion'] }

                fname = "%s_%s.slcio" % ( self.basename, outType.lower() )
                print '+++Output %s Filename: %s' %( outType, fname )
                getattr(application, 'setOutput%sFile' % outType.capitalize())( fname, metaPath )
                self.finalpaths.append( metaPath )
                path = metaPath

        elif hasattr( application, 'outputFile' ) and \
             hasattr( application, 'datatype' ) and \
             not application.outputFile and \
             not application.willBeCut:

            if not application.datatype and self.datatype:
                application.datatype = self.datatype

            path_gen_or_sim = ''
            if application.datatype == 'gen':
                # for historical reasons generated stdhep separated from mokka/dst/rec files

                ## Set DataType
                path_gen_or_sim = joinPathForMetaData( '/'.join( self.basepath.split( '/' )[:-2] ) + '/splitted/' )
                self.finalMetaDict[ path_gen_or_sim ] = { 'Datatype': 'gen' }

                ## Set MachineParams and Energy
                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, energypath )
                self.finalMetaDict[ path_gen_or_sim ] = { 'Energy': str(self.energy),
                                                          'MachineParams':self.machineparams }

                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, self.evttype )
                self.finalMetaDict[ path_gen_or_sim ] = { 'EvtType': self.evttype.strip('/') }

            elif application.datatype == 'SIM':
                ## Set DataType
                path_gen_or_sim = joinPathForMetaData( self.basepath + 'sim' )
                self.finalMetaDict[ path_gen_or_sim ] = { 'Datatype': 'SIM' }

                ## Set Energy and MachineParams
                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, energypath )
                self.finalMetaDict[ path_gen_or_sim ] = { 'Energy': str(self.energy),
                                                          'MachineParams':self.machineparams }

                ## Set EventType
                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, self.evttype )
                self.finalMetaDict[ path_gen_or_sim ] = { 'EvtType': self.evttype.strip('/') }

                ## Set DetectorModel
                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, self.detector )
                self.finalMetaDict[ path_gen_or_sim ] = { 'DetectorModel': self.detector.strip('/') }

                ## Always use ILDConfig
                path_gen_or_sim = joinPathForMetaData( path_gen_or_sim, ildConfigPath )
                self.finalMetaDict[ path_gen_or_sim ].update( { 'ILDConfig': self.prodparameters['ILDConfigVersion'] } )

            path = path_gen_or_sim


            path_gen_or_sim = path_gen_or_sim.rstrip('/')+'/'
            
            self.log.info( 'Will store the files under: %s' % path_gen_or_sim )
            self.finalpaths.append( path_gen_or_sim )

            extension = 'stdhep'
            if application.datatype in ['SIM', 'REC']:
                extension = 'slcio'
            fname = self.basename + "_%s" % ( application.datatype.lower() ) + "." + extension
            application.setOutputFile( fname, path_gen_or_sim )    
            print "+++Output SIM/GEN Filename", fname

        ## Applied for all productions, these are the metadata at the production level
        metap = {}
        ## Drop ILDConfig, MachineParams, Energy from this list, they are set at different level
        ## SoftwareTag is only sometimes at the prodID level
        for imeta in ['GenProcessName',
                      'NumberOfEvents',
                      'BeamParticle1','BeamParticle2',
                      'PolarizationB1','PolarizationB2']:
            if imeta in self.compatmeta:
                print 'Updating final metadata with {"%s":"%s"}' %(imeta,self.compatmeta[imeta])
                metap.update( {imeta : self.compatmeta[imeta]} )

        ## Add software to non-searchable metadata
        curpackage = "%s.%s" % (application.appname, application.version)
        if "SWPackages" in self.prodparameters:
          if not self.prodparameters["SWPackages"].count(curpackage):
            self.prodparameters["SWPackages"] += ";%s" % curpackage
        else:
          self.prodparameters["SWPackages"] = curpackage

        softmeta = application.appname + "." + application.version
        print "++++Software meta for", application.appname, "=", softmeta
        metap.update( { "SoftwareTag": softmeta } )

        self.prodMetaDict.update( metap )
        pprint.pprint( self.prodMetaDict )
        self.basepath = path

        self.checked = True

        return S_OK()



    def append( self, application ):
        """ Append application to production job, in addition to `Job.append` calls checkProductionMetadata for applications

        :param application: Application instance
        :type application: :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Application`
        :returns: S_OK, S_ERROR
        """

        ## First check meta data before doing the normal append
        resMD = application.checkProductionMetaData( self.compatmeta )
        if not resMD['OK']:
            self.log.error( "Failed to check production Meta Data", resMD['Message'] )
            return resMD

        return super(ILDProductionJobDBD, self).append( application )

    def __createFileName(self, application): #pylint: disable=too-many-branches
        """ create the filename for ILD productions

        A partial description of the desired filename is found here
        https://svnweb.cern.ch/trac/lcgentools/browser/trunk/ILC/documents/generator-conventions.pdf

        :param application: application used for production job
        :returns: S_OK, S_ERROR
        """
        # TODO: Make sure basename is correct. Maybe allow for setting basename prefix
        # Final name being e.g. NAME_rec.slcio, need to define NAME, maybe based on meta data (include
        # EvtClass automatically)
        if not self.basename:
            if 'ILDConfigVersion' in self.prodparameters:
                if application.appname in ( 'mokka', 'ddsim' ): # sim
                    self.basename = 's' + self.prodparameters['ILDConfigVersion']
                elif application.appname == 'marlin': # reco
                    self.basename = 'r' + self.prodparameters['ILDConfigVersion']
                    self.basename += '.s' + self.compatmeta['ILDConfig']
                # we dont need this tag in stdhep's: metadata search will fail
                # if not present
                elif application.appname == 'stdhepsplit':
                    self.compatmeta.pop( 'SoftwareTag', None )
                    self._reportError( "Drop 'SoftwareTag' from metadata: not needed for stdhepsplit app" )
                # need extension if planning to use additional modules (LCIOSplit)
            else:
                if application.datatype not in ( 'gen', 'gensplit'): # for stdhepsplit we dont need to return
                    self._reportError(" Printing metadata before exit:")
                    pprint.pprint( self.compatmeta )
                    pprint.pprint( self.prodparameters )
                    return self._reportError( "'ILDConfigVersion' should be defined to build the path")

        if 'DetectorModel' in self.compatmeta:
            self.basename += '.m' + self.compatmeta['DetectorModel']
        elif self.detector:
            self.basename += '.m' + self.detector

        if self.energy:
            self.basename += '.' if self.basename else ''
            self.basename += 'E' + str( self.energy )

            if 'MachineParams' in self.compatmeta:
                self.basename += '-' + self.compatmeta['MachineParams']

        if 'GenProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['GenProcessID'] )
        elif 'ProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['ProcessID'] )

        if 'GenProcessName' in self.compatmeta:
            self.basename += '.P' + self.compatmeta['GenProcessName']
        elif self.genprocname:
            self.basename += '.P' + self.genprocname
        else:
            return self._reportError( "GenProcessName is missing! It should appear in the basename")

        ##always use e and p for beam polarisation fields
        self.basename += '.e%s' % self.compatmeta.get( 'PolarizationB1', '' )
        self.basename += '.p%s' % self.compatmeta.get( 'PolarizationB2', '' )

        return S_OK()


    def __checkParameters( self, application ): #pylint: disable=too-many-return-statements, too-many-branches
      """ check if everything is consistent, parameters set, meta data defined, applications configured correctly...


      :returns: S_OK, S_ERROR
      """

      if self.created:
          return S_ERROR( "The production was created, you cannot add new applications to the job." )

      curpackage = "%s.%s" % ( application.appname, application.version )
      self.prodparameters.setdefault( 'SoftwareTag', curpackage )
      if curpackage not in self.prodparameters["SoftwareTag"]:
          self.prodparameters["SoftwareTag"] += ";%s" % curpackage

      if not application.logFile:
          logf = application.appname + "_" + application.version + "_@{STEP_ID}.log"
          resLog = application.setLogFile( logf )
          if not resLog['OK']:
              return resLog

      if not self.energy:
          if application.energy:
              self.energy = Decimal( str( application.energy ) )
          else:
              return S_ERROR( "Could not find the energy defined, it is needed for the production definition." )
      elif not application.energy:
          resEnergy = application.setEnergy( float( self.energy ) )
          if not resEnergy['OK']:
              return resEnergy

      if not self.evttype:
          if hasattr( application, 'evttype' ):
              self.evttype = application.evtType
          else:
              return S_ERROR( "Event type not found nor specified, it is mandatory for the production paths." )

      if not application.accountInProduction:
          # needed for the overlay
          resUPP = self._updateProdParameters( application )
          if not resUPP['OK']:
              return resUPP
          self.checked = True
          return S_OK("ShortCut")

      if not self.outputStorage:
          return S_ERROR( "You need to specify the Output storage element" )

      resSE = application.setOutputSE( self.outputStorage )
      if not resSE['OK']:
          return resSE

      if not self.detector:
          if hasattr( application, "detectorModel" ):
              self.detector = application.detectorModel
              if not self.detector:
                  return S_ERROR( "Application does not know which model to use, so the production does not either." )

      return S_OK()
