"""
ILD DBD specific  job utility

@author: S. Poss
@since: Jul 01, 2012
"""

__RCSID__ = "235f82e (2014-10-20 17:03:09 +0200) Andre Sailer <andre.philippe.sailer@cern.ch>"

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC import S_OK, S_ERROR
from ILCDIRAC.Core.Utilities.LFNPathUtilities import joinPathForMetaData
import types, string
from decimal import Decimal

#pylint: disable=W0311
#pylint: disable=R0902
#pylint: disable=R0904
class ILDProductionJob( ProductionJob ):
    """ILD Production Jobs definition"""
    def __init__( self ):
        super( ILDProductionJob, self ).__init__()
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
        self.usesofttag = False
        self.matchToInput=''


    def setMatchToInput( self, matchToInput ):
        """ Help to find faster the input directory
        """
        self.matchToInput = matchToInput


    def setSoftwareTagInFinalPath( self, softwaretag ):
        """ I need softwaretag registered on the splitted stdhep files
        """
        for finalpaths in self.finalpaths:
            self.finalMetaDict[finalpaths].update({"SoftwareTag":softwaretag})

        return S_OK()


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

    def setUseSoftTagInPath( self, usesoft = True):
        """ At DBD simulation uses a lower ilcsoftware version than reconstruction
            That version is included in the path
        """
        self.usesofttag = usesoft

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
        metakeys = metadata.keys()
        res = self.fc.getMetadataFields()

        for k,v in metadata.items():
            print "[0] meta[%s] %s"%(k,v)

        if not res['OK']:
            print "Could not contact File Catalog"
            return S_ERROR()
        metaFCkeys = res['Value']['DirectoryMetaFields'].keys()
        metaFCkeys.extend( res['Value']['FileMetaFields'].keys() )

        for key in metakeys:
            for meta in metaFCkeys:
                if meta != key:
                    if meta.lower() == key.lower():
                        return self._reportError( "Key syntax error %s, should be %s" % ( key, meta ), name='ILDProduction' )
            if not metaFCkeys.count( key ):
                return self._reportError( "Key %s not found in metadata keys, allowed are %s" % ( key, metaFCkeys ) )
        # if 'ProdID' not in metadata:
        #    return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")

        # res = self.fc.findDirectoriesByMetadata( metadata )

        # do i need this?
        tmp_metadata = {}
        tmp_metadata.update(metadata)
        # for kk in ['SoftwareTag', 'ILDConfig']:
        for kk in ['SoftwareTag', 'ILDConfig','ProcessID']:
            tmp_metadata.pop(kk, None) # why i was dropping ProdID?

        # using tmp_metadata for search dirs (metadata not modified)
        res = self.fc.findDirectoriesByMetadata( tmp_metadata )

        if not res['OK']:
            return self._reportError( "Error looking up the catalog for available directories" )
        elif len( res['Value'] ) < 1:
            return self._reportError( 'Could not find any directory corresponding to the query issued' )
        dirs = res['Value'].values()
        compatmeta = {}

        print 'dirs found: %d' %len(dirs)
        # for d in dirs:
        #     print '%s'%d
        dir_found = False
        if self.matchToInput:
            print 'Will try to match dir with %s' %self.matchToInput
        for mdir in dirs:
            if self.matchToInput:
                if not self.matchToInput in mdir:
                    continue
            if 'ProdID' in metadata:
                val = '/' + str(metadata['ProdID']).zfill(8)
                if not val in mdir:
                    continue

            dir_found = True
            print '[debug tino] Found mdir %s' %mdir
            res = self.fc.getDirectoryUserMetadata( mdir )
            if not res['OK']:
                return self._reportError( "Error looking up the catalog for directory metadata" )
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
                return self._reportError( 'Failed to get file metadata, cannot build filename' )
            compatmeta.update( res['Value'] )
            print '[tino debug] Updated compatmeta to: %s' %compatmeta

            for k,v in self.compatmeta.items():
                print "my_lfn %s compatmeta[%s] %s"%(my_lfn,k,v)

        else:
            if not self.dryrun:
              print res
              self._reportError( "No files matching the metadata: Metadata is wrong or files are not under /ilc/prod/ilc directory" )
                

        if not len(compatmeta):
            print 'ERROR, compatmeta is empty: this is expected when dryrun = True'

        print 'compatmeta contains ProcessID? (%s) See below:'%compatmeta
        for k,v in self.compatmeta.items():
            print "compatmeta[%s] %s"%(k,v)

        self.log.verbose( "Using %s to build path" % str( compatmeta ) )
        if compatmeta.has_key( 'EvtClass' ):
            if type( compatmeta['EvtClass'] ) in types.StringTypes and not self.evtclass:
                self.evtclass = compatmeta['EvtClass']
            if type( compatmeta['EvtClass'] ) == type( [] ) and not self.evtclass:
                self.evtclass = compatmeta['EvtClass'][0]
        if compatmeta.has_key( 'EvtType' ):
            if type( compatmeta['EvtType'] ) in types.StringTypes and not self.evttype:
                self.evttype = compatmeta['EvtType']
            if type( compatmeta['EvtType'] ) == type( [] ) and not self.evttype:
                self.evttype = compatmeta['EvtType'][0]
        # elif compatmeta.has_key( 'GenProcessName' ):
        #     if type( compatmeta['GenProcessName'] ) in types.StringTypes:
        #         self.evttype = compatmeta['GenProcessName']
        #     if type( compatmeta['GenProcessName'] ) == type( [] ):
        #         self.evttype = compatmeta['GenProcessName'][0]            
        elif compatmeta.has_key( 'GenProcessType' ):
            if type( compatmeta['GenProcessType'] ) in types.StringTypes and not self.evttype:
                self.evttype = compatmeta['GenProcessType']
            if type( compatmeta['GenProcessType'] ) == type( [] ) and not self.evttype:
                self.evttype = compatmeta['GenProcessType'][0]
        elif not self.evttype:
            return self._reportError( "Neither EvtType nor GenProcessType are in the metadata: if you dont set app evttype with setEvtType at least one should be " )

        if 'GenProcessName' in compatmeta:
            self.genprocname = compatmeta['GenProcessName']

        if not self.genprocname:
            return self._reportError( "GenProcessName is missing! It should appear in the basename")

        if 'GenProcessID' in compatmeta:
            if type( compatmeta['GenProcessID'] ) == type( 2L ):
                self.processID = compatmeta['GenProcessID']
            if type( compatmeta['GenProcessID'] ) == type( [] ):
                self.processID = int( compatmeta['GenProcessID'][0] )
        elif 'ProcessID' in compatmeta:
            if type( compatmeta['ProcessID'] ) == type( 2L ):
                self.processID = compatmeta['ProcessID']
            if type( compatmeta['ProcessID'] ) == type( [] ):
                self.processID = int( compatmeta['ProcessID'][0] )
        else:
            return self._reportError( "Cannot find ProcessID, it's mandatory for path definition" )
                
                
        if compatmeta.has_key( "Energy" ):
            if type( compatmeta["Energy"] ) in types.StringTypes:
                self.energycat = compatmeta["Energy"]
            if type( compatmeta["Energy"] ) == type( [] ):
                self.energycat = compatmeta["Energy"][0]

        if compatmeta.has_key( "MachineParams" ):
            if type( compatmeta["MachineParams"] ) in types.StringTypes:
                self.machineparams = compatmeta["MachineParams"]
            if type( compatmeta["MachineParams"] ) == type( [] ):
                self.machineparams = compatmeta["MachineParams"][0]
        if not self.machineparams:
            return self._reportError( "MachineParams should part of the metadata" )        
        gendata = False        
        if compatmeta.has_key( 'Datatype' ):
            if type( compatmeta['Datatype'] ) in types.StringTypes:
                self.datatype = compatmeta['Datatype']
                if compatmeta['Datatype'].lower() == 'gen':
                    gendata = True
            if type( compatmeta['Datatype'] ) == type( [] ):
                self.datatype = compatmeta['Datatype'][0]
                if compatmeta['Datatype'][0].lower() == 'gen':
                    gendata = True

        if compatmeta.has_key( "DetectorModel" ) and not gendata:
            if type( compatmeta["DetectorModel"] ) in types.StringTypes:
                self.detector = compatmeta["DetectorModel"]
            if type( compatmeta["DetectorModel"] ) == type( [] ):
                self.detector = compatmeta["DetectorModel"][0]

        self.compatmeta = compatmeta
        self.basename = ''
#
        if not self.energycat:# FIXME
            print "Printing metadata before exit:"
            for k,v in self.compatmeta.items():
                print "compatmeta[%s] %s"%(k,v)
            return self._reportError("ERROR::ILDProductionJob.py: self.energycat is null")

        self.energy = Decimal( self.energycat )    
        
        self.inputBKSelection = metadata
        self.prodparameters["FCInputQuery"] = self.inputBKSelection

        self.inputdataquery = True
        return S_OK()        
        
    def _addRealFinalization( self ):
        """ See L{ProductionJob} for definition
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
        
        self.workflow.addStep( finalization )
        self.workflow.createStepInstance( 'Job_Finalization', 'finalization' )

        return S_OK() 
    
    def _jobSpecificParams( self, application ):
        """ For production additional checks are needed: ask the user
        """
        if self.created:
            return S_ERROR( "The production was created, you cannot add new applications to the job." )

        if not application.LogFile:
            logf = application.appname + "_" + application.Version + "_@{STEP_ID}.log"
            res = application.setLogFile( logf )
            if not res['OK']:
                return res
            
            # in fact a bit more tricky as the log files have the prodID and jobID in them
        
        if "SoftwareTag" in self.prodparameters:
            curpackage = "%s.%s" % ( application.appname, application.Version )
            if not self.prodparameters["SoftwareTag"].count( curpackage ):
                self.prodparameters["SoftwareTag"] += ";%s" % ( curpackage )
        else :
            self.prodparameters["SoftwareTag"] = "%s.%s" % ( application.appname, application.Version )
            
        # softwarepath = application.appname+application.Version
        if 'ILDConfigVersion' in self.prodparameters:
            softwarepath = self.prodparameters['ILDConfigVersion']
        else:
            return S_ERROR( "ILDConfig not set, it is mandatory for path definition, please use p.setILDConfig() before appending applications" )

        # override softwarepath if asked by user
        if self.usesofttag:
            if 'SoftwareTag' in self.compatmeta:
                softwarepath = self.compatmeta['SoftwareTag']
            else:
                 print "Warning: usesofttag is True but no SoftwareTag in metadata. For Mokka or Marlin job this is wrong"

        if not self.energy:
            if application.Energy:
                self.energy = Decimal( str( application.Energy ) )
            else:
                return S_ERROR( "Could not find the energy defined, it is needed for the production definition." )
        elif not application.Energy:
            res = application.setEnergy( float( self.energy ) )
            if not res['OK']:
                return res
        if self.energy:
            self._setParameter( "Energy", "float", float( self.energy ), "Energy used" )
            self.prodparameters["Energy"] = float( self.energy )
            
        if not self.evttype:
            if hasattr( application, 'EvtType' ):
                self.evttype = application.evtType
            else:
                return S_ERROR( "Event type not found nor specified, it's mandatory for the production paths." )    
            
        if not application.accountInProduction:
            # needed for the stupid overlay
            res = self._updateProdParameters( application )
            if not res['OK']:
                return res    
            self.checked = True
            return S_OK()    
        
        if not self.outputStorage:
            return S_ERROR( "You need to specify the Output storage element" )
        
        res = application.setOutputSE( self.outputStorage )
        if not res['OK']:
            return res
        
        if not self.detector:
            if hasattr( application, "DetectorModel" ):
                self.detector = application.DetectorModel            
                if not self.detector:
                    return S_ERROR( "Application does not know which model to use, so the production does not either." )
            # else:
            #    return S_ERROR("Application does not know which model to use, so the production does not either.")
        
        
        energypath = "%s-%s/" % ( self.energy, self.machineparams )  # 1000-B1s_ws
 
        
        # TODO: Make sure basename is correct. Maybe allow for setting basename prefix
        # Final name being e.g. NAME_rec.slcio, need to define NAME, maybe based on meta data (include 
        # EvtClass automatically)
        if not self.basename:
            # self.basename = 's' + self.prodparameters['ILDConfigVersion']
            if 'SoftwareTag' in self.compatmeta:
                if application.appname == 'mokka':     # sim
                    self.basename = 's' + self.compatmeta['SoftwareTag']
                elif application.appname == 'marlin':  # reco
                    self.basename = 'r' + self.prodparameters['ILDConfigVersion']
                    self.basename += '.s' + self.compatmeta['SoftwareTag']
                elif application.appname == 'stdhepsplit':  # we dont need this tag in stdhep's: metadata search will fail if not present
                    self.compatmeta.pop('SoftwareTag')
                    self._reportError( "Drop 'SoftwareTag' from metadata: not needed for stdhepsplit app")
                # need extension if planning to use additional modules (LCIOSplit)
            else:
              if application.datatype != 'gen': # for stdhepsplit we dont need to return
                self._reportError(" Printing metadata before exit:")
                for k,v in self.compatmeta.items():
                    print "compatmeta[%s] %s"%(k,v)

                return self._reportError( "'SoftwareTag' should be defined to build the path")

        if 'DetectorModel'    in self.compatmeta:
            self.basename += '.m' + self.compatmeta['DetectorModel']
        elif self.detector:
            self.basename += '.m' + self.detector
        if self.energy:
            if not self.basename:
              self.basename += 'E' + str( self.energy )
            else:
              self.basename += '.E' + str( self.energy )
        if 'MachineParams' in self.compatmeta:
            self.basename += '-' + self.compatmeta['MachineParams']
            
        if 'GenProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['GenProcessID'] )
        elif 'ProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['ProcessID'] )
        # if 'EvtType' in self.compatmeta:
        #     self.basename += '.P' + self.compatmeta['EvtType']  # To be fixed with Jan
        # elif 'GenProcessType' in self.compatmeta:
        #     self.basename += '.P' + self.compatmeta['GenProcessType']
        # elif self.evttype:
        #     self.basename += '.P' + self.evttype
        # ILD convention is adding GenProcessName not type


        # if self.Energy in [250,350] and application.appname == 'stdhepsplit':
        #     print "Called swapping GenProcessName '%s' with EvtType '%s' metadata fields" % (self.genprocname, self.genprocname)
        #     self.__swapGenProcNameEvtType()

        if 'GenProcessName' in self.compatmeta:
            self.basename += '.P' + self.compatmeta['GenProcessName']
        elif self.genprocname:
            self.basename += '.P' + self.genprocname
        else:
            return self._reportError( "GenProcessName is missing! It should appear in the basename")

        if 'BeamParticle1' in self.compatmeta:
            self.basename += '.'
            if self.compatmeta['BeamParticle1'] == 'e1':
                self.basename += 'e'
            elif self.compatmeta['BeamParticle1'] == 'E1':
                self.basename += 'p'
            else:
                self.basename += self.compatmeta['BeamParticle1']
        if 'PolarizationB1' in self.compatmeta:
            self.basename += self.compatmeta['PolarizationB1']
        if 'BeamParticle2' in self.compatmeta:
            self.basename += '.'
            if self.compatmeta['BeamParticle2'] == 'E1':
                self.basename += 'p'
            elif self.compatmeta['BeamParticle2'] == 'e1':
                self.basename += 'e'
            else:
                self.basename += self.compatmeta['BeamParticle2']
        if 'PolarizationB2' in self.compatmeta:
            self.basename += self.compatmeta['PolarizationB2']

        
        if not self.machine[-1] == '/':
            self.machine += "/"
            
        if not self.evttype[-1] == '/':
            evttypemeta = self.evttype
            self.evttype += '/'    
        else:
            evttypemeta = self.evttype.rstrip( "/" )
            
        if not self.evtclass[-1] == '/':
            evtclassmeta = self.evtclass
            self.evtclass += '/'    
        else:
            evtclassmeta = self.evtclass.rstrip( "/" )

        # softwaremeta = softwarepath # this changes soft to ildconf.
        softwaremeta = ''
        detectormeta = ''
        if 'SoftwareTag' in self.compatmeta:
            softwaremeta = self.compatmeta['SoftwareTag']
        softwarepath += "/"
        if self.detector:
            if not self.detector[-1] == "/":
                detectormeta = self.detector
                self.detector += "/"
            else:
                detectormeta = self.detector.rstrip( "/" )
            
        path = self.basepath
        # ##Need to resolve file names and paths
        # TODO: change basepath for ILD Don't forget LOG PATH in ProductionOutpuData module
        if hasattr( application, "setOutputRecFile" ) and not application.willBeCut:
            # metaBasePathRec = joinPathForMetaData(self.basepath, 'rec', energypath, self.evttype)
            metaBasePathRec = joinPathForMetaData(self.basepath, 'rec', energypath)
            # self.finalMetaDict[ metaBasePathRec ] = {"EvtClass" : evtclassmeta}
            self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype )] = {"EvtType" : evttypemeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype, str( self.processID ))] = {'ProcessID': self.processID}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype, str( self.processID ), self.detector)] = {"DetectorModel" : detectormeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype, str( self.processID ), self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}

            # no processid
            self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype, self.detector)] = {"DetectorModel" : detectormeta}
            self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.evttype, self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}
            
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.detector)] = {"DetectorModel" : detectormeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathRec, self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}

            # this part is from Andre
            fname = self.basename + "_rec.slcio"
             # fname = self.basename + "-REC.slcio"# system will not find files
            # pathRec = joinPathForMetaData( self.basepath , 'rec' , energypath , self.evtclass , self.evttype , str( self.processID ) , self.detector , softwarepath)
            # pathRec = joinPathForMetaData( self.basepath , 'rec' , energypath , self.evttype , str( self.processID ) , self.detector , softwarepath)

            # no processid
            pathRec = joinPathForMetaData( self.basepath , 'rec' , energypath , self.evttype , self.detector , softwarepath)
            # pathRec = joinPathForMetaData( self.basepath , 'rec' , energypath , self.evttype , self.detector , softwarepath)
            application.setOutputRecFile( fname, pathRec )
            self.finalpaths.append( pathRec )
            # metaBasePathDst = joinPathForMetaData(self.basepath, 'dst', energypath, self.evttype)
            metaBasePathDst = joinPathForMetaData(self.basepath, 'dst', energypath)
            # self.finalMetaDict[ metaBasePathDst ] = {"EvtClass" : evtclassmeta}
            self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype )] = {"EvtType" : evttypemeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype, str( self.processID ))] = {'ProcessID': self.processID}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype, str( self.processID ), self.detector)] = {"DetectorModel" : detectormeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype, str( self.processID ), self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}

            # no processid
            self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype, self.detector)] = {"DetectorModel" : detectormeta}
            self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.evttype, self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}
            
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.detector)] = {"DetectorModel" : detectormeta}
            # self.finalMetaDict[ joinPathForMetaData( metaBasePathDst, self.detector, softwarepath)] = {"SoftwareTag" : softwaremeta}
            fname = self.basename + "_dst.slcio"
            # fname = self.basename + "-DST.slcio"# system will not find files
            # pathDst = joinPathForMetaData( self.basepath , 'dst' , energypath , self.evtclass , self.evttype , str( self.processID ) , self.detector , softwarepath)
            # pathDst = joinPathForMetaData( self.basepath , 'dst' , energypath , self.evttype , str( self.processID ) , self.detector , softwarepath)

            # no processid
            pathDst = joinPathForMetaData( self.basepath , 'dst' , energypath , self.evttype, self.detector , softwarepath)
            # pathDst = joinPathForMetaData( self.basepath , 'dst' , energypath , self.evttype , self.detector , softwarepath)
            application.setOutputDstFile( fname, pathDst )
            self.finalpaths.append( pathDst )

            path = pathDst
        elif hasattr( application, "outputFile" ) and hasattr( application, 'datatype' ) and ( not application.outputFile ) and ( not application.willBeCut ):
            if ( not application.datatype ) and self.datatype:
                application.datatype = self.datatype
            # if application.datatype == 'gen':
            #     datatype = 'generated/'
            #     self.basepath = "/".join( self.basepath.split( "/" )[:-2] ) + '/'  # because for generated, it's common to all ILC
            # elif application.datatype == 'SIM':
            #     datatype = 'sim/'
            # path = self.basepath + datatype
            # # path += energypath + self.evtclass
            # path += energypath + self.evttype
            # self.finalMetaDict[path] = {"EvtType" : evttypemeta}

            path_gen_or_sim = ""
            if application.datatype == 'gen':
                # for historical reasons generated stdhep separated from mokka/dst/rec files
                # path_gen_or_sim = joinPathForMetaData( '/ilc/prod/ilc/mc-dbd/' , 'generated' , energypath , self.evttype , str( self.processID ) , self.detector , softwarepath)
                # no processid
                # path_gen_or_sim = joinPathForMetaData( '/ilc/prod/ilc/mc-dbd/' , 'generated' , energypath , self.evttype, self.detector , softwarepath)
                # path_gen_or_sim = joinPathForMetaData( "/".join( self.basepath.split( "/" )[:-2] ) + '/' , 'generated' , energypath , self.evttype, self.detector , softwarepath)
                # mc-dbd.generated already include 'generated'
                path_gen_or_sim = joinPathForMetaData( "/".join( self.basepath.split( "/" )[:-2] ) + '/' ,  energypath , self.evttype, self.detector , softwarepath)

            elif application.datatype == 'SIM':
                # path_gen_or_sim = joinPathForMetaData( self.basepath , 'sim' , energypath , self.evttype , str( self.processID ) , self.detector , softwarepath)
                # no processid
                path_gen_or_sim = joinPathForMetaData( self.basepath , 'sim' , energypath , self.evttype, self.detector , softwarepath)

            path = path_gen_or_sim
            self.finalMetaDict[path_gen_or_sim] = {"EvtType" : evttypemeta}
            metap = {}
            # i want processid just in the last metapath
            # if 'GenProcessID' in self.compatmeta:
            #     metap.update( {"ProcessID":self.compatmeta['GenProcessID']} )  # because we need 2 fields for the same info: file and directory metadata
            #     self.finalMetaDict[path_gen_or_sim] = metap     
            # elif 'ProcessID' in self.compatmeta:
            #     metap.update( {"ProcessID":self.compatmeta['ProcessID']} )    
            #     self.finalMetaDict[path_gen_or_sim] = metap

            # If not included beam/pol the paths are not set correctly
            # for imeta in ['SoftwareTag','GenProcessName','MachineParams','Energy','BeamParticle1','BeamParticle2','PolarizationB1','PolarizationB2']: # NumberOfEvents needed or Mokka will fail
            for imeta in ['NumberOfEvents','SoftwareTag','GenProcessName','MachineParams','Energy','BeamParticle1','BeamParticle2','PolarizationB1','PolarizationB2']:
                if imeta in self.compatmeta:
                    print 'Updating final metadata with {"%s":"%s"}' %(imeta,self.compatmeta[imeta])
                    metap.update( {imeta : self.compatmeta[imeta]} )    

            if application.datatype:
                print 'Updating final metadata with {"%s":"%s"}' %('Datatype',application.datatype)
                metap.update({'Datatype' : application.datatype})
                    
            self.finalMetaDict[path_gen_or_sim] = metap
            
            
            # if hasattr( application, "DetectorModel" ):
            #     if application.DetectorModel:
            #         path_gen_or_sim += application.DetectorModel
            #         self.finalMetaDict[path_gen_or_sim] = {"DetectorModel" : application.DetectorModel}
            #         path_gen_or_sim += '/'
            #     elif self.detector:
            #         path_gen_or_sim += self.detector
            #         self.finalMetaDict[path_gen_or_sim] = {"DetectorModel" : self.detector}
            #         path_gen_or_sim += '/'
            # path_gen_or_sim += softwarepath         
            # self.finalMetaDict[path_gen_or_sim] = {"SoftwareTag" : softwaremeta}

            if not path_gen_or_sim[-1] == '/':
                path_gen_or_sim += "/"
            
            self.log.info( "Will store the files under", "%s" % path_gen_or_sim )
            self.finalpaths.append( path_gen_or_sim )

            extension = 'stdhep'
            if application.datatype in ['SIM', 'REC']:
                extension = 'slcio'
            fname = self.basename + "_%s" % ( application.datatype.lower() ) + "." + extension
            application.setOutputFile( fname, path_gen_or_sim )    

        self.basepath = path

        if not res['OK']:
            return res            
        
        self.checked = True
            
        return S_OK()



    def append( self, application ):
        """ Append application to production job, in addition to `Job.Append` calls checkProductionMetadata for applications

        :param application: Application instance
        :type application: :mod:`~ILCDIRAC.Interfaces.API.NewInterface.Application`
        :returns: S_OK, S_ERROR
        """

        ## First check meta data before doing the normal append
        resMD = application.checkProductionMetaData( self.compatmeta )
        if not resMD['OK']:
            self.log.error( "Failed to check production Meta Data", resMD['Message'] )
            return resMD

        return super(ILDProductionJob, self).append( application )
