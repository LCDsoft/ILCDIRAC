########################################################################
# $HeadURL$
########################################################################

"""
SID DBD specific production job utility

@author: S. Poss
@since: Jul 01, 2012
"""

__RCSID__ = "$Id$"

from ILCDIRAC.Interfaces.API.NewInterface.ProductionJob import ProductionJob
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC import S_OK, S_ERROR

import types, string
from decimal import Decimal

#pylint: disable=W0311
class ILDProductionJob( ProductionJob ):
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
        
    def setILDConfig( self, Version ):
        """ This is because in the ProductionJob, it's called Config
        """
        return self.setConfig( Version )
        
    def setInputDataQuery( self, metadata ):
        """ Define the input data query needed, also get from the data the meta info requested to build the path
        """
        metakeys = metadata.keys()
        res = self.fc.getMetadataFields()
        if not res['OK']:
            print "Could not contact File Catalog"
            return S_ERROR()
        metaFCkeys = res['Value']['DirectoryMetaFields'].keys()
        metaFCkeys.extend( res['Value']['FileMetaFields'].keys() )

        for key in metakeys:
            for meta in metaFCkeys:
                if meta != key:
                    if meta.lower() == key.lower():
                        return self._reportError( "Key syntax error %s, should be %s" % ( key, meta ), name='SIDProduction' )
            if not metaFCkeys.count( key ):
                return self._reportError( "Key %s not found in metadata keys, allowed are %s" % ( key, metaFCkeys ) )
        # if not metadata.has_key("ProdID"):
        #    return self._reportError("Input metadata dictionary must contain at least a key 'ProdID' as reference")
        res = self.fc.findDirectoriesByMetadata( metadata )
        if not res['OK']:
            return self._reportError( "Error looking up the catalog for available directories" )
        elif len( res['Value'] ) < 1:
            return self._reportError( 'Could not find any directory corresponding to the query issued' )
        dirs = res['Value'].values()
        for mdir in dirs:
            res = self.fc.getDirectoryMetadata( mdir )
            if not res['OK']:
                return self._reportError( "Error looking up the catalog for directory metadata" )
            compatmeta = res['Value']
            compatmeta.update( metadata )
        
        # get all the files available, if any
        res = self.fc.findFilesByMetadata( metadata, '/ilc/prod/ilc' )
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
        
        self.log.verbose( "Using %s to build path" % str( compatmeta ) )
        if compatmeta.has_key( 'EvtClass' ):
            if type( compatmeta['EvtClass'] ) in types.StringTypes:
                self.evtclass = compatmeta['EvtClass']
            if type( compatmeta['EvtClass'] ) == type( [] ):
                self.evtclass = compatmeta['EvtClass'][0]
        if compatmeta.has_key( 'EvtType' ):
            if type( compatmeta['EvtType'] ) in types.StringTypes:
                self.evttype = compatmeta['EvtType']
            if type( compatmeta['EvtType'] ) == type( [] ):
                self.evttype = compatmeta['EvtType'][0]
        elif compatmeta.has_key( 'GenProcessName' ):
            if type( compatmeta['GenProcessName'] ) in types.StringTypes:
                self.evttype = compatmeta['GenProcessName']
            if type( compatmeta['GenProcessName'] ) == type( [] ):
                self.evttype = compatmeta['GenProcessName'][0]            
        else:
            return self._reportError( "EvtType is not in the metadata, it has to be!" )
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

        if not self.energy:
            if application.Energy:
                self.energy = Decimal( str( application.energy ) )
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
                self.evttype = application.EvtType
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
            if hasattr( application, "detectorModel" ):
                self.detector = application.detectorModel
                if not self.detector:
                    return S_ERROR( "Application does not know which model to use, so the production does not either." )
            # else:
            #    return S_ERROR("Application does not know which model to use, so the production does not either.")
        
        
        energypath = "%s-%s/" % ( self.energy, self.machineparams )  # 1000-B1s_ws
 
        
        # TODO: Make sure basename is correct. Maybe allow for setting basename prefix
        # Final name being e.g. NAME_rec.slcio, need to define NAME, maybe based on meta data (include 
        # EvtClass automatically)
        if not self.basename:
            self.basename = 's' + self.prodparameters['ILDConfigVersion']
        if 'DetectorModel'    in self.compatmeta:
            self.basename += '.m' + self.compatmeta['DetectorModel']
        elif self.detector:
            self.basename += '.m' + self.detector
        if self.energy:
            self.basename += '.E' + str( self.energy )
        if 'MachineParams' in self.compatmeta:
            self.basename += '-' + self.compatmeta['MachineParams']
            
        if 'GenProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['GenProcessID'] )
        elif 'ProcessID' in self.compatmeta:
            self.basename += '.I' + str( self.compatmeta['ProcessID'] )
        if 'EvtType' in self.compatmeta:
            self.basename += '.P' + self.compatmeta['EvtType']  # To be fixed with Jan
        elif 'GenProcessType' in self.compatmeta:
            self.basename += '.P' + self.compatmeta['GenProcessType']
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

        softwaremeta = softwarepath
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
            path = self.basepath + 'rec/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector + softwarepath
            self.finalMetaDict[self.basepath + 'rec/' + energypath + self.evtclass] = {"EvtClass" : evtclassmeta}
            self.finalMetaDict[self.basepath + 'rec/' + energypath + self.evtclass + self.evttype] = {"EvtType" : evttypemeta}
            self.finalMetaDict[self.basepath + 'rec/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/"] = {'ProcessID': self.processID}
            self.finalMetaDict[self.basepath + 'rec/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector] = {"DetectorModel" : detectormeta}
            self.finalMetaDict[self.basepath + 'rec/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector + softwarepath] = {"SoftwareTag" : softwaremeta}
            fname = self.basename + "_rec.slcio"
            application.setOutputRecFile( fname, path )    
            self.finalpaths.append( path )
            path = self.basepath + 'dst/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector + softwarepath
            self.finalMetaDict[self.basepath + 'dst/' + energypath + self.evtclass] = {"EvtClass" : evtclassmeta}
            self.finalMetaDict[self.basepath + 'dst/' + energypath + self.evtclass + self.evttype] = {"EvtType" : evttypemeta}
            self.finalMetaDict[self.basepath + 'dst/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/"] = {'ProcessID': self.processID}
            self.finalMetaDict[self.basepath + 'dst/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector] = {"DetectorModel" : detectormeta}
            self.finalMetaDict[self.basepath + 'dst/' + energypath + self.evtclass + self.evttype + str( self.processID ) + "/" + self.detector + softwarepath] = {"SoftwareTag" : softwaremeta}
            fname = self.basename + "_dst.slcio"
            application.setOutputDstFile( fname, path )    
            self.finalpaths.append( path )
        elif hasattr( application, "OutputFile" ) and hasattr( application, 'datatype' ) and ( not application.OutputFile ) and ( not application.willBeCut ):
            if ( not application.datatype ) and self.datatype:
                application.datatype = self.datatype
            if application.datatype == 'gen':
                datatype = 'generated/'
                self.basepath = "/".join( self.basepath.split( "/" )[:-2] ) + '/'  # because for generated, it's common to all ILC
            elif application.datatype == 'SIM':
                datatype = 'sim/'
            path = self.basepath + datatype
            path += energypath + self.evtclass
            self.finalMetaDict[path] = {"EvtClass" : evtclassmeta}
            path += self.evttype
            self.finalMetaDict[path] = {"EvtType" : evttypemeta}
            path += str( self.processID ) + "/"
            metap = {}
            if 'GenProcessID' in self.compatmeta:
                metap.update( {"ProcessID":self.compatmeta['GenProcessID']} )  # because we need 2 fields for the same info: file and directory metadata
                self.finalMetaDict[path] = metap     
            elif 'ProcessID' in self.compatmeta:
                metap.update( {"ProcessID":self.compatmeta['ProcessID']} )    
                self.finalMetaDict[path] = metap     
            if hasattr( application, "DetectorModel" ):
                if application.DetectorModel:
                    path += application.DetectorModel
                    self.finalMetaDict[path] = {"DetectorModel" : application.DetectorModel}
                    path += '/'
                elif self.detector:
                    path += self.detector
                    self.finalMetaDict[path] = {"DetectorModel" : self.detector}
                    path += '/'
            path += softwarepath         
            self.finalMetaDict[path] = {"SoftwareTag" : softwaremeta}
            path += '/'
            
            self.log.info( "Will store the files under", "%s" % path )
            self.finalpaths.append( path )

            extension = 'stdhep'
            if application.datatype in ['SIM', 'REC']:
                extension = 'slcio'
            fname = self.basename + "_%s" % ( application.datatype.lower() ) + "." + extension
            application.setOutputFile( fname, path )    

        self.basepath = path
        
        res = self._updateProdParameters( application )
        if not res['OK']:
            return res            
        
        self.checked = True
            
        return S_OK()
