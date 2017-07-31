"""
tests for PrepareOptionFiles

"""

import os
import shutil
import tempfile
import unittest
from xml.etree.ElementTree import ElementTree
from mock import patch, MagicMock as Mock

from ILCDIRAC.Tests.Utilities.GeneralUtils import assertDiracFailsWith

__RCSID__ = "$Id$"

MODULE_NAME = 'ILCDIRAC.Core.Utilities.PrepareOptionFiles'
def cleanup(tempdir):
  """
  Remove files after run
  """
  try:
    shutil.rmtree(tempdir)
  except OSError:
    pass


class TestPrepareMarlinXMLFileBase( unittest.TestCase ):
  """ tests for the perpareXMLFile function used to modify marlin steering files """

  def setUp( self ):
    self.basedir = os.getcwd()
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)
    self.inputXMLFile = "marlininput.xml"
    self.testedTree = None
    self.expectedTree = None
    self.expectedOutput = ""

  def tearDown(self):
    """ Remove the fake files
    """
    os.chdir( self.basedir )
    cleanup ( self.tmpdir )


  def getExpectedXMLTree( self ):
    return self.getTree( self.expectedOutput )

  @staticmethod
  def getTree( xmlFile ):
    et = ElementTree()
    et.parse( xmlFile )
    return et

  @staticmethod
  def findAttributeByName( elements, processorName ):
    """ check if processor name is executed in tree """
    for elem in elements:
      if processorName == elem.attrib.get('name'):
        return True
    return False

  def findProcessorInTree( self, processorName):
    """check if the testTree and the expectedTree containt the same processor
    with optional parameter and optional value comparison

    """
    expected = TestPrepareMarlinXMLFile.findAttributeByName(
      self.expectedTree.findall( 'execute/processor' ), processorName )
    tested = TestPrepareMarlinXMLFile.findAttributeByName(
      self.testedTree.findall( 'execute/processor' ), processorName )

    if expected:
      print "found",processorName, "in expectedProcessors"
    if tested:
      print "found",processorName, "in testedProcessors"

    return expected == tested

  def checkProcessorParameter( self, processorName, processorParameter,
                               parameterValue):
    """check that the processorParameter has the correct value """
    processors = self.testedTree.findall('processor')
    for proc in processors:
      if proc.attrib.get('name') == processorName:
        parameters = proc.findall('parameter')
        for param in parameters:
          if param.attrib.get('name') == processorParameter:
            if param.text is not None and param.text.strip() == str( parameterValue ):
              return True
            elif param.attrib.get('value') == str(parameterValue):
              return True
            elif not parameterValue and not (param.attrib.get('value') or param.text):
              return True
            else:
              print "Parameter",processorParameter,"value is", \
                repr(param.attrib.get('value')), \
                "or", repr(param.text)

    groups = self.testedTree.findall('group')
    for group in groups:
      groupParameters = group.findall('parameter')
      groupProcessors = group.findall('processor')
      for proc in groupProcessors:
        if proc.attrib.get('name') == processorName:
          parameters = proc.findall('parameter') + groupParameters
          for param in parameters:
            if param.attrib.get('name') == processorParameter:
              if param.text is not None and param.text.strip() == str( parameterValue ):
                return True
              elif param.attrib.get('value') == str(parameterValue):
                return True
              elif not parameterValue and not (param.attrib.get('value') or param.text):
                return True
              else:
                print "Parameter",processorParameter,"value is", \
                  repr(param.attrib.get('value')), \
                  "or", repr(param.text)



    return False


  def checkGlobalTag( self, parameterName, parameterValue ):
    """ check if global parameter has correct value in testedTree """
    parameterValue = str(parameterValue)
    params = self.testedTree.findall('global/parameter')
    print "checking tag",parameterName
    for param in params:
      if param.get('name') == parameterName:
        if param.get('value') == parameterValue or param.text == parameterValue:
          print "found parameter", parameterName, "with",parameterValue
          return True,None

        message = "The parameter "+repr(parameterName)+" does not have the value " +repr(parameterValue)+" but "+ \
          repr(param.get('value'))+" or"+repr(param.text)
        return False, message
    return False, "parameter " + parameterName + " not found "

class TestPrepareMarlinXMLFile( TestPrepareMarlinXMLFileBase ):
  """ tests for the perpareXMLFile function used to modify marlin steering files """

  def setUp( self ):
    self.basedir = os.getcwd()
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)
    self.inputXMLFile = "marlininput.xml"
    self.expectedOutput = os.path.join( os.getenv("DIRAC"),
                                        "ILCDIRAC/Testfiles/marlinExpectedOutput.xml" )
    for xmlFile in ("marlininput.xml",
                    "marlinInputSmall.xml",
                    "marlinRecoProd.xml",
                    "marlininputild.xml",
                    "marlininputildNoOver.xml"):
      shutil.copyfile( os.path.join( os.getenv( "DIRAC" ), "ILCDIRAC/Testfiles/",
                                     xmlFile ), xmlFile )
    #self.createInputXMLFile()
    self.expectedTree = self.getExpectedXMLTree()
    self.testedTree = None

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputfile.xml",
                          inputXML="marlininput.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= "mySLCIOOutput.slcio",
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=False,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ('gghad', 0, None) ],
                        )
    self.assertTrue( res['OK'] )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputfile.xml" )
    self.assertTrue( self.findProcessorInTree( "InitDD4hep" ), "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "SILENT" ) )
    self.assertTrue( self.checkProcessorParameter(
      "InitDD4hep", "DD4hepXMLFile", "/cvmfs/monty.python.fr/myDetector.xml") )
    self.assertTrue( self.checkProcessorParameter( "MyOverlayTiming",
                                                   "BackgroundFileNames",
                                                   "file1\nfile2") )
    ## Make sure these checks are not always true
    self.assertFalse( *self.checkGlobalTag( "Verbosity", "NotSILENT" ) )
    self.assertFalse( self.checkProcessorParameter( "MyOverlayTiming",
                                                    "BackgroundFileNames",
                                                    "NotTheseFiles") )

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_initialParametersMissing( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputfilesmall.xml",
                          inputXML="marlinInputSmall.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= "mySLCIOOutput.slcio",
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputfilesmall.xml" )
    self.assertTrue( self.findProcessorInTree( "InitDD4hep" ),
                     "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "WARNING" ) )
    self.assertTrue( self.checkProcessorParameter(
      "InitDD4hep", "DD4hepXMLFile", "/cvmfs/monty.python.fr/myDetector.xml") )
    self.assertTrue( self.checkProcessorParameter( "MyOverlayTiming",
                                                   "NBunchtrain", "0" ) )
    self.assertTrue( self.checkProcessorParameter( "MyOverlayTiming",
                                                   "NumberBackground", "0.0" ) )
    self.assertTrue( self.checkProcessorParameter( "MyOverlayTiming",
                                                   "BackgroundFileNames", "" ) )
    self.assertTrue( self.checkProcessorParameter(
      "MyLCIOOutputProcessor", "LCIOOutputFile", "mySLCIOOutput.slcio" ) )


  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_productionOutputFiles( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="marlinRecoProd.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ('gghad', 0, None) ],
                        )
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputprod.xml" )
    self.assertTrue( self.findProcessorInTree( "InitDD4hep" ),
                     "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "WARNING" ) )
    self.assertTrue( self.checkProcessorParameter(
      "InitDD4hep", "DD4hepXMLFile", "/cvmfs/monty.python.fr/myDetector.xml") )
    self.assertTrue( self.checkProcessorParameter(
      "MyOverlayTiming", "BackgroundFileNames", "file1\nfile2") )
    self.assertTrue( self.checkProcessorParameter(
      "MyLCIOOutputProcessor", "LCIOOutputFile", "outputrec.slcio") )
    self.assertTrue( self.checkProcessorParameter(
      "DSTOutput", "LCIOOutputFile", "outputdst.slcio") )

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_ildRecoFile( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprodild.xml",
                          inputXML="marlininputild.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ('aa_lowpt', 333, None) ],
                        )
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputprodild.xml" )
    #self.assertTrue( self.findProcessorInTree( "BGoverLay" ), "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "WARNING" ) )
    self.assertTrue( self.checkProcessorParameter( "BgOverlay", "InputFileNames",
                                                   "file1\nfile2") )
    self.assertTrue( self.checkProcessorParameter( "BgOverlay", "NSkipEventsRandom",
                                                   "666") )

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_ildRecoFile_NoOver( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprodildNoOver.xml",
                          inputXML="marlininputildNoOver.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputprodildNoOver.xml" )
    #self.assertTrue( self.findProcessorInTree( "BGoverLay" ), "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "WARNING" ) )
    self.assertTrue( self.checkProcessorParameter( "BgOverlay", "InputFileNames", "") )
    self.assertTrue( self.checkProcessorParameter( "BgOverlay", "expBG", "0.0") )


  def test_createFile_NoOutputSet( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="marlinNoOutput.xml",
                          inputXML="marlinInputSmall.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC='',
                          outputDST='',
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "marlinNoOutput.xml" )
    #self.assertTrue( self.findProcessorInTree( "BGoverLay" ), "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "WARNING" ) )
    self.assertTrue( self.checkProcessorParameter( "MyLCIOOutputProcessor",
                                                   "LCIOOutputFile", "") )


  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=[] ) )
  def test_createFile_overlayTiming_NoFiles( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="marlinRecoProd.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ('gghad', 0, None) ],
                        )
    self.assertFalse( res['OK'] )
    self.assertIn( "Could not find any overlay files", res['Message'] )

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=[] ) )
  def test_createFile_bgoverlay_NoFiles( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="marlininputild.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ('aa_lowpt', 0, None) ],
                        )
    self.assertFalse( res['OK'] )
    self.assertIn( "Could not find any overlay files", res['Message'] )

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=[] ) )
  def test_createFile_bgoverlay_failOverlayActive( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="marlininputild.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[],
                        )
    assertDiracFailsWith( res, 'Found active overlay processors', self )


  def test_createFile_parseError( self ):

    def parseModified( *_args, **_kwargs ):
      """ throw exception for parse """
      raise RuntimeError("Parse Failed")

    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    with patch("%s.ElementTree.parse" % MODULE_NAME, new=parseModified):
      res = prepareXMLFile( finalxml="outputprod.xml",
                            inputXML="marlininputild.xml",
                            inputGEAR="gearMyFile.xml",
                            inputSLCIO="mySLCIOInput.slcio",
                            numberofevts=501,
                            outputFile= '',
                            outputREC="outputrec.slcio",
                            outputDST="outputdst.slcio",
                            debug=True,
                            dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertFalse( res['OK'] )
    self.assertIn( "Found Exception when parsing", res['Message'] )

  def test_createFile_inputFaulty( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="marlininputild.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO=None,
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=True,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertFalse( res['OK'], res['Message'] )
    self.assertIn( "inputSLCIO is neither", res['Message'] )


  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_inputList( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputfile.xml",
                          inputXML="marlininput.xml",
                          inputGEAR="gearMyFile.xml",
                          inputSLCIO=["file1","file2"],
                          numberofevts=501,
                          outputFile= "mySLCIOOutput.slcio",
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=False,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam = [ ('gghad', 0, None ) ] )
    self.assertTrue( res['OK'], res.get('Message', '') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputfile.xml" )
    self.assertTrue( self.findProcessorInTree( "InitDD4hep" ),
                     "Problem with InitDD4hep" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "file1 file2" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "GearXMLFile", "gearMyFile.xml" ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "SILENT" ) )



class TestPrepareClicProdXMLFile( TestPrepareMarlinXMLFileBase ):
  """ tests for the perpareXMLFile function used to modify marlin steering files """

  def setUp( self ):
    self.basedir = os.getcwd()
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)
    for xmlFile in ("clicReconstruction.xml",
                   ):
      shutil.copyfile( os.path.join( os.getenv( "DIRAC" ), "ILCDIRAC/Testfiles/",
                                     xmlFile ), xmlFile )
    self.testedTree = None

  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile_clicProd2017( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="clicReconstruction.xml",
                          inputGEAR="",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=False,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ( 'gghad', 0, 'Overlay380GeV' ) ],
                        )
    self.assertTrue( res['OK'], res.get('Message') )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputprod.xml" )
    self.assertTrue( *self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( *self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( *self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( *self.checkGlobalTag( "Verbosity", "SILENT" ) )
    self.assertTrue( self.checkProcessorParameter(
      "InitDD4hep", "DD4hepXMLFile", "/cvmfs/monty.python.fr/myDetector.xml") )
    self.assertTrue( self.checkProcessorParameter(
      "Overlay380GeV", "BackgroundFileNames", "file1\nfile2") )
    self.assertTrue( self.checkProcessorParameter(
      "Output_REC", "LCIOOutputFile", "outputrec.slcio") )
    self.assertTrue( self.checkProcessorParameter(
      "Output_DST", "LCIOOutputFile", "outputdst.slcio") )



  @patch("ILCDIRAC.Core.Utilities.MarlinXML.getOverlayFiles", new=Mock(return_value=[] ) )
  def test_createFile_clicProd2017_noOverlayFiles( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputprod.xml",
                          inputXML="clicReconstruction.xml",
                          inputGEAR="",
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= '',
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=False,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml",
                          overlayParam=[ ( 'gghad', 0, 'Overlay380GeV' ) ],
                        )
    assertDiracFailsWith( res, 'Could not find any overlay Files', self )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestPrepareClicProdXMLFile )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
