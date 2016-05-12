"""
tests for PrepareOptionFiles

"""

import filecmp
import os
import shutil
import tempfile
import unittest

from xml.etree.ElementTree import ElementTree

from mock import mock_open, patch, MagicMock as Mock

from DIRAC import S_OK, S_ERROR

__RCSID__ = "$Id$"

def cleanup(tempdir):
  """
  Remove files after run
  """
  try:
    shutil.rmtree(tempdir)
  except OSError:
    pass


class TestPrepareMarlinXMLFile( unittest.TestCase ):
  """ tests for the perpareXMLFile function used to modify marlin steering files """

  def setUp( self ):
    self.basedir = os.getcwd()
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)
    self.inputXMLFile = "marlininput.xml"
    self.expectedOutput = os.path.join( os.getenv("DIRAC"), "ILCDIRAC/Testfiles/marlinExpectedOutput.xml" )
    shutil.copyfile( os.path.join( os.getenv("DIRAC"), "ILCDIRAC/Testfiles/marlininput.xml"), self.inputXMLFile )
    #self.createInputXMLFile()
    self.expectedTree = self.getExpectedXMLTree()
    self.testedTree = None

  def tearDown(self):
    """ Remove the fake files
    """
    os.chdir( self.basedir )
    #cleanup ( self.tmpdir )

  def getExpectedXMLTree( self ):
    return TestPrepareMarlinXMLFile.getTree( self.expectedOutput )

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
    expected = TestPrepareMarlinXMLFile.findAttributeByName( self.expectedTree.findall('execute/processor'), processorName )
    tested = TestPrepareMarlinXMLFile.findAttributeByName( self.testedTree.findall('execute/processor'), processorName )

    if expected:
      print "found",processorName, "in expectedProcessors"
    if tested:
      print "found",processorName, "in testedProcessors"

    return expected == tested

  def checkProcessorParameter( self, processorName, processorParameter, parameterValue):
    """check that the processorParameter has the correct value """
    processors = self.testedTree.findall('processor')
    for proc in processors:
      if proc.attrib.get('name') == processorName:
        parameters = proc.findall('parameter')
        for param in parameters:
          if param.attrib.get('name') == processorParameter:
            if param.text is not None and param.text.strip() == str(parameterValue):
              return True
            elif param.attrib.get('value') == str(parameterValue):
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
    for param in params:
      if param.get('name') == parameterName:
        if param.get('value') == parameterValue or param.text == parameterValue:
          return True
        else:
          print "The parameter",parameterName,"does not have the value",parameterValue,"but", \
            repr(param.get('value')),"or",repr(param.text)
          return False
    return False

  @patch("ILCDIRAC.Core.Utilities.PrepareOptionFiles.getOverlayFiles", new=Mock(return_value=["file1","file2"] ) )
  def test_createFile( self ):
    from ILCDIRAC.Core.Utilities.PrepareOptionFiles import prepareXMLFile
    res = prepareXMLFile( finalxml="outputfile.xml",
                          inputXML=self.inputXMLFile,
                          inputGEAR=None,
                          inputSLCIO="mySLCIOInput.slcio",
                          numberofevts=501,
                          outputFile= "mySLCIOOutput.slcio",
                          outputREC="outputrec.slcio",
                          outputDST="outputdst.slcio",
                          debug=False,
                          dd4hepGeoFile="/cvmfs/monty.python.fr/myDetector.xml")
    self.assertTrue( res['OK'] )
    self.testedTree = TestPrepareMarlinXMLFile.getTree( "outputfile.xml" )
    self.assertTrue( self.findProcessorInTree( "InitDD4hep" ), "Problem with InitDD4hep" )
    self.assertTrue( self.checkGlobalTag( "LCIOInputFiles", "mySLCIOInput.slcio" ) )
    self.assertTrue( self.checkGlobalTag( "MaxRecordNumber", 501 ) )
    self.assertTrue( self.checkGlobalTag( "SkipNEvents", 0 ) )
    self.assertTrue( self.checkGlobalTag( "Verbosity", "SILENT" ) )
    self.assertTrue( self.checkProcessorParameter( "InitDD4hep", "DD4hepXMLFile", "/cvmfs/monty.python.fr/myDetector.xml") )
    self.assertTrue( self.checkProcessorParameter( "MyOverlayTiming", "BackgroundFileNames", "file1\nfile2") )

if __name__ == "__main__":
  SUITE = unittest.defaultTestLoader.loadTestsFromTestCase( TestPrepareMarlinXMLFile )
  TESTRESULT = unittest.TextTestRunner( verbosity = 2 ).run( SUITE )
  print TESTRESULT
