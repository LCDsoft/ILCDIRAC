"""
Test upload/replication/download/removal for different StorageElements
"""
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import random
import string

from itertools import permutations

from DIRAC.Core.Security import ProxyInfo
from DIRAC.Core.Base import Script
import pytest

__RCSID__ = "$Id$"

STORAGEELEMENTS = ["CERN-DIP-4", "CERN-SRM", "CERN-DST-EOS"]
SE_PAIRS = list(permutations(STORAGEELEMENTS, 2))

def randomFolder():
  """ create a random string of 8 characters """
  return ''.join(random.SystemRandom().choice(string.ascii_lowercase) for _ in xrange(8))

class MetaCreator(type):
  """ meta class to create all tests for all combination of SEs """
  # last argument has to be dict
  def __new__(mcs, name, bases, functionDict): #pylint: disable=redefined-builtin

    def gen_storing(site):
      """ create storing test for given site """
      def test(self): #pylint: disable=missing-docstring
        self.storing_test(site)
      return test

    def gen_removal(sitePairs):
      """ create removal test for given sites """
      def test(self): #pylint: disable=missing-docstring
        self.removal_test(*sitePairs)
      return test

    def gen_replication(sitePairs):
      """ create replication test for given sites """
      def test(self): #pylint: disable=missing-docstring
        self.replication_test(*sitePairs)
      return test

    ##all storing tests
    for site in STORAGEELEMENTS:
      testName = "test_storing_%s" % site
      testName = testName.ljust(70, '.')
      functionDict[testName] = gen_storing(site)

    ##all removal tests
    for sitePairs in SE_PAIRS:
      testName = "test_removal_%s_%s" % (sitePairs[0], sitePairs[1])
      testName = testName.ljust(70, '.')
      functionDict[testName] = gen_removal(sitePairs)

    ##all replication tests
    for sitePairs in SE_PAIRS:
      testName = "test_replication_%s_%s" % (sitePairs[0], sitePairs[1])
      testName = testName.ljust(70, '.')
      functionDict[testName] = gen_replication(sitePairs)

    return type.__new__(mcs, name, bases, functionDict)



@pytest.mark.integration
class SETestCase( unittest.TestCase ):
  """ Base class for the test cases of the storage elements.
  requires dirac proxy
  """

  __metaclass__ = MetaCreator

  localtestfile = 'testfile'
  lfntestfilename = "testfile_uploaded.txt"
  lfntestfilepath = "/ilc/user/"
  lfntestfile = ''
  options = [ '-o', "/Resources/FileCatalogs/LcgFileCatalog/Status=InActive",
              '-o', "/DIRAC/Setup=ILC-Test",
            ]

  @classmethod
  def setUpClass( cls ):
    # destroy kerberos token
    #try:
    #  subprocess.call(['kdestroy'])
    #except subprocess.CalledProcessError as err:
    #  print "WARNING: kdestroy did not succeed."
    #  print err.output
    # Constants for the tests
    Script.parseCommandLine()
    user = ProxyInfo.getProxyInfo()['Value']['username']
    SETestCase.lfntestfilepath += '%s/%s/setests/%s/' % (user[0], user, randomFolder())
    print "Using lfn %s" % SETestCase.lfntestfilepath
    SETestCase.lfntestfile = SETestCase.lfntestfilepath + SETestCase.lfntestfilename

  def setUp( self ):
    """set up the objects"""
    # Check if file exists already
    try:
      subprocess.check_output(["dirac-dms-remove-files", self.lfntestfile]+self.options)
      print "WARN Warning: file already existed on SE:", self.lfntestfile
    except subprocess.CalledProcessError:
      sys.exc_clear()

    # Make temporary dir to run test in
    self.curdir = os.getcwd()
    self.tmpdir = tempfile.mkdtemp("", dir = "./")
    os.chdir(self.tmpdir)

    # Create testfile with random bits
    with open(self.localtestfile, 'wb') as fout:
      fout.write(" My random testfile ")
      fout.write(os.urandom(1024*1024))

  def tearDown ( self ):
    self.removeFileAllowFailing()
    os.chdir(self.curdir)
    shutil.rmtree( self.tmpdir )

#For python < 2.7 (executes cmd and grabs output)
#proc = subprocess.Popen(['python', 'printbob.py',  'arg1 arg2 arg3 arg4'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#print proc.communicate()[0]

  def storing_test( self, site ):
    """Uploads the file to a given SE, then retrieves it and checks for equality
    """
    self.uploadFile(site)
    # get file from SE, check for equivalence
    result = subprocess.check_output(["dirac-dms-get-file", "-ddd", self.lfntestfile]+self.options)
    self.assertOperationSuccessful(result,
                                   "Retrieval of random file from storage element to local failed: " + result)

    self.assertTrue(filecmp.cmp(self.localtestfile, self.lfntestfilename), "Received wrong file")
    self.removeFile()

  def replication_test( self, site1, site2 ):
    """Replicates file to other SE, checks if it is replicated there.
    """
    self.uploadFile(site1)
    # Replicate file to SE2, remove replica from SE1, get file, rm from all
    self.replicateTo(site2)

    result = subprocess.check_output(["dirac-dms-remove-replicas", self.lfntestfile, site1]+self.options)
    self.assertTrue(result.count("Successfully removed") == 1,
                    "Failed removing replica of random file: " + result)

    result = subprocess.check_output(["dirac-dms-get-file", self.lfntestfile]+self.options)
    self.assertOperationSuccessful(result,
                                   "Retrieval of random file from storage element to local failed: " + result)

    self.assertTrue(filecmp.cmp(self.localtestfile, self.lfntestfilename),
                    "Received wrong file")
    self.removeDownloadedFile()

  def removal_test( self, site1, site2 ):
    """Uploads file to SE1, replicates to SE2, removes file and checks if retrieve fails
    """
    self.uploadFile(site1)
    self.replicateTo(site2)

    result = subprocess.check_output(["dirac-dms-remove-files", self.lfntestfile]+self.options)
    self.assertTrue(result.count("Successfully removed 1 files") == 1,
                    "Removal of random file failed: " + result)

    try:
      result = subprocess.check_output(["dirac-dms-get-file", self.lfntestfile]+self.options)
      self.fail("Get file should not succeed")
    except subprocess.CalledProcessError as err:
      self.assertTrue(err.output.count("ERROR") >= 1,
                      "File not removed from SE even though it should be: " + err.output)

  def uploadFile( self, site ):
    """Adds the local random file to the storage elements
    """
    try:
      result = subprocess.check_output(["dirac-dms-add-file", "-ddd",
                                        self.lfntestfile,
                                        self.localtestfile, site]+self.options)
      self.assertTrue(result.count("Successfully uploaded ") == 1,
                      "Upload of random file failed")
    except subprocess.CalledProcessError as err:
      self.fail( err.output )

  def removeFile ( self ):
    """Removes the random file from the storage elements
    """
    result = subprocess.check_output( ["dirac-dms-remove-files",
                                       self.lfntestfile]+self.options)
    self.assertTrue(result.count("Successfully removed 1 files") == 1,
                    "Removal of random file failed: " + result)

  def assertOperationSuccessful( self, result, message ):
    """Checks if the DMS operation completed successfully. This is indicated by the first line consisting of "'Failed: {}'"
    and the second line consisting of "'Successful': {" followed by the filename (not checked)
    """
    self.assertTrue(result.count("'Failed': {}") == 1 &
                    result.count("'Successful': {'") == 1, message)

  def replicateTo( self, site ):
    """Replicates the random file to another storage element and checks if it worked
    """
    try:
      cmd = ["dirac-dms-replicate-lfn",
             self.lfntestfile, site, "-ddd"]+self.options
      result = subprocess.check_output( cmd )
      self.assertOperationSuccessful(result, "Failed replicating file")
    except subprocess.CalledProcessError as err:
      print err.output
      raise RuntimeError( "Command %s failed " % cmd )

  def removeFileAllowFailing ( self ):
    """Removes the random file from the storage elements, if it exists.
    If it doesn't exist, nothing happens
    """
    try:
      subprocess.check_output(["dirac-dms-remove-files", self.lfntestfile]+self.options)
    except subprocess.CalledProcessError:
      sys.exc_clear()

  def removeDownloadedFile( self ):
    """ remove the lfn test file if it exists locally """
    if os.path.exists( self.lfntestfilename ):
      try:
        os.unlink ( self.lfntestfilename )
      except EnvironmentError as err:
        print "failed to remove lfn", repr(err)
