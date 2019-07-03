"""
Test upload/replication/download/removal for different StorageElements
"""
from __future__ import print_function
import filecmp
import os
import random
import shutil
import string
import subprocess
import sys
import tempfile

from itertools import permutations

import pytest

from DIRAC.Core.Security import ProxyInfo
from DIRAC.Core.Base import Script

# mark all tests in this file as integration tests
pytestmark = pytest.mark.integration  # pylint: disable=invalid-name

__RCSID__ = '$Id$'

STORAGEELEMENTS = ['CERN-DIP-4', 'CERN-SRM', 'CERN-DST-EOS']
SE_PAIRS = list(permutations(STORAGEELEMENTS, 2))

SE_ARGUMENTS = [(SE, ) for SE in STORAGEELEMENTS]

SE_PAIR_ARGUMENTS = [pytest.param(site1, site2, marks=pytest.mark.timeout(100)) for site1, site2 in SE_PAIRS]


def randomFolder():
  """ create a random string of 8 characters """
  return ''.join(random.SystemRandom().choice(string.ascii_lowercase) for _ in xrange(8))


def assertOperationSuccessful(result, message):
  """Check if the DMS operation completed successfully.

  Success is indicated by the first line consisting of "'Failed: {}'"
  and the second line consisting of "'Successful': {" followed by the filename (not checked)
  """
  assert result.count("'Failed': {}") == 1 & result.count("'Successful': {'") == 1, message


def removeFileAllowFailing(options):
  """Remove the random file from the storage elements, if it exists."""
  try:
    subprocess.check_output(['dirac-dms-remove-files', options.lfntestfile] + options.options)
  except subprocess.CalledProcessError:
    sys.exc_clear()


def removeFile(options):
  """Remove the random file from the storage elements."""
  result = subprocess.check_output(['dirac-dms-remove-files', options.lfntestfile] + options.options)
  assert result.count('Successfully removed 1 files') == 1, 'Removal of random file failed: ' + result


def uploadFile(site, options):
  """Upload the local random file to the storage elements."""
  try:
    result = subprocess.check_output(['dirac-dms-add-file', '-ddd',
                                      options.lfntestfile,
                                      options.localtestfile, site] + options.options)
    assert result.count('Successfully uploaded ') == 1, 'Upload of random file failed'
  except subprocess.CalledProcessError as err:
    assert False, err.output


def replicateTo(site, options):
  """Replicate the random file to another storage element and check if it worked."""
  try:
    cmd = ['dirac-dms-replicate-lfn', options.lfntestfile, site, '-ddd'] + options.options
    result = subprocess.check_output(cmd)
    assertOperationSuccessful(result, 'Failed replicating file')
  except subprocess.CalledProcessError as err:
    print('Command failed:' % cmd)
    print(err.output)
    assert False


def removeDownloadedFile(options):
  """Remove the lfn test file if it exists locally."""
  if os.path.exists(options.lfntestfilename):
    try:
      os.unlink(options.lfntestfilename)
    except EnvironmentError as err:
      print('failed to remove lfn', repr(err))


@pytest.fixture(scope='module')
def proxySetup():
  """Ensure dirac commands can be run.

  Need to execute parseCommandLIne so we can get the proxy information.
  This fixture is run once for the module.
  """
  Script.parseCommandLine()


@pytest.fixture
def opt(proxySetup):
  """Options to be used in the tests."""
  user = ProxyInfo.getProxyInfo()['Value']['username']

  class Options(object):
    localtestfile = 'testfile'
    lfntestfilename = 'testfile_uploaded.txt'
    lfntestfilepath = '/ilc/user/'
    lfntestfilepath += '%s/%s/setests/%s/' % (user[0], user, randomFolder())
    lfntestfile = os.path.join(lfntestfilepath, lfntestfilename)
    options = ['-o', '/Resources/FileCatalogs/LcgFileCatalog/Status=InActive',
                   '-o', '/DIRAC/Setup=ILC-Test',
                   ]
    print('Using lfn %s' % lfntestfilepath)
  return Options()


@pytest.fixture
def randomFile(opt):
  """Set up the objects."""
  # Check if file exists already
  try:
    subprocess.check_output(['dirac-dms-remove-files', opt.lfntestfile] + opt.options)
    print('WARN Warning: file already existed on SE:', opt.lfntestfile)
  except subprocess.CalledProcessError:
    sys.exc_clear()

  # Make temporary dir to run test in
  opt.curdir = os.getcwd()
  opt.tmpdir = tempfile.mkdtemp('', dir='./')
  os.chdir(opt.tmpdir)

  # Create testfile with random bits
  with open(opt.localtestfile, 'wb') as fout:
    fout.write(' My random testfile ')
    fout.write(os.urandom(1024 * 1024))
  yield
  # tear down
  removeFileAllowFailing(opt)
  os.chdir(opt.curdir)
  shutil.rmtree(opt.tmpdir)


@pytest.mark.parametrize(('site',), SE_ARGUMENTS)
def test_storing(randomFile, opt, site):
  """Upload the file to a given SE, then retrieve it and check for equality."""
  uploadFile(site, opt)
  # get file from SE, check for equivalence
  result = subprocess.check_output(['dirac-dms-get-file', '-ddd', opt.lfntestfile] + opt.options)
  assertOperationSuccessful(result, 'Retrieval of random file from storage element to local failed: ' + result)
  assert filecmp.cmp(opt.localtestfile, opt.lfntestfilename), 'Stored wrong file'
  removeFile(opt)


@pytest.mark.parametrize(('site1', 'site2'), SE_PAIR_ARGUMENTS)
def test_replication(randomFile, opt, site1, site2):
  """Replicate file to other SE, check if it is replicated there."""
  uploadFile(site1, opt)
  # Replicate file to SE2, remove replica from SE1, get file, rm from all
  replicateTo(site2, opt)

  result = subprocess.check_output(['dirac-dms-remove-replicas', opt.lfntestfile, site1] + opt.options)
  assert result.count('Successfully removed') == 1, 'Failed removing replica of random file: ' + result

  result = subprocess.check_output(['dirac-dms-get-file', opt.lfntestfile] + opt.options)
  assertOperationSuccessful(result, 'Retrieval of random file from storage element to local failed: ' + result)

  assert filecmp.cmp(opt.localtestfile, opt.lfntestfilename), 'Received wrong file'
  removeDownloadedFile(opt)


@pytest.mark.parametrize(('site1', 'site2'), SE_PAIR_ARGUMENTS)
def test_removal(randomFile, opt, site1, site2):
  """Upload file to SE1, replicate to SE2, remove file and ensure retrieve fails."""
  uploadFile(site1, opt)
  replicateTo(site2, opt)

  result = subprocess.check_output(['dirac-dms-remove-files', opt.lfntestfile] + opt.options)
  assert result.count('Successfully removed 1 files') == 1, 'Removal of random file failed: ' + result

  try:
    result = subprocess.check_output(['dirac-dms-get-file', opt.lfntestfile] + opt.options)
    assert False, 'Get file should not succeed'
  except subprocess.CalledProcessError as err:
    assert err.output.count('ERROR') >= 1, 'File not removed from SE even though it should be: ' + err.output
