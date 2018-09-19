"""Test the LFNPathUtilities."""
__RCSID__ = "$Id$"

import pytest

from ILCDIRAC.Core.Utilities.LFNPathUtilities import joinPathForMetaData, cleanUpLFNPath

LOG_PATH = "/ilc/prod/ilc/mc-dbd/ild/"
JOB_ID = 12


@pytest.mark.parametrize('paths, expectedPath',
                         [(("/ilc", "grid", "softwareVersion", "/"), "/ilc/grid/softwareVersion/"),
                          (("/ilc//grid", "/", "softwareVersion", "/"), "/ilc/grid/softwareVersion/"),
                          (("/ilc//grid", "/", "softwareVersion/", "/"), "/ilc/grid/softwareVersion/"),
                          ])
def test_joinPathForMetaData(paths, expectedPath):
  """Test for joinPathForMetaData."""
  assert joinPathForMetaData(*paths) == expectedPath


@pytest.mark.parametrize('lfn, expectedPath',
                         [('%s/%s' % (LOG_PATH, str(int(JOB_ID) / 1000).zfill(3)), '/ilc/prod/ilc/mc-dbd/ild/000'),
                          ('LFN:/some/path/to/some/where', '/some/path/to/some/where'),
                          ('lFn:/some/path/to/some/where', '/some/path/to/some/where'),
                          ])
def test_lfnCleanup(lfn, expectedPath):
  """Test for cleanUpLFNPath."""
  assert cleanUpLFNPath(lfn) == expectedPath
