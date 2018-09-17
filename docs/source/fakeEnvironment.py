"""Fake environment imports for documentation.

This module allows to create the documentation without having to do
any kind of special installation.
"""

# pylint: disable=invalid-name

import sys
import mock

import DIRAC.Core.Base.Script

# needed by something
sys.modules['DIRAC.AccountingSystem.Client.DataStoreClient'] = mock.Mock()
sys.modules['DIRAC.AccountingSystem.Client.DataStoreClient.gDataStoreClient'] = mock.Mock()

# needed for SoftwareVersions
sys.modules['DIRAC.WorkloadManagementSystem.Client.JobState.CachedJobState'] = mock.Mock()

scriptMock = mock.MagicMock(name="ScriptMock")
scriptMock.parseCommandLine = mock.MagicMock()
DIRAC.Core.Base.Script = scriptMock
sys.modules['DIRAC.Core.Base.Script'] = scriptMock
sys.modules['Script'] = scriptMock
