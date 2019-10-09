"""Test the options for the agents."""
import logging
import pytest


from DIRAC.tests.Utilities.assertingUtils import AgentOptionsTest

AGENTS = [('ILCDIRAC.OverlaySystem.Agent.ResetCounters', {}),
          ('ILCDIRAC.FrameworkSystem.Agent.MonitorAgents', {}),
          ('ILCDIRAC.WorkloadManagementSystem.Agent.JobResetAgent', {}),
          ('ILCDIRAC.ILCTransformationSystem.Agent.FileStatusTransformationAgent', {}),
          ('ILCDIRAC.ILCTransformationSystem.Agent.DataRecoveryAgent', {}),
          ('ILCDIRAC.CalibrationSystem.Agent.CalibrationAgent', {}),
          ]


@pytest.mark.parametrize('agentPath, options', AGENTS)
def test_AgentOptions(caplog, agentPath, options, mocker):
  """Check that all options in ConfigTemplate are found in the initialize method, including default values."""
  caplog.set_level(logging.DEBUG)
  AgentOptionsTest(agentPath, options, mocker)
