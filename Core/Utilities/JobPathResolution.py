""" The job path resolution module is a VO-specific plugin that
    allows to define VO job policy in a simple way.  This allows the
    inclusion of ILC specific WMS optimizers without compromising the
    generic nature of DIRAC.

    The arguments dictionary from the JobPathAgent includes the ClassAd
    job description and therefore decisions are made based on the existence
    of JDL parameters.
    :author: Stuart Paterson
"""

__RCSID__ = "$Id$"

from DIRAC                                                 import S_OK, S_ERROR, gLogger

COMPONENT_NAME = 'ILCJobPathResolution'

class JobPathResolution(object):
  """VO Specific plugin for jobpath resolution"""
  #############################################################################
  def __init__(self, argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)

  #############################################################################
  def execute(self):
    """Given the arguments from the JobPathAgent, this function resolves job optimizer
       paths according to ILC VO policy.
    """
    ilcPath = ''

    if 'ConfigPath' not in self.arguments:
      self.log.warn('No CS ConfigPath defined')
      return S_ERROR('JobPathResoulution Failure')

    self.log.verbose('Attempting to resolve job path for ILC')
    job = self.arguments['JobID']
    classadJob = self.arguments['ClassAd']

    inputData = classadJob.get_expression('InputData').replace('"', '').replace('Unknown', '')
    if inputData:
      self.log.info('Job %s has input data requirement' % (job))
      ilcPath += 'InputData'


    if not ilcPath:
      self.log.info('No ILC specific optimizers to be added')

    return S_OK(ilcPath)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
