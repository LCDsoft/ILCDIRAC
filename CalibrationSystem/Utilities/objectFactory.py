""" object factory """

from DIRAC import S_OK, S_ERROR, gLogger

LOG = gLogger.getSubLogger(__name__)


class ObjectFactory:
  def __init__(self):
    self._builders = {}

  def register_builder(self, key, builder):
    self._builders[key] = builder

  def create(self, key):
    builder = self._builders.get(key)
    if not builder:
      LOG.error('Unknown key: %s. Available keys are: %s' % (key, self._builders.keys()))
      raise ValueError(key)
    return builder
