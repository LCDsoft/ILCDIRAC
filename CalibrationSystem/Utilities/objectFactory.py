"""Object factory."""

from DIRAC import gLogger

LOG = gLogger.getSubLogger(__name__)


class ObjectFactory:
  """Standard object factiry."""

  def __init__(self):
    """Initialize."""
    self._builders = {}

  def registerBuilder(self, key, builder):
    """Register builder."""
    self._builders[key] = builder

  def getClass(self, key):
    """Return class (builder) which corresponds to the input key."""
    builder = self._builders.get(key)
    if not builder:
      LOG.error('Unknown key: %s. Available keys are: %s' % (key, self._builders.keys()))
      raise ValueError(key)
    return builder
