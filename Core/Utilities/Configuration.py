""" helper functions for getting configuration options """

import os


def getOptionValue(ops, basePath, optionName, defaultValue, levels):
  """ get option from any place in the hierarchy starting from basepath, going through each section level

  :param ops: Operation helper
  :param str basePath: section in Operation to start looking for the option
  :param str optionName: the name of the option to find
  :param defaultValue: the default value to use for this option
  :param list levels: the different [sub-]sub-sections to check for this option
  :returns: value at the deepest level in the configuration
  """

  join = os.path.join
  value = ops.getValue(join(basePath, optionName), defaultValue)

  path = basePath
  for level in levels:
    path = join(path, level)
    value = ops.getValue(join(path, optionName), value)

  return value
