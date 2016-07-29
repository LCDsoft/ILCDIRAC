""" Helper Functions for Job Interfaces """

def getValue( value, conversion=None, typeToCheck=None ):
  """ returns the first entry, if it is a list, or the value otherwise

  :param value: value to check
  :param callable conversion: type to convert the value to, callable
  :param class typeToCheck: class the parameter should be an instance of
  """
  newValue = None
  if isinstance( value, list ):
    newValue = value[0]
  elif typeToCheck is None or isinstance( value, typeToCheck ):
    newValue = value

  if conversion is not None and newValue is not None:
    newValue = conversion( newValue )

  return newValue
