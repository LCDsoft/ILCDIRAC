"""CalibrationSystem Utilities to deal with binary files."""


def binaryFileToString(filename):
  """Return binary string from filename.

  :param str filename: filename
  :returns: string of file content
  """
  with open(filename, 'rb') as source:
    fileContent = source.read()
    return fileContent


def stringToBinaryFile(contentString, filename):
  """Write contentString to binaryFile."""
  with open(filename, 'wb') as target:
    target.write(contentString)
