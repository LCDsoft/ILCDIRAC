''' module for splitting utilities and everything related to it
'''


def addJobIndexToFilename( filename, jobIndex ):
  """ add the jobIndex number to the filename before the extension or replace %n with jobIndex

  :param str filename: the original name of the file
  :param int jobIndex: the jobIndex number
  :returns: new filename with jobIndex
  """
  if '%n' in filename:
    filename = filename.replace( '%n', str(jobIndex) )
    return filename

  fileParts = filename.rsplit( '.', 1 )
  if len(fileParts) == 2:
    filename = "%s_%d.%s" % ( fileParts[0], jobIndex, fileParts[1] )
    return filename

  filename = "%s_%d" % ( filename, jobIndex )
  return filename
