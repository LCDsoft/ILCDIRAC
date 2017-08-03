""" utilities to treat Marlin XML Steering files

Implementations for Overlay processors can handle OverlayTiming and Overlay processor Type
"""

from xml.etree.ElementTree import Comment

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.OverlayFiles import getOverlayFiles

DEFAULT_OVERLAY_PROCESSORS = [ 'overlaytiming', 'bgoverlay' ]

def setOverlayFilesParameter( tree, overlayParam=None ):
  """ set the parameters for overlay processors in MarlinSteering xml

  treat processors and groups of processors

  :param tree: XML tree of marlin steering file
  :param overlayParam: list of three tuples of backgroundType, eventsPerBackgroundFile, processorName
  """

  overlayActive = __checkOverlayActive( tree )
  if not overlayParam and not overlayActive:
    return S_OK()

  if not overlayParam and overlayActive:
    return S_ERROR( "Found active overlay processors, but no overlayInput was run" )

  for backgroundType, eventsPerBackgroundFile, processorName in overlayParam:
    processorsToCheck = [ processorName ] if processorName else DEFAULT_OVERLAY_PROCESSORS
    for processorType in processorsToCheck:
      resOT = __checkOverlayProcessor( tree, eventsPerBackgroundFile, processorType.lower(), backgroundType  )
      if not resOT['OK']:
        return resOT
      resGroupO = __checkOverlayGroup( tree, eventsPerBackgroundFile, processorType.lower(), backgroundType )
      if not resGroupO['OK']:
        return resGroupO

  return S_OK()

def __checkOverlayActive( tree ):
  """ checks if the overlayProcessor would actually overlay anything, or if the parameters for number of events are 0

  :returns: True or False
  :rtype: bool
  """
  overlay = False
  processors = tree.findall('execute/processor')
  for processor in processors:
    processorName = processor.attrib.get('name','').lower()
    if any( processorName.count( pattern.lower() ) for pattern in DEFAULT_OVERLAY_PROCESSORS ):
      overlay = True
  if not overlay:
    return False

  for processor in tree.findall('processor'):
    processorName =processor.attrib.get('name', '').lower()
    processorType =processor.attrib.get('type', '').lower()
    if any( processorName.count( pattern.lower() ) for pattern in DEFAULT_OVERLAY_PROCESSORS ) or \
       any( processorType.count( pattern.lower() ) for pattern in DEFAULT_OVERLAY_PROCESSORS ):
      for param in processor.findall('parameter'):
        if param.attrib.get('name') in ( 'NumberBackground', 'NBunchtrain', 'expBG' ) and \
           ( param.attrib.get('value') in ('0', '0.0') or param.text in ( '0', '0.0' ) ):
          return False

  return True


def __checkOverlayGroup( tree, eventsPerBackgroundFile, processorType, bkgType ):
  """ check if there is an OverlayProcessor, also handling overlay processors that get parameters from a group """
  groups = tree.findall('group')
  for group in groups:
    groupParameters = group.findall('parameter')
    resG = __checkOverlayProcessor( group, eventsPerBackgroundFile, processorType, bkgType, groupParameters )
    if not resG['OK']:
      return resG
  return S_OK()

def __checkOverlayProcessor( tree, eventsPerBackgroundFile, processorType, bkgType, groupParameters=None,  ):
  """ check the for the overlay processor *processorType* and set the appropriate parameter values """

  for processor in tree.findall('processor'):
    if processor.attrib.get('name', '').lower().count(processorType.lower()) or \
       processor.attrib.get('type', '').lower().count(processorType.lower()):
      files = getOverlayFiles( bkgType )
      if not files:
        return S_ERROR('Could not find any overlay files')
      if 'overlaytiming' in processor.attrib.get('type', '').lower():
        __changeProcessorTagValue( processor, 'parameter', 'BackgroundFileNames', '\n'.join(files), "Overlay files changed", groupParameters )
      if processor.attrib.get('type', '').lower() == 'overlay':
        __changeProcessorTagValue( processor, 'parameter', "InputFileNames", "\n".join(files), "Overlay files changed" )
        __changeProcessorTagValue( processor, 'parameter', "NSkipEventsRandom",
                                   "%d" % int( len(files) * eventsPerBackgroundFile ), "NSkipEventsRandom Changed" )

  return S_OK()

def setOutputFileParameter( tree, outputFile, outputREC, outputDST ):
  for processor in tree.findall('processor'):
    if 'name' not in processor.attrib:
      continue
    if outputFile:
      if processor.attrib.get('name') == 'MyLCIOOutputProcessor' \
         or processor.attrib.get('type') == 'LCIOOutputProcessor':
        __changeProcessorTagValue( processor, 'parameter', 'LCIOOutputFile', outputFile, 'output file changed')
    else:
      if outputREC:
        if processor.attrib.get('name') in( 'MyLCIOOutputProcessor', 'Output_REC' ):
          __changeProcessorTagValue( processor, 'parameter', 'LCIOOutputFile', outputREC, 'REC file changed')
      if outputDST:
        if processor.attrib.get('name') in ( 'DSTOutput', 'Output_DST' ):
          __changeProcessorTagValue( processor, 'parameter', 'LCIOOutputFile', outputDST, 'DST file changed')
  return S_OK()


def __changeProcessorTagValue( processor, tagTypename, parameterName, newValue, newComment, additionalParameters=None ):
  """ modify the value of the tag with *tagTypename* with name attribute equall to *parameterName* to *newValue*

  :param processor: XML object representing processor
  :param tagTypename: name of the tag to look for, e.g. <parameter /> to find all parameters for given processor section
  :param parameterName: name of the parameter to change <parameter name="parameterName"/>
  :param newValue: new value to set for the parameter
  :param newComment: comment to set
  """
  tags = processor.findall( tagTypename ) + ( additionalParameters if additionalParameters else [] )
  for tag in tags:
    if tag.attrib.get('name') == parameterName:
      tag.text = newValue
      com = Comment( newComment )
      processor.insert(0, com)
