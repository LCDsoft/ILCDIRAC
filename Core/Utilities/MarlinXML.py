""" utilities to treat Marlin XML Steering files """

from xml.etree.ElementTree import Comment

from DIRAC import S_OK, S_ERROR

from ILCDIRAC.Core.Utilities.GetOverlayFiles import getOverlayFiles

def setOverlayFilesParameter( tree, overlay=False, eventsPerBackgroundFile=0 ):
  """ set the parameters for overlay processors in MarlinSteering xml

  treat processors and groups of processors
  Set the "BackgroundFileNames" parameter in all cases.
  Select bkgType based on FIXME

  :param tree: XML tree of marlin steering file
  """
  for processor in tree.findall('processor'):
    # OverlayTiming  processor treatment
    if processor.attrib.get('name', '').lower().count('overlaytiming') or \
       processor.attrib.get('type', '').lower().count('overlaytiming'):
      for param in processor.findall('parameter'):
        if ( param.attrib.get('name') == 'NumberBackground' and param.attrib['value'] == '0.0' ) or \
           ( param.attrib.get('name') == 'NBunchtrain' and param.attrib['value'] == '0' ):
          overlay = False
      if overlay:
        files = getOverlayFiles()
        if not files:
          return S_ERROR('Could not find any overlay files')
        __changeProcessorTagValue( processor, 'parameter', 'BackgroundFileNames', '\n'.join(files), "Overlay files changed")
    # BGOverlay Processor Treatment
    if processor.attrib.get('name','').lower().count('bgoverlay'):
      bkg_Type = 'aa_lowpt' #specific to ILD_DBD #FIXME
      params = processor.findall('parameter')
      for param in params:
        if param.attrib.get('name') == 'expBG' and (param.text == '0' or param.text == '0.0'):
          overlay = False
      if overlay:
        files = getOverlayFiles(bkg_Type)
        if not files:
          return S_ERROR('Could not find any overlay files')
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


def __changeProcessorTagValue( processor, tagTypename, parameterName, newValue, newComment):
  """ modify the value of the tag with *tagTypename* with name attribute equall to *parameterName* to *newValue*

  :param processor: XML object representing processor
  :param tagTypename: name of the tag to look for, e.g. <parameter /> to find all parameters for given processor section
  :param parameterName: name of the parameter to change <parameter name="parameterName"/>
  :param newValue: new value to set for the parameter
  :param newComment: comment to set
  """
  tags = processor.findall( tagTypename )
  for tag in tags:
    if tag.attrib.get('name') == parameterName:
      tag.text = newValue
      com = Comment( newComment )
      processor.insert(0, com)
