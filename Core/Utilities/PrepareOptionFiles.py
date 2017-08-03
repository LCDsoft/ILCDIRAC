"""
Provides a set of methods to prepare the option files needed by the ILC applications.

:author: Stephane Poss
:since: Jan 29, 2010
"""

import os


from xml.etree.ElementTree                                import ElementTree
from xml.etree.ElementTree                                import Element
from xml.etree.ElementTree                                import Comment
from xml.etree.ElementTree                                import tostring

from DIRAC import S_OK, gLogger, S_ERROR, gConfig

from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDeps
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.OverlayFiles                 import getOverlayFiles
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Core.Utilities.MarlinXML                    import setOverlayFilesParameter, setOutputFileParameter
from ILCDIRAC.Workflow.Modules.OverlayInput               import allowedBkg


__RCSID__ = "$Id$"

def getNewLDLibs(platform, application, applicationVersion):
  """ Prepare the LD_LIBRARY_PATH environment variable: make sure all lib folder are included

  :param string platform: System config used for the job
  :param string application: name of the application considered
  :param string applicationVersion: version of the application considered
  :return: new LD_LIBRARY_PATH
  """
  log = gLogger.getSubLogger("GetLDLibs")
  log.verbose("Getting all lib folders")
  new_ld_lib_path = ""
  deps = resolveDeps(platform, application, applicationVersion)
  for dep in deps:
    res = getSoftwareFolder(platform, dep["app"], dep['version'])
    if not res['OK']:
      continue
    basedepfolder = res['Value']
    if os.path.exists(os.path.join(basedepfolder, "lib")):
      log.verbose("Found lib folder in %s" % (basedepfolder))
      newlibdir = os.path.join(basedepfolder, "lib")
      new_ld_lib_path = newlibdir
      ####Remove the libc
      removeLibc(new_ld_lib_path)
    if os.path.exists(os.path.join(basedepfolder, "LDLibs")):
      log.verbose("Found lib folder in %s" % (basedepfolder))
      newlibdir = os.path.join(basedepfolder, "LDLibs")
      new_ld_lib_path = newlibdir
      ####Remove the libc
      removeLibc(new_ld_lib_path)
  if "LD_LIBRARY_PATH" in os.environ:
    if new_ld_lib_path:
      new_ld_lib_path = new_ld_lib_path + ":%s" % os.environ["LD_LIBRARY_PATH"]
    else:
      new_ld_lib_path = os.environ["LD_LIBRARY_PATH"]
  return new_ld_lib_path

def getNewPATH(platform, application, applicationVersion):
  """ Same as :func:`getNewLDLibs`,but for the PATH

  :param string platform: System config used for the job
  :param string application: name of the application considered
  :param string applicationVersion: version of the application considered
  :return: new PATH
  """
  log = gLogger.getSubLogger("GetPaths")
  log.verbose("Getting all PATH folders")
  new_path = ""
  deps = resolveDeps(platform, application, applicationVersion)
  for dep in deps:
    res = getSoftwareFolder(platform, dep['app'], dep['version'])
    if not res['OK']:
      continue
    depfolder = res['Value']
    if os.path.exists(os.path.join(depfolder, "bin")):
      log.verbose("Found bin folder in %s" % (depfolder))
      newpathdir = os.path.join(depfolder, "bin")
      new_path = newpathdir
  if "PATH" in os.environ:
    if new_path:
      new_path = new_path + ":%s" % os.environ["PATH"]
    else:
      new_path = os.environ["PATH"]  
  return new_path

def prepareWhizardFile(input_in, evttype, energy, randomseed, nevts, lumi, output_in):
  """Prepares the whizard.in file to run
  
  Using specified parameters in the job definition passed from :mod:`~ILCDIRAC.Workflow.Modules.WhizardAnalysis`
  
  :param string input_in: input whizard.in to modify
  :param string evttype: process type that will prepend stdhep output name
  :param int randomseed: random seed to use
  :param int nevts: number of events to generate
  :param lumi: luminosity to use
  :type lumi: int (float?)
  :param string output_in: whizard.in output file name (usually whizard.in)
  :return: S_OK
  """
  foundprocessid = False
  with open(input_in, "r") as inputfile, open(output_in, "w") as outputfile:
    for line in inputfile:
      if line.count("seed"):
        outputfile.write(" seed = %s\n" % randomseed)
      elif line.count("sqrts"):
        outputfile.write(" sqrts = %s\n" % energy)
      elif line.count("n_events") and not lumi:
        outputfile.write(" n_events = %s\n" % nevts)
      elif lumi and line.count("luminosity"):
        outputfile.write(" luminosity = %s\n" % lumi)
      elif line.count("write_events_file") and evttype:
        outputfile.write(" write_events_file = \"%s\" \n" % evttype)
      elif line.count("process_id"):
        outputfile.write(line)
        if line.split("\"")[1]:
          foundprocessid = True
      else:
        outputfile.write(line)

  return S_OK(foundprocessid)

def prepareWhizardFileTemplate(input_in, evttype, parameters, output_in):
  """Prepares the whizard.in file to run
  
  Using specified parameters in the job definition passed from :mod:`~ILCDIRAC.Workflow.Modules.WhizardAnalysis`
  
  :param string input_in: input whizard.in to modify
  :param string evttype: process type that will prepend stdhep output name
  :param dict parameters: dictionary of parameters to set in the whizard.in
  :param string output_in: whizard.in output file name (usually whizard.in)
  :return: S_OK()
  """
  foundprocessid = False

  replaceDict = {}
  replaceDict["SEEDSEED"]         = " seed = %s\n"                % parameters['SEED']
  replaceDict["ENERGYENERGY"]     = " sqrts = %s\n"               % parameters['ENERGY']
  replaceDict["RECOILRECOIL"]     = " beam_recoil = %s\n"         % parameters['RECOIL']
  replaceDict["NBEVTSNBEVTS"]     = " n_events = %s\n"            % parameters['NBEVTS']
  replaceDict["LUMILUMI"]         = " luminosity=%s\n"            % parameters['LUMI']
  replaceDict["INITIALSINITIALS"] = " keep_initials = %s\n"       % parameters['INITIALS']
  replaceDict["PNAME1PNAME1"]     = " particle_name = '%s'\n"     % parameters['PNAME1']
  replaceDict["PNAME2PNAME2"]     = " particle_name = '%s'\n"     % parameters['PNAME2']
  replaceDict["POLAB1POLAB1"]     = " polarization = %s\n"        % parameters['POLAB1']
  replaceDict["POLAB2POLAB2"]     = " polarization = %s\n"        % parameters['POLAB2']
  replaceDict["USERB1USERB1"]     = " USER_spectrum_on = %s\n"    % parameters['USERB1']
  replaceDict["USERB2USERB2"]     = " USER_spectrum_on = %s\n"    % parameters['USERB2']
  replaceDict["USERSPECTRUMB1"]   = " USER_spectrum_mode = %s\n"  % parameters['USERSPECTRUM']
  replaceDict["USERSPECTRUMB2"]   = " USER_spectrum_mode = -%s\n" % parameters['USERSPECTRUM']
  replaceDict["ISRB1ISRB1"]       = " ISR_on = %s\n"              % parameters['ISRB1']
  replaceDict["ISRB2ISRB2"]       = " ISR_on = %s\n"              % parameters['ISRB2']
  replaceDict["EPAB1EPAB1"]       = " EPA_on = %s\n"              % parameters['EPAB1']
  replaceDict["EPAB2EPAB2"]       = " EPA_on = %s\n"              % parameters['EPAB2']

  with open(input_in, "r") as inputfile, open(output_in, "w") as outputfile:
    for line in inputfile:
      written = False
      for para, value in replaceDict.items():
        if line.count(para):
          outputfile.write(value)
          written = True
          break # break from looping dict
      if line.count("write_events_file") and evttype:
        outputfile.write(' write_events_file = "%s" \n' % evttype)
      elif line.count("process_id"):
        outputfile.write(line)
        if line.split("\"")[1]:
          foundprocessid = True
      elif not written:
        outputfile.write(line)

  return S_OK(foundprocessid)

def prepareSteeringFile(inputSteering, outputSteering, detectormodel,
                        stdhepFile, mac, nbOfRuns, startFrom,
                        randomseed, mcrunnumber,
                        processID='', debug = False, outputlcio = None, 
                        filemeta = None ):
  """Writes out a steering file for Mokka
  
  Using specified parameters in the job definition passed from :mod:`~ILCDIRAC.Workflow.Modules.MokkaAnalysis`
  
  :param string inputSteering: input steering file name
  :param string outputSteering: new steering file that will be used by Mokka
  :param string detectormodel: detector model to use from the DB
  :param string stdhepFile: generator file name to put in the mac file, if needed
  :param string mac: input macro file
  :param int nbOfRuns: number of runs to use
  :param int startFrom: First event to read from the generator file
  :param int randomseed: Seed to use
  :param int mcrunnumber: MC Run number written to lcio file header
  :param string processID: process ID written to lcio file header
  :param bool debug: overwrite default print level. If set to True, don't change printLevel steering parameter
  :param string outputlcio: output slcio file name
  :param dict filemeta: meta data dictionary used to set various metadata parameters Mokka can write to the lcio file header
  :return: S_OK()
  """
  if filemeta is None:
    filemeta = {}

  macname = "mokkamac.mac"
  if len(mac) < 1:
    with open(macname, "w") as macfile:
      if stdhepFile:
        macfile.write("/generator/generator %s\n" % stdhepFile)
      macfile.write("/run/beamOn %s\n" % nbOfRuns)
  else:
    macname = mac

  with open(inputSteering, "r") as inputsteer, open(str(outputSteering), "w") as output:
    for line in inputsteer:
      if line.count("/Mokka/init/initialMacroFile") or line.count("/Mokka/init/BatchMode") or line.count("/Mokka/init/randomSeed"):
        continue
      if outputlcio and line.count("lcioFilename"):
        continue
      if not outputlcio and detectormodel and line.count("/Mokka/init/detectorModel"):
        continue
      output.write( line )
    if detectormodel:
      output.write("#Set detector model to value specified\n")
      output.write("/Mokka/init/detectorModel %s\n" % detectormodel)

    if not debug:
      output.write("#Set debug level to 1\n")
      output.write("/Mokka/init/printLevel 1\n")
    output.write("#Set batch mode to true\n")
    output.write("/Mokka/init/BatchMode true\n")
    output.write("#Set mac file to the one created on the site\n")
    output.write("/Mokka/init/initialMacroFile %s\n" % macname)
    output.write("#Setting random seed\n")
    output.write("/Mokka/init/randomSeed %s\n" % (randomseed))
    output.write("#Setting run number, same as seed\n")
    output.write("/Mokka/init/mcRunNumber %s\n" % (mcrunnumber))
    if outputlcio:
      output.write("#Set outputfile name to job specified\n")
      output.write("/Mokka/init/lcioFilename %s\n" % outputlcio)
    if processID:
      output.write("#Set processID as event parameter\n")
      output.write("/Mokka/init/lcioEventParameter string Process %s\n" % processID)
    elif 'GenProcessID' in filemeta:
      output.write("#Set processID as event parameter\n")
      output.write("/Mokka/init/lcioEventParameter string Process %s\n" % filemeta['GenProcessID'])
    if 'CrossSection' in filemeta:
      output.write("/Mokka/init/lcioEventParameter float CrossSection_fb %s\n" % float(filemeta['CrossSection']))
    if 'Energy' in filemeta:
      output.write("/Mokka/init/lcioEventParameter float Energy %s\n" % float(filemeta['Energy']))
    if 'PolarizationB1' in filemeta:
      polb1 = filemeta['PolarizationB1']
      if not polb1.count('L') and not polb1.count('R'):
        polb1 = '0.'
      else:
        polb1 = polb1.replace("L","-").replace("R","")
        if polb1 == '-':
          polb1 = '-1.0'
        elif polb1 == '':
          polb1 = '1.0'
        else:
          polb1 = str(float(polb1)/100.)
      output.write("/Mokka/init/lcioEventParameter float Pol_ep %s\n" % float(polb1))
    if 'PolarizationB2' in filemeta:
      polb2 = filemeta['PolarizationB2']
      if not polb2.count('L') and not polb2.count('R'):
        polb2 = '0.'
      else:
        polb2 = polb2.replace("L","-").replace("R","")
        if polb2 == '-':
          polb2 = '-1.0'
        elif polb2 == '':
          polb2 = '1.0'
        else:
          polb2 = str(float(polb2)/100.)
      output.write("/Mokka/init/lcioEventParameter float Pol_em %s\n" % float(polb2))

    output.write("#Set event start number to value given as job parameter\n")
    output.write("/Mokka/init/startEventNumber %d\n" % startFrom)

  return S_OK(True)

def fixedXML(element):
  """ As the ElementTree writes out proper XML, we need to corrupt it for LCFI
  """
  fixed_element = element.replace("&amp;","&")
  fixed_element = fixed_element.replace("&gt;",">").replace("&lt;","<")
  return fixed_element

def prepareXMLFile(finalxml, inputXML, inputGEAR, inputSLCIO,
                   numberofevts, outputFile, outputREC, outputDST, debug,
                   dd4hepGeoFile=None,
                   overlayParam=None,
                  ):
  """Write out a xml file for Marlin
  
  Takes in input the specified job parameters for Marlin application given from :mod:`~ILCDIRAC.Workflow.Modules.MarlinAnalysis`
  
  :param string finalxml: name of the xml file that will be used by Marlin
  :param string inputXML: name of the provided input XML file
  :param string inputGEAR: name of the Gear file
  :param inputSLCIO: input slcio file list
  :type inputSLCIO: list of strings
  :param int numberofevts: number of events to process
  :param string outputFile: name of the outputfile
  :param string outputREC: file name of REC
  :param string outputDST: file name of DST
  :param bool debug: set to True to use given mode, otherwise set verbosity to SILENT
  :param str dd4hepGeoFile: path to the dd4hep Geometry XML file, optional, default None
  :param int overlayParam: list of tuples of background type, number of events in each background file, and processorName; optional, default None
  :return: S_OK
  """
  tree = ElementTree()
  try:
    tree.parse(inputXML)
  except Exception as x:
    gLogger.error( "Found Exception when parsing Marlin input XML", repr(x) )
    return S_ERROR("Found Exception when parsing Marlin input XML")

  # Handle inputSLCIO being list or string
  if isinstance(inputSLCIO, list):
    inputSLCIO = " ".join(inputSLCIO)
  elif not isinstance(inputSLCIO, basestring):
    return S_ERROR("inputSLCIO is neither string nor list! Actual type is %s " % type(inputSLCIO))

  glob = tree.find('global')
  lciolistfound = False
  for param in glob.findall("parameter"): #pylint: disable=E1101
    if param.attrib.get('name') == 'LCIOInputFiles' and inputSLCIO:
      lciolistfound = True
      com = Comment("input file list changed")
      glob.insert(0, com) #pylint: disable=E1101
      param.text = inputSLCIO
    if numberofevts > 0 and param.attrib.get('name') == 'MaxRecordNumber':
      if 'value' in param.attrib:
        param.attrib['value'] = str(numberofevts)
        com = Comment("MaxRecordNumber changed")
        glob.insert(0, com) #pylint: disable=E1101
    if param.attrib.get('name') == "GearXMLFile":
      if 'value' in param.attrib:
        param.attrib['value'] = inputGEAR
        com = Comment("input gear changed")
        glob.insert(0, com) #pylint: disable=E1101
      else:
        param.text = str(inputGEAR)
        com = Comment("input gear changed")
        glob.insert(0, com) #pylint: disable=E1101
    if not debug:
      if param.attrib.get('name') == 'Verbosity':
        param.text = "SILENT"
        com = Comment("verbosity changed")
        glob.insert(0, com) #pylint: disable=E1101

  ## Add lcioInputFiles parameter if it was not present before
  if not lciolistfound and inputSLCIO:
    name = {"name": "LCIOInputFiles"}
    lciolist = Element("parameter", name)
    lciolist.text = inputSLCIO
    globparams = tree.find("global")
    globparams.append(lciolist) #pylint: disable=E1101

  resOF = setOutputFileParameter( tree, outputFile, outputREC, outputDST )
  if not resOF['OK']:
    return resOF
  resOver = setOverlayFilesParameter( tree, overlayParam )
  if not resOver['OK']:
    return resOver

  for param in tree.findall('processor'):
    if 'name' not in param.attrib:
      continue
    ## Deal with the InitializeDD4hep parameter value for the XML File
    if param.attrib.get('type') == "InitializeDD4hep" and dd4hepGeoFile is not None:
      for subparam in param.findall('parameter'):
        if subparam.attrib.get('name') == "DD4hepXMLFile":
          subparam.text = dd4hepGeoFile
          com = Comment("DD4hepGeoFile changed")
          param.insert(0, com)



  #now, we need to de-escape some characters as otherwise LCFI goes crazy because it does not unescape
  root = tree.getroot()
  root_str = fixedXML(tostring(root))
  with open(finalxml,"w") as of:
    of.write(root_str)
  #tree.write(finalxml)
  return S_OK(True)


def prepareMacFile(inputmac, outputmac, stdhep, nbevts,
                   startfrom, detector = None, randomseed = 0,
                   outputlcio = None, _debug = False):
  """Writes out a mac file for SLIC
  
  Takes the parameters passed from :mod:`~ILCDIRAC.Workflow.Modules.SLICAnalysis` to define a new mac file if none was provided
  
  :param string inputmac: name of the specified mac file
  :param string outputmac: name of the final mac file used by SLIC
  :param string stdhep: name of the generator file to use
  :param int nbevts: number of events to process
  :param int startfrom: event nu,ber to start from in the generator file
  :param string detector: Detector model to use.
  :param int randomseed: random seed to use for simulation
  :param string outputlcio: name of the produced output slcio file, this is useful when combined with :func:`setOutputData() <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>`
  :param bool debug: UNUSED
  :return: S_OK
  """

  listtext = []
  replacelines = []
  replacelines.append("/generator/filename")
  replacelines.append("/generator/skipEvents")
  #replacelines.append("/run/initialize")
  replacelines.append("/random/seed")
  replacelines.append("/lcio/path")
  replacelines.append("/run/beamOn")

  with open(inputmac, 'r') as inputmacfile, open(outputmac, 'w') as output:

    for line in inputmacfile:
      if any( rl in line for rl in replacelines ):
        continue
      if detector and line.count("/lcdd/url"):
        continue
      if outputlcio and line.count("/lcio/filename"):
        continue

      listtext.append(line)

    finaltext = "\n".join(listtext)
    finaltext += "\n"
    if detector:
      output.write("/lcdd/url %s.lcdd\n" % detector)
    #output.write("/run/initialize\n")
    if outputlcio:
      output.write("/lcio/filename %s\n" % outputlcio)
    output.write("/lcio/runNumber %s\n" % randomseed)
    output.write(finaltext)
    if stdhep:
      output.write("/generator/filename %s\n" % stdhep)
    output.write("/generator/skipEvents %s\n" % startfrom)
    output.write("/random/seed %s\n" % (randomseed))
    output.write("/run/beamOn %s\n" % nbevts)
  return S_OK(True)

def prepareLCSIMFile(inputlcsim, outputlcsim, numberofevents,
                     trackingstrategy, inputslcio, jars = None,
                     cachedir = None, outputFile = None,
                     outputRECFile = None, outputDSTFile = None,
                     debug = False):
  """Writes out a lcsim file for LCSIM
  
  Takes the parameters passed from :mod:`~ILCDIRAC.Workflow.Modules.LCSIMAnalysis`
  
  :param string inputlcsim: name of the provided lcsim
  :param string outputlcsim: name of the lcsim file on which LCSIM is going to run, defined in :mod:`~ILCDIRAC.Workflow.Modules.LCSIMAnalysis`
  :param int numberofevents: Number of events to process
  :param string trackingstrategy: trackingstrategy file to use, can be empty
  :param inputslcio: list of slcio files on which LCSIM should run
  :type inputslcio: list of strings
  :param jars: list of jar files that should be added in the classpath definition
  :type jars: list of strings
  :param string cachedir: folder that holds the cache directory, instead of Home
  :param string outputFile: File name of the output
  :param string outputDSTFile: filename of the DST file
  :param string outputRECFile: filename of the REC file
  :param bool debug: By default set verbosity to true
  
  :return: S_OK(string)
  """
  printtext = ''

  tree = ElementTree()
  try:
    tree.parse(inputlcsim)
  except Exception, x:
    print "Found Exception %s %s" % (Exception, x)
    return S_ERROR("Found Exception %s %s" % (Exception, x))
  if not inputslcio:
    return S_ERROR("Empty input file list")
  baseelem = tree.getroot()
  if baseelem is None:
    return S_ERROR("Invalid lcsim file structure")
  ##handle the input slcio file list
  filesinlcsim = tree.find("inputFiles")
  if filesinlcsim is not None:
    filesinlcsim.clear() #pylint: disable=E1101
  else:
    baseelem = tree.getroot()
    filesinlcsim = Element("inputFiles")
    baseelem.append(filesinlcsim)

  #set = Element("fileSet")
  for slcio in inputslcio:
    newfile = Element('file')
    newfile.text = slcio
    filesinlcsim.append(newfile)
  #filesinlcsim.append(set)

  if jars:
    classpath = tree.find("classpath")
    if classpath is not None:
      classpath.clear() #pylint: disable=E1101
    else:
      baseelem = tree.getroot()
      classpath = Element("classpath")
      baseelem.append(classpath)
    for jar in jars:
      newjar = Element("jar")
      newjar.text = jar
      classpath.append(newjar)
  #handle number of events
  if numberofevents:
    nbevts = tree.find("control/numberOfEvents")     
    if nbevts is not None:
      nbevts.text = str(numberofevents)
    else:
      control = tree.find('control')
      nbevtselm = Element("numberOfEvents")
      nbevtselm.text = str(numberofevents)
      control.append(nbevtselm) #pylint: disable=E1101
  #handle verbosity
  if debug:
    debugline = tree.find("control/verbose")
    if debugline is not None:
      debugline.text = 'true'
    else:
      control = tree.find('control')
      debugelem = Element('verbose')
      debugelem.text = 'true'
      control.append(debugelem) #pylint: disable=E1101

  if cachedir:
    cachedirline = tree.find("control/cacheDirectory")
    if cachedirline is not None:
      cachedirline.text = cachedir
    else:
      control = tree.find('control')
      cachedirelem = Element("cacheDirectory")
      cachedirelem.text = cachedir
      control.append(cachedirelem) #pylint: disable=E1101
      
  res = gConfig.getOption("/LocalSite/LcsimPrintEveryEvent", 1)
  lcsimPrintEveryEvent = 1 if not res['OK'] else res['Value']
  drivers = tree.findall("drivers/driver")      
  eventInterval = tree.find("drivers/driver/eventInterval")
  if eventInterval is not None:
    evtint = eventInterval.text #pylint: disable=E1101
    if int(evtint) < 10:    
      eventInterval.text = "%s" % lcsimPrintEveryEvent
  else:
    notdriver = True
    for driver in drivers:
      if 'type' in driver.attrib:
        if driver.attrib["type"] == "org.lcsim.job.EventMarkerDriver" :
          eventInterval = Element("eventInterval")
          eventInterval.text = "%s" % lcsimPrintEveryEvent
          driver.append(eventInterval)
          notdriver = False
    if notdriver:
      drivers = tree.find("drivers")
      propdict = {}
      propdict['name'] = 'evtMarker'
      propdict['type'] = 'org.lcsim.job.EventMarkerDriver'
      eventmarker = Element("driver", propdict)
      eventInterval = Element("eventInterval")
      eventInterval.text = "%s" % lcsimPrintEveryEvent
      eventmarker.append(eventInterval)
      drivers.append(eventmarker) #pylint: disable=E1101
      execut = tree.find("execute")
      if execut is not None:
        evtmarkattrib = {}
        evtmarkattrib['name'] = "evtMarker"
        evtmark = Element("driver", evtmarkattrib)
        execut.append(evtmark) #pylint: disable=E1101
        
  #drivers = tree.findall("drivers/driver")      

  if trackingstrategy:
    for driver in drivers:
      if 'type' in driver.attrib:
        if driver.attrib['type'] == 'org.lcsim.recon.tracking.seedtracker.steeringwrappers.SeedTrackerWrapper':
          driver.remove(driver.find('strategyFile'))
          strategy = Element("strategyFile")
          strategy.text = trackingstrategy
          driver.append(strategy)

  mark = tree.find("drivers/driver/marker")
  if mark is not None:
    printtext = mark.text #pylint: disable=E1101
  else:
    for driver in drivers:
      if 'type' in driver.attrib:
        if driver.attrib["type"] == "org.lcsim.job.EventMarkerDriver" :
          marker = Element("marker")
          marker.text = "LCSIM"
          driver.append(marker)
          printtext = marker.text

  ##Take care of overlay
  for driver in drivers:
    if 'type' in driver.attrib:
      if driver.attrib['type'] == "org.lcsim.util.OverlayDriver":
        #if driver.attrib['name']=="eventOverlay":
        ov_name = driver.find("overlayName")
        bkg_Type = "gghad"
        if ov_name is not None:
          bkg_Type = ov_name.text.lower()
          res = allowedBkg(bkg_Type)
          if not res['OK']:
            return res
        driver.remove(driver.find('overlayFiles'))
        files = getOverlayFiles(bkg_Type)
        if not files:
          return S_ERROR('Could not find any overlay files')
        overlay = Element('overlayFiles')
        overlay.text = "\n".join(files)
        driver.append(overlay)
  ##Take care of the output files
  writerfound = False
  recwriterfound = False
  dstwriterfound = False
  for driver in drivers:
    if 'type' in driver.attrib:
      if driver.attrib['type'] == "org.lcsim.util.loop.LCIODriver":
        if driver.attrib['name'] == "Writer":
          if outputFile:
            driver.remove(driver.find('outputFilePath'))
            outputelem = Element("outputFilePath")
            outputelem.text = outputFile
            driver.append(outputelem)
          writerfound = True
          continue
        if driver.attrib['name'] == "RECWriter" and outputRECFile:
          driver.remove(driver.find('outputFilePath'))
          outputelem = Element("outputFilePath")
          outputelem.text = outputRECFile
          driver.append(outputelem)
          recwriterfound = True
          continue
        if driver.attrib['name'] == "DSTWriter" and outputDSTFile:
          driver.remove(driver.find('outputFilePath'))
          outputelem = Element("outputFilePath")
          outputelem.text = outputDSTFile
          driver.append(outputelem)
          dstwriterfound = True
          continue
  if not writerfound and outputFile:
    drivers = tree.find("drivers")
    propdict = {}
    propdict['name'] = 'Writer'
    propdict['type'] = 'org.lcsim.util.loop.LCIODriver'
    output = Element("driver", propdict)
    outputelem = Element("outputFilePath")
    outputelem.text = outputFile
    output.append(outputelem)
    drivers.append(output) #pylint: disable=E1101
    execut = tree.find("execute")
    if execut is not None:
      outputattrib = {}
      outputattrib['name'] = "Writer"
      outputmark = Element("driver", outputattrib)
      execut.append(outputmark) #pylint: disable=E1101
  if not recwriterfound and outputRECFile:
    #drivers = tree.find("drivers")
    #propdict = {}
    #propdict['name'] = 'RECWriter'
    #propdict['type'] = 'org.lcsim.util.loop.LCIODriver'
    #output = Element("driver", propdict)
    #outputelem = Element("outputFilePath")
    #outputelem.text = outputRECFile
    #output.append(outputelem)
    #drivers.append(output)
    #execut = tree.find("execute")
    #if(execut):
    #  outputattrib = {}
    #  outputattrib['name'] = "RECWriter"
    #  outputmark = Element("driver", outputattrib)
    #  execut.append(outputmark)
    pass
  if not dstwriterfound and outputDSTFile:
    #drivers = tree.find("drivers")
    #propdict = {}
    #propdict['name'] = 'DSTWriter'
    #propdict['type'] = 'org.lcsim.util.loop.LCIODriver'
    #output = Element("driver", propdict)
    #outputelem = Element("outputFilePath")
    #outputelem.text = outputDSTFile
    #output.append(outputelem)
    #drivers.append(output)
    #execut = tree.find("execute")
    #if(execut):
    #  outputattrib = {}
    #  outputattrib['name'] = "DSTWriter"
    #  outputmark = Element("driver", outputattrib)
    #  execut.append(outputmark)
    pass
  tree.write(outputlcsim)
  return S_OK(printtext)

def prepareTomatoSalad(inputxml, outputxml, inputSLCIO, outputFile, collection):
  """Prepare the proper steering file for Tomato

  :param string inputxml: name of the xml steering file
  :param string outputxml: name of the final tomato steering file
  :param string inputSLCIO: inputSLCIO
  :param string outputFile: name of the produced output slcio file, this is
      useful when combined with :func:`setOutputData()
      <ILCDIRAC.Interfaces.API.NewInterface.UserJob.UserJob.setOutputData>`
  :param string collection: collection to be analysed

  :return: S_OK

  """
  if not inputxml:
    with open('default.xml',"w") as inputxmlf:
      inputxmlf.write("""
<?xml version="1.0" encoding="us-ascii"?>
<!-- ?xml-stylesheet type="text/xsl" href="http://ilcsoft.desy.de/marlin/marlin.xsl"? -->
<!-- ?xml-stylesheet type="text/xsl" href="marlin.xsl"? -->

<marlin xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="http://ilcsoft.desy.de/marlin/marlin.xsd">

   <execute>
      <processor name="MyTomatoProcessor"/>
   </execute>

   <global>
      <parameter name="Verbosity" value="ERROR"/>
   </global>

 <processor name="MyTomatoProcessor" type="TomatoProcessor">
 <!--Automated analysis-->
  <!--Name of the MCParticle collection-->
  <parameter name="MCCollectionName" type="string" lcioInType="MCParticle"> MCParticle </parameter>
  <!--Root OutputFile-->
  <parameter name="OutputFile" type="string" value="tomato.root"/>
  <!--verbosity level of this processor ("DEBUG0-4,MESSAGE0-4,WARNING0-4,ERROR0-4,SILENT")-->
  <!--parameter name="Verbosity" type="string" value=""/-->
</processor>

</marlin>      
    """)
    inputxml = 'default.xml'
  tree = ElementTree()
  try:
    tree.parse(inputxml)
  except Exception as x:
    print "Found Exception %r" % x
    return S_ERROR("Found Exception %r" % x)
  params = tree.findall('global/parameter')
  glob = tree.find('global')
  lciolistfound = False
  for param in params:
    if 'name' in param.attrib:
      if param.attrib['name'] == 'LCIOInputFiles':
        lciolistfound = True
        com = Comment("input file list changed")
        glob.insert(0, com) #pylint: disable=E1101
        param.text = inputSLCIO
  if not lciolistfound:
    name = {}
    name["name"] = "LCIOInputFiles"
    lciolist = Element("parameter", name)
    lciolist.text = inputSLCIO
    globparams = tree.find("global")
    globparams.append(lciolist) #pylint: disable=E1101

  params = tree.findall('processor')
  for param in params:
    if 'type' in param.attrib and param.attrib['type'] == 'TomatoProcessor':
      subparams = param.findall('parameter')
      for subparam in subparams:
        if 'name' in subparam.attrib:
          if outputFile and subparam.attrib['name'] == 'OutputFile':
            com = Comment('Outputfile changed')
            param.insert(0, com)
            subparam.text = outputFile
          if collection and subparam.attrib['name'] == 'MCCollectionName':
            com = Comment('Collections to analyse changed')
            param.insert(0, com)
            subparam.text = collection
         
  tree.write(outputxml)              
  return S_OK()
