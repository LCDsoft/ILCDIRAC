# $HeadURL$
'''
Provides a set of methods to prepare the option files needed by the ILC applications.

@author: Stephane Poss
@since: Jan 29, 2010
'''

__RCSID__ = "$Id$"

from DIRAC import S_OK, gLogger, S_ERROR, gConfig

from xml.etree.ElementTree                                import ElementTree
from xml.etree.ElementTree                                import Element
from xml.etree.ElementTree                                import Comment
from xml.etree.ElementTree                                import tostring
from ILCDIRAC.Core.Utilities.ResolveDependencies          import resolveDeps
from ILCDIRAC.Core.Utilities.PrepareLibs                  import removeLibc
from ILCDIRAC.Core.Utilities.GetOverlayFiles              import getOverlayFiles
from ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation import getSoftwareFolder
from ILCDIRAC.Workflow.Modules.OverlayInput               import allowedBkg
import os

def GetNewLDLibs(systemConfig, application, applicationVersion):
  """ Prepare the LD_LIBRARY_PATH environment variable: make sure all lib folder are included
  @param systemConfig: System config used for the job
  @param application: name of the application considered
  @param applicationVersion: version of the application considered
  @return: new LD_LIBRARY_PATH
  """
  log = gLogger.getSubLogger("GetLDLibs")
  log.verbose("Getting all lib folders")
  new_ld_lib_path = ""
  deps = resolveDeps(systemConfig, application, applicationVersion)
  for dep in deps:
    res = getSoftwareFolder(systemConfig, dep["app"], dep['version'])
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
  if os.environ.has_key("LD_LIBRARY_PATH"):
    if new_ld_lib_path:
      new_ld_lib_path = new_ld_lib_path + ":%s" % os.environ["LD_LIBRARY_PATH"]
    else:
      new_ld_lib_path = os.environ["LD_LIBRARY_PATH"]  
  return new_ld_lib_path

def GetNewPATH(systemConfig, application, applicationVersion):
  """ Same as L{GetNewLDLibs},but for the PATH
  """
  log = gLogger.getSubLogger("GetPaths")
  log.verbose("Getting all PATH folders")
  new_path = ""
  deps = resolveDeps(systemConfig, application, applicationVersion)
  for dep in deps:
    res = getSoftwareFolder(systemConfig, dep['app'], dep['version'])
    if not res['OK']:
      continue
    depfolder = res['Value']
    if os.path.exists(os.path.join(depfolder, "bin")):
      log.verbose("Found bin folder in %s" % (depfolder))
      newpathdir = os.path.join(depfolder, "bin")
      new_path = newpathdir
  if os.environ.has_key("PATH"):
    if new_path:
      new_path = new_path + ":%s" % os.environ["PATH"]
    else:
      new_path = os.environ["PATH"]  
  return new_path

def PrepareWhizardFile(input_in, evttype, energy, randomseed, nevts, lumi, output_in):
  """Prepares the whizard.in file to run
  
  Using specified parameters in the job definition passed from L{WhizardAnalysis}
  
  @param input_in: input whizard.in to modify
  @type input_in: string
  @param evttype: process type that will prepend stdhep output name
  @type evttype: string
  @param randomseed: random seed to use
  @type randomseed: int
  @param nevts: number of events to generate
  @type nevts: int
  @param lumi: luminosity to use
  @type lumi: int
  @param output_in: whizard.in output file name (usually whizard.in)
  @type output_in: string
  @return: S_OK()
  """
  inputfile = file(input_in, "r")  
  outputfile = file(output_in, "w")
  foundprocessid = False
  for line in inputfile:
    if line.count("seed"):
      outputfile.write(" seed = %s\n" % randomseed)
    elif line.count("sqrts"):
      outputfile.write(" sqrts = %s\n" % energy)
    elif line.count("n_events") and not lumi:
      outputfile.write(" n_events = %s\n" % nevts)
    elif lumi and line.count("luminosity"):
      outputfile.write(" luminosity = %s\n" % lumi)
    elif line.count("write_events_file") and len(evttype):
      outputfile.write(" write_events_file = \"%s\" \n" % evttype)
    elif line.count("process_id"):
      outputfile.write(line)
      if len(line.split("\"")[1]):
        foundprocessid = True
    else:
      outputfile.write(line)

  inputfile.close()
  outputfile.close()  

  return S_OK(foundprocessid)

def PrepareWhizardFileTemplate(input_in, evttype, parameters, output_in):
  """Prepares the whizard.in file to run
  
  Using specified parameters in the job definition passed from L{WhizardAnalysis}
  
  @param input_in: input whizard.in to modify
  @type input_in: string
  @param evttype: process type that will prepend stdhep output name
  @type evttype: string
  @param parameters: dictionary of parameters to set in the whizard.in
  @type parameters: dict 
  @param output_in: whizard.in output file name (usually whizard.in)
  @type output_in: string
  @return: S_OK()
  """
  inputfile = file(input_in, "r")  
  outputfile = file(output_in, "w")
  foundprocessid = False
  for line in inputfile:
    if line.count("SEEDSEED"):
      outputfile.write(" seed = %s\n" % parameters['SEED'])
    elif line.count('ENERGYENERGY'):
      outputfile.write(" sqrts = %s\n" % (parameters['ENERGY']))
    elif line.count('RECOILRECOIL'):
      outputfile.write(" beam_recoil = %s\n" % (parameters['RECOIL']))
    elif line.count('NBEVTSNBEVTS'):
      outputfile.write(" n_events = %s\n" % parameters['NBEVTS'])
    elif line.count('LUMILUMI') and parameters['LUMI']:
      outputfile.write(' luminosity=%s\n' % parameters['LUMI'])
    elif line.count('INITIALSINITIALS'):
      outputfile.write(' keep_initials = %s\n' % parameters['INITIALS'])
    elif line.count('PNAME1PNAME1'):
      outputfile.write(' particle_name = \'%s\'\n' % parameters['PNAME1'])
    elif line.count('PNAME2PNAME2'):
      outputfile.write(' particle_name = \'%s\'\n' % parameters['PNAME2'])
    elif line.count('POLAB1POLAB1'):
      outputfile.write(' polarization = %s\n' % parameters['POLAB1'])
    elif line.count('POLAB2POLAB2'):
      outputfile.write(' polarization = %s\n' % parameters['POLAB2'])
    elif line.count('USERB1USERB1'):
      outputfile.write(' USER_spectrum_on = %s\n' % parameters['USERB1'])
    elif line.count('USERB2USERB2'):
      outputfile.write(' USER_spectrum_on = %s\n' % parameters['USERB2'])
    elif line.count('USERSPECTRUMB1'):
      outputfile.write(' USER_spectrum_mode = %s\n' % parameters['USERSPECTRUM'])
    elif line.count('USERSPECTRUMB2'):
      outputfile.write(' USER_spectrum_mode = -%s\n' % parameters['USERSPECTRUM'])
    elif line.count('ISRB1ISRB1'):
      outputfile.write(' ISR_on = %s\n' % parameters['ISRB1'])
    elif line.count('ISRB2ISRB2'):
      outputfile.write(' ISR_on = %s\n' % parameters['ISRB2'])
    elif line.count('EPAB1EPAB1'):
      outputfile.write(' EPA_on = %s\n' % (parameters['EPAB1']))
    elif line.count('EPAB2EPAB2'):
      outputfile.write(' EPA_on = %s\n' % (parameters['EPAB2']))
    elif line.count("write_events_file") and len(evttype):
      outputfile.write(" write_events_file = \"%s\" \n" % evttype)
    elif line.count("process_id"):
      outputfile.write(line)
      if len(line.split("\"")[1]):
        foundprocessid = True
    else:
      outputfile.write(line)

  return S_OK(foundprocessid)

def PrepareSteeringFile(inputSteering, outputSteering, detectormodel,
                        stdhepFile, mac, nbOfRuns, startFrom,
                        randomseed, mcrunnumber,
                        processID='', debug = False, outputlcio = None, 
                        filemeta = {}):
  """Writes out a steering file for Mokka
  
  Using specified parameters in the job definition passed from L{MokkaAnalysis}
  
  @param inputSteering: input steering file name
  @type inputSteering: string
  @param outputSteering: new steering file that will be used by Mokka
  @type outputSteering: string
  @param detectormodel: detector model to use from the DB
  @type detectormodel: string
  @param stdhepFile: generator file name to put in the mac file, if needed
  @type stdhepFile: string
  @param mac: input mac file
  @type mac: string
  @param nbOfRuns: number of runs to use
  @type nbOfRuns: string
  @param startFrom: First event to read from the generator file
  @type startFrom: int
  @param randomseed: Seed to use
  @type randomseed: int
  @param debug: overwrite default print level, if set to True, don't change input steering parameter
  @type debug: bool
  @param outputlcio: output slcio file name, not used
  @type outputlcio: string
  @return: S_OK()
  
  """
  macname = "mokkamac.mac"
  if len(mac) < 1:
    macfile = file(macname, "w")
    if len(stdhepFile) > 0:
      macfile.write("/generator/generator %s\n" % stdhepFile)
    macfile.write("/run/beamOn %s\n" % nbOfRuns)
    macfile.close()
  else:
    macname = mac
    
  inputsteer = file(inputSteering, "r")
  output = file(str(outputSteering), "w")
  for line in inputsteer:
    if not line.count("/Mokka/init/initialMacroFile"):
      if not line.count("/Mokka/init/BatchMode"):
        if not line.count("/Mokka/init/randomSeed"):
          if outputlcio:
            if not line.count("lcioFilename"):
              if detectormodel:
                if not line.count("/Mokka/init/detectorModel"):
                  output.write(line)
                else:
                  output.write(line)
              else:
                output.write(line)
          else:
            if detectormodel:
              if not line.count("/Mokka/init/detectorModel"):
                output.write(line)
            else:
              output.write(line)
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
    if not polb1.count('L') or not polb1.count('R'):
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
    if not polb2.count('L') or not polb2.count('R'):
      polb2 = '0.'
    else:
      polb2 = polb2.replace("L","-").replace("R","")
      if polb2 == '-':
        polb2 = '-1.0'
      elif polb2 == '':
        polb2 = '1.0'
      else:
        polb2 = str(float(polb1)/100.)
    output.write("/Mokka/init/lcioEventParameter float Pol_em %s\n" % float(polb2))
      
  output.write("#Set event start number to value given as job parameter\n")  
  output.write("/Mokka/init/startEventNumber %d\n" % startFrom)
  output.close()
  return S_OK(True)

def fixedXML(element):
  """ As the ElementTree writes out proper XML, we need to corrupt it for LCFI
  """
  fixed_element = element.replace("&amp;","&")
  fixed_element = fixed_element.replace("&gt;",">").replace("&lt;","<")
  return fixed_element

def PrepareXMLFile(finalxml, inputXML, inputGEAR, inputSLCIO,
                   numberofevts, outputFile, outputREC, outputDST, basedir, debug):
  """Write out a xml file for Marlin
  
  Takes in input the specified job parameters for Marlin application given from L{MarlinAnalysis}
  
  @param finalxml: name of the xml file that will be used by Marlin
  @type finalxml: string
  @param inputXML: name of the provided input XML file
  @type inputXML: string
  @param inputSLCIO: input slcio file list
  @type inputSLCIO: list of strings
  @param numberofevts: number of events to process
  @type numberofevts: int
  @param outputREC: file name of REC
  @type outputREC: string
  @param outputDST: file name of DST
  @type outputDST: string
  @param basedir: Base directory, needed for the overlay files resolution
  @type basedir: string
  @param debug: set to True to use given mode, otherwise set verbosity to SILENT
  @type debug: bool
  @return: S_OK()
  
  """
  tree = ElementTree()
  try:
    tree.parse(inputXML)
  except Exception, x:
    print "Found Exception %s %s" % (Exception, x)
    return S_ERROR("Found Exception %s %s" % (Exception, x))

  root = tree.getroot()
  ##Get all processors:
  overlay = False
  #recoutput=False
  #dstoutput=False
  processors = tree.findall('execute/processor')
  for processor in processors:
    if processor.attrib.has_key('name'):
      if processor.attrib['name'].lower().count('overlaytiming'):
        overlay = True
      if processor.attrib['name'].lower().count('bgoverlay'):
        overlay = True  
      #if processor.attrib['name'].lower().count('lciooutputprocessor'):
      #  recoutput=True
      #if processor.attrib['name'].lower().count('dstoutput'):
      #  dstoutput=True  
  params = tree.findall('global/parameter')
  glob = tree.find('global')
  lciolistfound = False
  for param in params:
    if param.attrib.has_key('name'):
      if param.attrib['name'] == 'LCIOInputFiles' and inputSLCIO:
        lciolistfound = True
        com = Comment("input file list changed")
        glob.insert(0, com)
        param.text = inputSLCIO
      if numberofevts > 0:
        if param.attrib['name'] == 'MaxRecordNumber':
          if param.attrib.has_key('value'):
            param.attrib['value'] = str(numberofevts)
            com = Comment("MaxRecordNumber changed")
            glob.insert(0, com)
            
      if param.attrib['name'] == "GearXMLFile":
        if param.attrib.has_key('value'):
          param.attrib['value'] = inputGEAR
          com = Comment("input gear changed")
          glob.insert(0, com)
        else:
          param.text = inputGEAR
          com = Comment("input gear changed")
          glob.insert(0, com)
      if not debug:
        if param.attrib['name'] == 'Verbosity':
          param.text = "SILENT"
          com = Comment("verbosity changed")
          glob.insert(0, com)
  if not lciolistfound and inputSLCIO:
    name = {}
    name["name"] = "LCIOInputFiles"
    lciolist = Element("parameter", name)
    lciolist.text = inputSLCIO
    globparams = tree.find("global")
    globparams.append(lciolist)

  params = tree.findall('processor')
  for param in params:
    if param.attrib.has_key('name'):
      if len(outputFile) > 0:
        if param.attrib['name'] == 'MyLCIOOutputProcessor':
          subparams = param.findall('parameter')
          for subparam in subparams:
            if subparam.attrib.has_key('name'):
              if subparam.attrib['name'] == 'LCIOOutputFile':
                subparam.text = outputFile
                com = Comment("output file changed")
                param.insert(0, com)
      else:
        if len(outputREC) > 0:
          if param.attrib['name'] == 'MyLCIOOutputProcessor':
            subparams = param.findall('parameter')
            for subparam in subparams:
              if subparam.attrib.has_key('name'):
                if subparam.attrib['name'] == 'LCIOOutputFile':
                  subparam.text = outputREC
                  com = Comment("REC file changed")
                  param.insert(0, com)
        if len(outputDST) > 0:
          if param.attrib['name'] == 'DSTOutput':
            subparams = param.findall('parameter')
            for subparam in subparams:
              if subparam.attrib.has_key('name'):
                if subparam.attrib['name'] == 'LCIOOutputFile':
                  subparam.text = outputDST
                  com = Comment("DST file changed")
                  param.insert(0, com)
      if param.attrib['name'].lower().count('overlaytiming'):
        subparams = param.findall('parameter')
        for subparam in subparams:
          if subparam.attrib.has_key('name'):
            if subparam.attrib['name'] == 'NumberBackground':
              if subparam.attrib['value'] == '0.0':
                overlay = False
            if subparam.attrib['name'] == 'NBunchtrain':
              if subparam.attrib['value'] == '0':
                overlay = False          
        if overlay: 
          files = getOverlayFiles( basedir )
          if not len(files):
            return S_ERROR('Could not find any overlay files')
          for subparam in subparams:
            if subparam.attrib.has_key('name'):
              if subparam.attrib['name'] == "BackgroundFileNames":
                subparam.text = "\n".join(files)
                com = Comment("Overlay files changed")
                param.insert(0, com)
      if param.attrib['name'].lower().count('bgoverlay'):
        bkg_Type = 'aa_lowpt' #specific to ILD_DBD
        subparams = param.findall('parameter')
        for subparam in subparams:
          if subparam.attrib.has_key('name'):
            if subparam.attrib['name'] == 'expBG':
              if subparam.text == '0' or subparam.text == '0.0' :
                overlay = False
            if subparam.attrib['name'] == 'NBunchtrain':
              if subparam.text == '0':
                overlay = False          
        if overlay: 
          files = getOverlayFiles(basedir, bkg_Type)
          if not len(files):
            return S_ERROR('Could not find any overlay files')
          for subparam in subparams:
            if subparam.attrib.has_key('name'):
              if subparam.attrib['name'] == "InputFileNames":
                subparam.text = "\n".join(files)
                com = Comment("Overlay files changed")
                param.insert(0, com)
  
  #now, we need to de-escape some characters as otherwise LCFI goes crazy because it does not unescape
  root_str = fixedXML(tostring(root))
  of = file(finalxml,"w")
  of.write(root_str)
  of.close()
  #tree.write(finalxml)
  return S_OK(True)


def PrepareMacFile(inputmac, outputmac, stdhep, nbevts,
                   startfrom, detector = None, randomseed = 0,
                   outputlcio = None, debug = False):
  """Writes out a mac file for SLIC
  
  Takes the parameters passed from L{SLICAnalysis} to define a new mac file if none was provided
  
  @param inputmac: name of the specified mac file
  @type inputmac: string
  @param outputmac: name of the final mac file used by SLIC
  @type outputmac: string
  @param stdhep: name of the generator file to use
  @type stdhep: string
  @param nbevts: number of events to process
  @type nbevts: string
  @param startfrom: event nu,ber to start from in the generator file
  @type startfrom: string
  @param detector: Detector model to use.  
  @type detector: string
  @param outputlcio: name of the produced output slcio file, this is useful when combined with setOutputData of ILCJob class
  @type outputlcio: string

  @return: S_OK()
  """
  inputmacfile = file(inputmac, 'r')
  output = file(outputmac, 'w')
  listtext = []
  for line in inputmacfile:
    if not line.count("/generator/filename"):
      if not line.count("/generator/skipEvents"):
        #if line.find("/run/initialize")<0:
        if not line.count("/random/seed"):
          if not line.count("/lcio/path"):
            if not line.count("/run/beamOn"):
              if detector:
                if not line.count("/lcdd/url"):
                  if outputlcio:
                    if not line.count("/lcio/filename"):
                      #output.write(line)
                      listtext.append(line)
                  else:
                    #output.write(line)
                    listtext.append(line)
              else :
                if outputlcio:
                  if not line.count("/lcio/filename"):
                    #output.write(line)
                    listtext.append(line)
                else: 
                  #output.write(line)
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
  if len(stdhep) > 0:
    output.write("/generator/filename %s\n" % stdhep)
  output.write("/generator/skipEvents %s\n" % startfrom)
  output.write("/random/seed %s\n" % (randomseed))
  output.write("/run/beamOn %s\n" % nbevts)
  inputmacfile.close()
  output.close()
  return S_OK(True)

def PrepareLCSIMFile(inputlcsim, outputlcsim, numberofevents,
                     trackingstrategy, inputslcio, basedir, jars = None,
                     cachedir = None, outputFile = None,
                     outputRECFile = None, outputDSTFile = None,
                     debug = False):
  """Writes out a lcsim file for LCSIM
  
  Takes the parameters passed from LCSIMAnalysis
  
  @param inputlcsim: name of the provided lcsim
  @type inputlcsim: string
  @param outputlcsim: name of the lcsim file on which LCSIM is going to run, defined in L{LCSIMAnalysis}
  @type outputlcsim: string
  @param numberofevents: Number of events to process
  @type numberofevents: int 
  @param inputslcio: list of slcio files on which LCSIM should run
  @type inputslcio: list of string
  @param jars: list of jar files that should be added in the classpath definition
  @type jars: list of strings
  @param cachedir: folder that holds the cache directory, instead of Home
  @type cachedir: string
  @param outputFile: File name of the output
  @type outputFile: string
  @param debug: By default set verbosity to true
  @type debug: bool
  
  @return: S_OK(string)
  """
  printtext = ''

  tree = ElementTree()
  try:
    tree.parse(inputlcsim)
  except Exception, x:
    print "Found Exception %s %s" % (Exception, x)
    return S_ERROR("Found Exception %s %s" % (Exception, x))
  if not len(inputslcio):
    return S_ERROR("Empty input file list")
  ##handle the input slcio file list
  filesinlcsim = tree.find("inputFiles")
  if filesinlcsim:
    filesinlcsim.clear()
  else:
    baseelem = tree.getroot()
    if not baseelem is None:
      filesinlcsim = Element("inputFiles")
      baseelem.append(filesinlcsim)
    else:
      return S_ERROR("Invalid lcsim file structure")
  #set = Element("fileSet")
  for slcio in inputslcio:
    newfile = Element('file')
    newfile.text = slcio
    filesinlcsim.append(newfile)
  #filesinlcsim.append(set)

  if jars:
    if len(jars) > 0:
      classpath = tree.find("classpath")
      if not classpath is None:
        classpath.clear()
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
    if not nbevts is None:
      nbevts.text = str(numberofevents)
    else:
      control = tree.find('control')
      nbevtselm = Element("numberOfEvents")
      nbevtselm.text = str(numberofevents)
      control.append(nbevtselm)
  #handle verbosity
  if debug:
    debugline = tree.find("control/verbose")
    if not debugline is None:
      debugline.text = 'true'
    else:
      control = tree.find('control')
      debugelem = Element('verbose')
      debugelem.text = 'true'
      control.append(debugelem)        

  if cachedir:
    cachedirline = tree.find("control/cacheDirectory")
    if not cachedirline is None:
      cachedirline.text = cachedir
    else:
      control = tree.find('control')
      cachedirelem = Element("cacheDirectory")
      cachedirelem.text = cachedir
      control.append(cachedirelem)
      
  LcsimPrintEveryEvent = 1
  res = gConfig.getOption("/LocalSite/LcsimPrintEveryEvent", 1)
  if not res['OK']:
    LcsimPrintEveryEvent = 1
  else:
    LcsimPrintEveryEvent = res['Value']
  drivers = tree.findall("drivers/driver")      
  eventInterval = tree.find("drivers/driver/eventInterval")
  if not eventInterval is None:
    evtint = eventInterval.text
    if int(evtint) < 10:    
      eventInterval.text = "%s" % LcsimPrintEveryEvent
  else:
    notdriver = True
    for driver in drivers:
      if driver.attrib.has_key("type"):
        if driver.attrib["type"] == "org.lcsim.job.EventMarkerDriver" :
          eventInterval = Element("eventInterval")
          eventInterval.text = "%s" % LcsimPrintEveryEvent
          driver.append(eventInterval)
          notdriver = False
    if notdriver:
      drivers = tree.find("drivers")
      propdict = {}
      propdict['name'] = 'evtMarker'
      propdict['type'] = 'org.lcsim.job.EventMarkerDriver'
      eventmarker = Element("driver", propdict)
      eventInterval = Element("eventInterval")
      eventInterval.text = "%s" % LcsimPrintEveryEvent
      eventmarker.append(eventInterval)
      drivers.append(eventmarker)
      execut = tree.find("execute")
      if(execut):
        evtmarkattrib = {}
        evtmarkattrib['name'] = "evtMarker"
        evtmark = Element("driver", evtmarkattrib)
        execut.append(evtmark)
        
  #drivers = tree.findall("drivers/driver")      

  if trackingstrategy:
    for driver in drivers:
      if driver.attrib.has_key('type'):
        if driver.attrib['type'] == 'org.lcsim.recon.tracking.seedtracker.steeringwrappers.SeedTrackerWrapper':
          driver.remove(driver.find('strategyFile'))
          strategy = Element("strategyFile")
          strategy.text = trackingstrategy
          driver.append(strategy)

  mark = tree.find("drivers/driver/marker")
  if not mark is None:
    printtext = mark.text
  else:
    for driver in drivers:
      if driver.attrib.has_key("type"):
        if driver.attrib["type"] == "org.lcsim.job.EventMarkerDriver" :
          marker = Element("marker")
          marker.text = "LCSIM"
          driver.append(marker)
          printtext = marker.text

  ##Take care of overlay
  for driver in drivers:
    if driver.attrib.has_key("type"):
      if driver.attrib['type'] == "org.lcsim.util.OverlayDriver":
        #if driver.attrib['name']=="eventOverlay":
        ov_name = driver.find("overlayName")
        bkg_Type = "gghad"
        if not ov_name is None:
          bkg_Type = ov_name.text.lower()
          res = allowedBkg(bkg_Type)
          if not res['OK']:
            return res
        driver.remove(driver.find('overlayFiles'))
        files = getOverlayFiles(basedir, bkg_Type)
        if not len(files):
          return S_ERROR('Could not find any overlay files')
        overlay = Element('overlayFiles')
        overlay.text = "\n".join(files)
        driver.append(overlay)
  ##Take care of the output files
  writerfound = False
  recwriterfound = False
  dstwriterfound = False
  for driver in drivers:
    if driver.attrib.has_key("type"):
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
    drivers.append(output)
    execut = tree.find("execute")
    if(execut):
      outputattrib = {}
      outputattrib['name'] = "Writer"
      outputmark = Element("driver", outputattrib)
      execut.append(outputmark)
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

def PrepareTomatoSalad(inputxml, outputxml, inputSLCIO, outputFile, collection):
  """ Prepare the proper steering file for Tomato
  """
  if not inputxml:
    inputxmlf = file('default.xml',"w")
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
    inputxmlf.close()
    inputxml = 'default.xml'
  tree = ElementTree()
  try:
    tree.parse(inputxml)
  except Exception, x:
    print "Found Exception %s %s" % (Exception, x)
    return S_ERROR("Found Exception %s %s" % (Exception, x))
  params = tree.findall('global/parameter')
  glob = tree.find('global')
  lciolistfound = False
  for param in params:
    if param.attrib.has_key('name'):
      if param.attrib['name'] == 'LCIOInputFiles':
        lciolistfound = True
        com = Comment("input file list changed")
        glob.insert(0, com)
        param.text = inputSLCIO
  if not lciolistfound:
    name = {}
    name["name"] = "LCIOInputFiles"
    lciolist = Element("parameter", name)
    lciolist.text = inputSLCIO
    globparams = tree.find("global")
    globparams.append(lciolist)

  params = tree.findall('processor')
  for param in params:
    if param.attrib.has_key('type'):
      if param.attrib['type'] == 'TomatoProcessor':
        subparams = param.findall('parameter')
        for subparam in subparams:
          if subparam.attrib.has_key('name'):
            if outputFile:
              if subparam.attrib['name'] == 'OutputFile':
                com = Comment('Outputfile changed')
                param.insert(0, com)
                subparam.text = outputFile
            if collection:
              if subparam.attrib['name'] == 'MCCollectionName':
                com = Comment('Collections to analyse changed')
                param.insert(0, com)
                subparam.text = collection
         
  tree.write(outputxml)              
  return S_OK()
