# $HeadURL$
# $Id$
'''
ILCDIRAC.Core.Utilities.PrepareSteeringFile

This provides a set of methods to prepare the Mokka steering files : 
the .mac file is created and set into the initial steering file

Created on Jan 29, 2010

@author: sposs
'''
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element

def PrepareSteeringFile(inputSteering,outputSteering,detectormodel,stdhepFile,mac,nbOfRuns,startFrom,debug,outputlcio=None):
  macname = "mokkamac.mac"
  if len(mac)<1:
    macfile = file(macname,"w")
    if len(stdhepFile)>0:
      macfile.write("/generator/generator %s\n"%stdhepFile)
    macfile.write("/run/beamOn %s\n"%nbOfRuns)
    macfile.close()
  else:
    macname = mac
    
  input = file(inputSteering,"r")
  output = file(str(outputSteering),"w")
  for line in input:
    if line.find("/Mokka/init/initialMacroFile")<0:
      if line.find("/Mokka/init/BatchMode")<0:
        if outputlcio:
          if line.find("lcioFilename")<0:
            #if line.find("#")>1:
              if detectormodel:
                if line.find("/Mokka/init/detectorModel")<0:
                  output.write(line)
                else:
                  output.write(line)
              else:
                output.write(line)
        else:
          #if line.find("#")==1:
            if detectormodel:
              if line.find("/Mokka/init/detectorModel")<0:
                output.write(line)
            else:
              output.write(line)
  if detectormodel:
    output.write("/Mokka/init/detectorModel %s\n"%detectormodel)
  
  if not debug:
    output.write("/Mokka/init/printLevel 0\n")

  output.write("/Mokka/init/BatchMode true\n")
  output.write("/Mokka/init/initialMacroFile %s\n"%macname)
  if outputlcio:
    output.write("/Mokka/init/lcioFilename %s\n"%outputlcio)
  output.write("/Mokka/init/startEventNumber %d"%startFrom)
  output.close()
  return True

def PrepareXMLFile(finalxml,inputXML,inputGEAR,inputSLCIO,numberofevts,debug):
  tree = ElementTree()
  tree.parse(inputXML)
  params = tree.findall('global/parameter')
  for param in params:
    if param.attrib.has_key('name'):
      if param.attrib['name']=='LCIOInputFiles':
        param.text = inputSLCIO
      if len(numberofevts)>0:
        if param.attrib['name']=='MaxRecordNumber':
          if param.attrib.has_key('value'):
            param.attrib['value'] = numberofevts
      if param.attrib['name']=="GearXMLFile":
        if param.attrib.has_key('value'):
          param.attrib['value'] = inputGEAR
      if not debug:
        if param.attrib['name']=='Verbosity':
          param.text = "SILENT"

  #outxml = file(finalxml,'w')
  #inputxml = file(inputXML,"r")
  #for line in inputxml:
    #if line.find("<!--")<0:
  #  if line.find("LCIOInputFiles")<0:
  #    outxml.write(line)
  #  else:
  #    outxml.write('<parameter name="LCIOInputFiles"> %s </parameter>\n'%inputSLCIO)
  #outxml.close()
  tree.write(finalxml)
  return True

def PrepareMacFile(inputmac,outputmac,stdhep,nbevts,startfrom,detector=None,outputlcio=None):
  inputmacfile = file(inputmac,'r')
  output = file(outputmac,'w')
  for line in inputmacfile:
    if line.find("/generator/filename")<0:
      if line.find("/generator/skipEvents")<0:
        if line.find("/run/beamOn")<0:
          if detector:
            if line.find("/lcdd/url")< 0:
              if outputlcio:
                if line.find("/lcio/filename")<0:
                  output.write(line)
              else:
                output.write(line)
          else :
            if outputlcio:
              if line.find("/lcio/filename")<0:
                output.write(line)
            else:
              output.write(line)
        
  if detector:
    output.write("/lcdd/url %s.lcdd\n"%detector)
  if outputlcio:
    output.write("/lcio/filename %s\n"%outputlcio)
  output.write("/generator/filename %s\n"%stdhep)
  output.write("/generator/skipEvents %s\n"%startfrom)
  output.write("/run/beamOn %s\n"%nbevts)
  inputmacfile.close()
  output.close()
  return True

def PrepareLCSIMFile(inputlcsim,outputlcsim,inputslcio,jars=None,debug=False):
  tree = ElementTree()
  tree.parse(inputlcsim)
  ##handle the input slcio file list
  filesinlcsim = tree.find("inputFiles")
  if filesinlcsim:
    filesinlcsim.clear()
  else:
    baseelem = tree.find("lcsim")
    filesinlcsim = Element("inputFiles")
    baseelem.append(filesinlcsim)
  set = Element("fileSet")
  for slcio in inputslcio:
    newfile = Element('file')
    newfile.text = slcio
    set.append(newfile)
  filesinlcsim.append(set)

  if jars:
    if len(jars)>0:
      classpath = tree.find("classpath")
      if classpath:
        classpath.clear()
      else:
        baseelem = tree.find("lcsim")
        classpath = Element("classpath")    
        baseelem.append(classpath)
      for jar in jars:
        newjar = Element("jar")
        newjar.text = jar
        classpath.append(newjar)
        
  #handle verbosity
  if debug:
    debugline = tree.find("verbose")
    if debugline :
      debugline.text = 'true'
    else:
      control = tree.find('control')
      debugelem = Element('verbose')
      debugelem.text = 'true'
      control.append(debugelem)        
      
  tree.write(outputlcsim)
  return True