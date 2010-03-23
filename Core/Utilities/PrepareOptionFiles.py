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

def PrepareSteeringFile(inputSteering,outputSteering,detectormodel,stdhepFile,nbOfRuns,startFrom,outputlcio=None,debug):
  macfile = file("mokkamac.mac","w")
  macfile.write("/generator/generator %s\n"%stdhepFile)
  macfile.write("/run/beamOn %s\n"%nbOfRuns)
  macfile.close()
    
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
    output.write("/Mokka/init/printLevel 0/n")

  output.write("/Mokka/init/BatchMode true\n")
  output.write("/Mokka/init/initialMacroFile mokkamac.mac\n")
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
          param.text = "MESSAGE"

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
