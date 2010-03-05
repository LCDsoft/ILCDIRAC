# $HeadURL$
# $Id$
'''
LCDDIRAC.Core.Utilities.PrepareSteeringFile

This provides a set of methods to prepare the Mokka steering files : 
the .mac file is created and set into the initial steering file

Created on Jan 29, 2010

@author: sposs
'''


def PrepareSteeringFile(inputSteering,outputSteering,detectormodel,stdhepFile,nbOfRuns,startFrom,outputlcio=None):
  macfile = file("mokkamac.mac","w")
  macfile.write("/generator/generator %s\n"%stdhepFile)
  macfile.write("/run/beamOn %s\n"%nbOfRuns)
  macfile.close()
    
  input = file(inputSteering,"r")
  output = file(str(outputSteering),"w")
  for line in input:
    if line.find("/Mokka/init/initialMacroFile")<0:
      if outputlcio:
        if line.find("lcioFilename")<0:
          if line.find("#")<0:
            if detectormodel:
              if line.find("/Mokka/init/detectorModel")<0:
                output.write(line)
              else:
                output.write(line)
      else:
        if line.find("#")<0:
          if detectormodel:
            if line.find("/Mokka/init/detectorModel")<0:
              output.write(line)
          else:
            output.write(line)
  if detectormodel:
    output.write("/Mokka/init/detectorModel %s"%detectormodel)
      
  output.write("/Mokka/init/initialMacroFile mokkamac.mac\n")
  if outputlcio:
    output.write("/Mokka/init/lcioFilename %s\n"%outputlcio)
  output.write("/Mokka/init/startEventNumber %d"%startFrom)
  output.close()
  return True

def PrepareXMLFile(finalxml,inputXML,inputSLCIO):
  outxml = file(finalxml,'w')
  inputxml = file(inputXML,"r")
  for line in inputxml:
    #if line.find("<!--")<0:
    if line.find("LCIOInputFiles")<0:
      outxml.write(line)
    else:
      outxml.write('<parameter name="LCIOInputFiles"> %s </parameter>'%inputSLCIO)
  outxml.close()
  return True
