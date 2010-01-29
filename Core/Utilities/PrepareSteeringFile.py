'''
LCDDIRAC.Core.Utilities.PrepareSteeringFile

This provides a set of methods to prepare the Mokka steering files : 
the .mac file is created and set into the initial steering file

Created on Jan 29, 2010

@author: sposs
'''


def PrepareSteeringFile(inputSteering,outputSteering,stdhepFile,nbOfRuns,outputlcio):
    macfile = file("mokkamac.mac","w")
    macfile.write("/generator/generator %s\n"%stdhepFile)
    macfile.write("/run/beamOn %s\n"%nbOfRuns)
    macfile.close()
    
    input = file(inputSteering,"r")
    output = file(str(outputSteering),"w")
    for line in input:
        if line.find("/Mokka/init/initialMacroFile")<0:
            if line.find("lcioFilename")<0:
                if line.find("#")<0:
                    output.write(line)
    output.write("/Mokka/init/initialMacroFile mokkamac.mac\n")
    output.write("/Mokka/init/lcioFilename %s\n"%outputlcio)
    output.close()
    return True

