###########################################################################################################################################
#
# This is the the script for chaining many pandora photon likelihood files into a single likelihood file
#
# Example usage: in bash
# python MergePandoraLikelihoodData.py "main(['InputFile1.xml', 'InputFile2.xml'], 'OutputFile.xml')"
#
# Input: ['InputFile1.xml', 'InputFile2.xml']
# Ouput: OutputFile.xml
#
# Tested with Pandora v02-08-02. If photon reconstruction algorithm in LCContent is not changed, then this script should be valid
#
###########################################################################################################################################

#!/usr/bin/python

import xml.etree.ElementTree as ET

#------------------------------------------------------------------------------------------------------------------------------------------
#
# get fixed xml files by adding a top element
#
# @param inputFile inputFile a string of input file
# @return root a elementtree with fixed top element
#
#------------------------------------------------------------------------------------------------------------------------------------------


def getFixedXMLTree(inputFile):
    content = ""
    with open(inputFile, "r") as f:
        content = f.read()
    ##fix xml; Added top element in order to use the xml tree structure
    content = "<HEAD>\n" + content + "</HEAD>\n"
    root = ET.fromstring(content)
    return root

#------------------------------------------------------------------------------------------------------------------------------------------
#
# Convert entries from likelihood data to absolute number
#
# @param root a elementtree
# @return root a elementtree with modified entries
#
#------------------------------------------------------------------------------------------------------------------------------------------


def convertLikelihoodToNum(root):
    nSignalVec = [int(i) for i in root.find("NSignalEvents").text.split()]
    nBackgroundVec = [int(i) for i in root.find("NBackgroundEvents").text.split()]

    dataChildrenNodes = [node for node in list(root) if node.tag.find("_") > 0]
    for node in dataChildrenNodes:
        nCurrentEnergyBin = int(node.tag.split("_")[-1])
        nBin = nSignalVec[nCurrentEnergyBin] if node.tag.find("Bkg") < 0 else nBackgroundVec[nCurrentEnergyBin]

        subnode = root.find("./" + node.tag + "/BinContents")
        subnode.text = " ".join([str(int(round(float(i) * nBin))) for i in subnode.text.split()])
    return root

#------------------------------------------------------------------------------------------------------------------------------------------
#
# Convert entries from absolute number to likelihood number
#
# @param root a elementtree
# @return root a elementtree with modified entries
#
#------------------------------------------------------------------------------------------------------------------------------------------


def convertNumToLikelihood(root):
    nSignalVec = [int(i) for i in root.find("NSignalEvents").text.split()]
    nBackgroundVec = [int(i) for i in root.find("NBackgroundEvents").text.split()]

    dataChildrenNodes = [node for node in list(root) if node.tag.find("_") > 0]
    for node in dataChildrenNodes:
        nCurrentEnergyBin = int(node.tag.split("_")[-1])
        nBin = nSignalVec[nCurrentEnergyBin] if node.tag.find("Bkg") < 0 else nBackgroundVec[nCurrentEnergyBin]

        subnode = root.find("./" + node.tag + "/BinContents")
        subnode.text = " ".join([str(float(i) / nBin) for i in subnode.text.split()])
    return root

#------------------------------------------------------------------------------------------------------------------------------------------
#
# Add entries of a tag of two trees
#
# @param initialRoot a elementtree
# @param finalRoot a elementtree to be modified
# @param tag the tag to be operated
# @return finalRoot a elementtree with modified entries
#
#------------------------------------------------------------------------------------------------------------------------------------------


def addTwoNode(initialRoot, finalRoot, tag):
    nVecFinal = [int(i) for i in finalRoot.find(tag).text.split()]
    nVecInitial = [int(i) for i in initialRoot.find(tag).text.split()]
    nVecSim = [sum(x) for x in zip(nVecFinal, nVecInitial)]
    finalRoot.find(tag).text = " ".join(str(i) for i in nVecSim)
    return finalRoot

#------------------------------------------------------------------------------------------------------------------------------------------
#
# This is the the main method for chaining many pandora photon likelihood files into a single likelihood file
#
# @param inputFiles a list of strings with names of input files
# @param outputFile a string of output file
#
#------------------------------------------------------------------------------------------------------------------------------------------


def main(inputFiles=["InputFile1.xml", "InputFile2.xml"], outputFile="OutputFile.xml"):
    finalRoot = None
    for inputFile in inputFiles:
        root = convertLikelihoodToNum(getFixedXMLTree(inputFile))

        if (finalRoot == None):
            finalRoot = root
        else:
            finalRoot = addTwoNode(root, finalRoot, "NSignalEvents")
            finalRoot = addTwoNode(root, finalRoot, "NBackgroundEvents")

            dataChildrenNodes = [node for node in list(root) if node.tag.find("_") > 0]
            for node in dataChildrenNodes:
                finalRoot = addTwoNode(root, finalRoot, "./" + node.tag + "/BinContents")

    finalRoot = convertNumToLikelihood(finalRoot)

    #fix xml; Strip the top element to revert the change
    finalCotent = ET.tostring(finalRoot).strip('</HEAD>').strip('<HEAD>').lstrip()

    with open(outputFile, "w") as f:
        f.write(finalCotent)
    return

#------------------------------------------------------------------------------------------------------------------------------------------
#
# This is the help function for calling methods in bash and record time
#
#------------------------------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    import sys
    try:
        func = sys.argv[1]
    except:
        func = None
    if func:
        try:
            import datetime
            timeNow = datetime.datetime.now()
            exec 'print %s' % func
            timeEnd = datetime.datetime.now()
            print ("%s starts at %s, finishes at %s, elapsed %s s" % (func, timeNow, timeEnd, timeEnd - timeNow))
        except:
            print "Error: incorrect syntax '%s'" % func
            exec 'print %s.__doc__' % func.split('(')[0]
    else:
        print "Error"
