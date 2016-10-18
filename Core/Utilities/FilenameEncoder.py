#
#  A utility class to decide a output filename from a input file name
#  According to the file name convension used by ILD.
#  Once rules are defined, output file, directory and meta values 
#  can be generated base on the DICT object. See __main__ 
#  attached below.
#
#  Akiya Miyamoto, 18 October 2016

import sys, re
import pprint
import copy, types

# Following keys are used for ILDProduction
# [meta] is meta key defined for corresponding directory
#  %s: ILDConfig for simulation
#  %r: ILDConfig for Marlin
#  %m: Detector model
#  %E: Energy-Machine
#  %I: GenProcessID
#  %P: ProcessName
#  %C: Event Class
#  %e: electron polarization or type of photon beam
#  %p: positron polarization or type of photon beam
#  %d: Data type (sim, rec, dst, dstm, .. )
#  %t: Production ID
#  %n: Generator file number
#  %j: Job number
#  %J: Sub directory ( Job number/1000.  Namely 000, 001, 002, ... )
#  %F: File type
#  %B: Base directory
#  %D: Upper case Data type. Used for meta value
#  %w: Energy. for meta value
#  %o: Machine parameter. such as TDR_ws for meta value


# =================================================
class FilenameEncoder():
   def __init__( self ):
     self.rules={}
     self.rules["sim"]={}
     self.rules["sim"]["file"]="%s.%m.%E.%I.%P.%e.%p.%n.%d.%t.%j.slcio"
     self.rules["sim"]["dir"] ="%B/%d/%E/%C/%m/%s/%t/%J"
     self.rules["sim"]["meta"]={"%B/%d":{"Datatype":"%D"}, \
              "%B/%d/%E":{"Energy":"%w", "MachineParams":"%o"}, \
              "%B/%d/%E/%C":{"EventClass":"%C"}, \
              "%B/%d/%E/%C/%m":{"DetectorModel":"%m"}, \
              "%B/%d/%E/%C/%m/%s":{"SimConfig":"%s"}, \
              "%B/%d/%E/%C/%m/%s/%t":{"ProdID":"%t"}, \
              "%B/%d/%E/%C/%m/%s/%t/%J":{"kJobNumber":"%J"} }
     self.rules["rec"]={}
     self.rules["rec"]["file"] = "%r.%s.%m.%E.%I.%P.%e.%p.%n.%d.%t.%j.slcio"
     self.rules["rec"]["dir"]  = self.rules["sim"]["dir"]
     self.rules["rec"]["meta"] = self.rules["sim"]["meta"]
     self.rules["dst"]={}
     self.rules["dst"]["file"] = "%r.%s.%m.%E.%I.%P.%e.%p.%n.%d.%t.%j.slcio"
     self.rules["dst"]["dir"]  = self.rules["sim"]["dir"]
     self.rules["dst"]["meta"] = self.rules["sim"]["meta"]

# =====================================================
   def __del__( self ):
     self.rules.clear()

# =====================================================
   def getARule(self, datatype, purpose="") :
     if purpose != "" :
       return self.rules[datatype][porpose] 
     else :
       return self.rules[datatype]     

# =====================================================
   def defineRules( self, rule, datatype="", category="" ):
#  define conversion rule
     if datatype == "" :
       self.rules = copy.deepcopy(rule) 
     elif category == "" :
       self.rules[datatype] = copy.deepcopy(rule)
     else :
       self.rules[datatype][category] = copy.deepcopy(rule) 

# =====================================================
   def convert( self, datatype, category, values ) :
#
# Calls file name, directory converter, or meta value maker 
# depending on the input arguments.
# datatype : datatype defined by rules, sim, rec, dst, ...
# category : Type of output converted.  file, dir (directory) or meta ( meta values ) 
# values   : Dictionary object for key word replacement

   
     if category == "file" : 
       return self.makeFilename( self.rules[datatype]["file"], values )
     
     elif category == "dir" :
       return self.makeFilename( self.rules[datatype]["dir"], values, addkey=False )
         
     elif category == "meta" :
       return self.makeDirMetaData( self.rules[datatype]["meta"], values ) 

     else :
       msg = "ERROR in FilenameEncoder.convert ... category=%s not defined." % (category) 
       print msg
       return msg

# =====================================================
   def decodeFilename(self, fullpath, replaceList=[["Gwhizard-1.95", "Gwhizard-1_95"]], separator="." ) :
#
# Decode a file name to Key and Value
# According to the DBD file name convention.
# File name is splitted by ".", each item is decoded
# assuming it consits of 1 character of key followed by 
# key value.  Excpetion seen in DBD generator files are also 
# handled.
# Only basename of fullpath is used, even if direcories are included 
# in fullpath
#
# replaceList is a list containing a pair of strings, 
# first string in the file name is replaced by the second string.
# 
  
     dirs = fullpath.split("/")
     filename = dirs[len(dirs)-1]
     ftemp    = filename
     for k in range(0, len(replaceList)):
       old=replaceList[k][0]
       ftemp=ftemp.replace(replaceList[k][0], replaceList[k][1])

     filemeta = {}
     for token in ftemp.split(separator) :
       conv=re.sub(r'^(\d)',r'n\1',token)
       conv=conv.replace("stdhep", "Fstdhep")
       conv=conv.replace("slcio" , "Fslcio")
       key=conv[0:1]
       value=conv[1:]
       if key == "E" :
         if value[0:1] == "0" :
           value=value[1:]

       filemeta[key]=value

     return filemeta

# =================================================
   def makeFilename( self, fileformat, filemeta, addkey=True, preonly=True ) :
# 
# Replace fileformat according to the filemeta.
# Filemeta is a DICT objects, each entry being key and value.
# As a default, key is one character and "%[key]" in fileformat 
# is replaced by "[key][value]".  
# if addkey is "False", replaced by "[value]" 
# if preonly is "False", "%[key]%" is replaced by value. 
# In this case, [key] can be more than one character.

     filename=fileformat
     for key in filemeta.keys() :
       pre=key
       if not addkey : 
         pre=""
       target="%"+key
       if not preonly :
         target="%"+key+"%"
       filename=filename.replace(target, pre+filemeta[key])

     return filename

# =================================================
   def makeDirMetaData( self, metaformat, items ):
#
# Returns a DICT object which should be used for directory meta key and value 
# definition.  "%[key]" strings in a dict object, metaformat, is 
# replaced according to items and a reusltant DICT object is returned.
#

     meta={}  
#   pprint.pprint(metaformat)

     for k in metaformat.keys():
       newkey=k
       newvalue=metaformat[k]
#        print newkey
       for kitem in items.keys() :
         newkey=newkey.replace("%"+kitem, items[kitem])

       itemmeta={}
       for v in newvalue.keys() :
          vnew=v
          vval=newvalue[v]
          for kitem in items.keys() :
            vnew=vnew.replace("%"+kitem, items[kitem])
            vval=vval.replace("%"+kitem, items[kitem])
          itemmeta[vnew]=vval

       meta[newkey]=itemmeta

     return meta

# =================================================
if __name__ == "__main__" : 

# Example to use this command.
#
# python FilenameEncoder.py /ilc/prod/ilc/ild/test/temp1/gensplit/500-TDR_ws/3f/run001/E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eB.pL.I37538.01_001.stdhep
#
# Akiya Miyamoto, 13-October-2016
#
  argvs = sys.argv

  file = "/ilc/prod/ilc/ild/test/temp1/gensplit/500-TDR_ws/3f/run001/E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eB.pL.I37538.01_001.stdhep"
  file = "E0500-TDR_ws.Pae_ell.Gwhizard-1.95.eB.pL.I37538.001.stdhep"
  if len(argvs) > 1 :
    file=argvs[1]

  print file


# ===============================================================
# Decode STDHEP file name and create Sim file and directory name
# ===============================================================
  fe = FilenameEncoder()

  print "######### Sim Filename, directory, meta values "
# Encode stdhep file name
  fileitem = fe.decodeFilename(file)
#   pprint.pprint(filemeta)

  fileitem["s"]="v01-14-01-p00"  # ILDConfig version for Sim
  fileitem["m"]="ILD_o1_v05"  # Detector model
  fileitem["d"]="sim"   # data type
  fileitem["t"]="7642"  # ProductionID
  fileitem["j"]="132"   # Job number

  simfile=fe.convert( "sim", "file", fileitem )
  print "simfile="+simfile

  diritem=copy.deepcopy(fileitem)
  diritem["B"]="/ilc/prod/ilc/ild/test/temp1"        # Base directory
  diritem["J"]="%3.3d"% ( int(fileitem["j"])/ 1000 ) # Sub-directory for job
  diritem["C"]="1f_3f"                               # event class

  dirmetaitem=copy.deepcopy(diritem)
  dirmetaitem["D"]="SIM"                             # Meta value for data type
  energy_machine=diritem["E"].split("-")             # 
  dirmetaitem["w"]=energy_machine[0]                 # Energy
  dirmetaitem["o"]=energy_machine[1]                 # Mcahine parameters  

  simdir = fe.convert( "sim", "dir", diritem)
  simmeta= fe.convert( "sim", "meta", dirmetaitem )

  print "simdir="+simdir
  pprint.pprint(simmeta)



# ===============================================================
# Construct Rec and DST file name from simfile
# ===============================================================

  print "######### Rec Filename, directory, meta values "
# Encode sim filename in order to build rec/dst files, directories
  recitem=fe.decodeFilename(simfile)
#   pprint.pprint(recmeta)
  recitem["r"]="v01-16-p05_500"  # ILDConfig version for Marlin
  recitem["d"]="rec"  # data type 
  recitem["t"]="7643" # #Production ID
  recitem["j"]="1032" # Job number
  recdiritem = recitem
  recdiritem["B"]="/ilc/prod/ilc/ild/test/temp1"
  recdiritem["J"]="%3.3d"% ( int(recitem["j"])/ 1000 )
  recdiritem["C"]="1f_3f"
  recmetaitem = copy.deepcopy(recdiritem)
  recmetaitem["D"]="REC"
  energy_machine=recdiritem["E"].split("-")             #
  recmetaitem["w"]=energy_machine[0]                 # Energy
  recmetaitem["o"]=energy_machine[1]                 # Mcahine parameters
# 
  recfile = fe.convert( "rec", "file", recitem )
  recdir  = fe.convert( "rec", "dir",  recdiritem )
  recmeta = fe.convert( "rec", "meta", recmetaitem )
  print "recfile="+recfile
  print "recdir="+recdir
  pprint.pprint( recmeta )

# 

  print "######### Rec Filename, directory, meta values "
  dstitem         = copy.deepcopy(recitem)
  dstdiritem      = copy.deepcopy(recdiritem)
  dstmetaitem     = copy.deepcopy(recmetaitem)

  dstitem["d"]     = "dst"
  dstdiritem["d"]  = "dst"
  dstmetaitem["d"] = "dst"

  dstfile = fe.convert( "dst", "file", dstitem )
  dstdir  = fe.convert( "dst", "dir",  dstdiritem)
  dstmeta = fe.convert( "dst", "meta", dstmetaitem ) 
  print "dstfile="+dstfile
  print "dstdir="+dstdir
  pprint.pprint(dstmeta)


  del fe

