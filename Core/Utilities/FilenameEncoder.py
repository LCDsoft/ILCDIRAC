""" Module to encode and decode ILD filename meta information """
import os
import re
import copy

class FilenameEncoder(object):
  '''

  A utility class to decode a output filename from an input file name
  according to the file name convension used by ILD.
  Once rules are defined, output file, directory and meta values
  can be generated base on the `dict` object. See __main__
  attached below.

  Examples to use this class will be found in ILCDIRAC/Core/Utilities/tests/Test_FilenameEncoder.py

  Following keys are used for ILDProduction
  [meta] is meta key defined for corresponding directory::

      %s: ILDConfig for simulation
      %r: ILDConfig for Marlin
      %m: Detector model
      %E: Energy-Machine
      %I: GenProcessID
      %P: ProcessName
      %C: Event Class
      %G: Generator program
      %e: electron polarization or type of photon beam
      %p: positron polarization or type of photon beam
      %d: Data type (gen, sim, rec, dst, dstm, .. )
      %t: Production ID
      %T: Directory name for Production ID
      %n: Generator file number ( could be [0-9]+_[0-9]+, when splitted )
      %j: Job number
      %J: Sub directory ( Job number/1000.  Namely 000, 001, 002, ... )
      %F: File type
      %B: Base directory
      %D: Upper case Data type. Used for meta value
      %w: Energy. for meta value
      %o: Machine parameter. such as TDR_ws for meta value

  :author: Akiya Miyamoto
  :date: 18 October 2016

  '''

  def __init__( self ):
    self.rules={}
    self.rules["gen"]={}
    self.rules["gen"]["file"] = "E%E.P%P.G%G.e%e.p%p.I%I.n%n.d_%d_%t_%j.%F"
    self.rules["gen"]["dir"]  = "%B/%d/%E/%C/%T/%J"
    self.rules["gen"]["meta"] = {"%B/%d"            :{"Datatype":"%D"},
                                 "%B/%d/%E"         :{"Energy":"%w", "MachineParams":"%o"},
                                 "%B/%d/%E/%C"      :{"EventClass":"%C"},
                                 "%B/%d/%E/%C/%T"   :{"ProdID":"%t"},
                                 "%B/%d/%E/%C/%T/%J":{"kJobNumber":"%J"} }

    self.rules["sim"]={}
    self.rules["sim"]["file"] = "s%s.m%m.E%E.I%I.P%P.e%e.p%p.n%n.d_%d_%t_%j.slcio"
    self.rules["sim"]["dir"]  = "%B/%d/%E/%C/%m/%s/%T/%J"
    self.rules["sim"]["meta"] = {"%B/%d"                  :{"Datatype":"%D"},
                                 "%B/%d/%E"               :{"Energy":"%w", "MachineParams":"%o"},
                                 "%B/%d/%E/%C"            :{"EventClass":"%C"},
                                 "%B/%d/%E/%C/%m"         :{"DetectorModel":"%m"},
                                 "%B/%d/%E/%C/%m/%s"      :{"ILDConfig":"%s"},
                                 "%B/%d/%E/%C/%m/%s/%T"   :{"ProdID":"%t"},
                                 "%B/%d/%E/%C/%m/%s/%T/%J":{"kJobNumber":"%J"} }

    self.rules["rec"]={}
    self.rules["rec"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.n%n.d_%d_%t_%j.slcio"
    self.rules["rec"]["dir"]  = "%B/%d/%E/%C/%m/%r/%T/%J"
    self.rules["rec"]["meta"] = {"%B/%d"                  :{"Datatype":"%D"},
                                 "%B/%d/%E"               :{"Energy":"%w", "MachineParams":"%o"},
                                 "%B/%d/%E/%C"            :{"EventClass":"%C"},
                                 "%B/%d/%E/%C/%m"         :{"DetectorModel":"%m"},
                                 "%B/%d/%E/%C/%m/%r"      :{"ILDConfig":"%r"},
                                 "%B/%d/%E/%C/%m/%r/%T"   :{"ProdID":"%t"},
                                 "%B/%d/%E/%C/%m/%r/%T/%J":{"kJobNumber":"%J"} }

    self.rules["dst"]={}
    self.rules["dst"]["file"] = "r%r.s%s.m%m.E%E.I%I.P%P.e%e.p%p.n%n.d_%d_%t_%j.slcio"
    self.rules["dst"]["dir"]  = self.rules["rec"]["dir"]
    self.rules["dst"]["meta"] = self.rules["rec"]["meta"]

# =====================================================
  def __del__( self ):
    self.rules.clear()

# =====================================================
  def getARule(self, datatype, category=""):
    '''

    returns a rule (rules) defined.

    :param str datatype: either "sim", "rec", "dst"
    :param str category: either "file", "dir", "meta"

    :returns: dict, except category="file" or "dir" is defined.

    '''

    if category != "":
      return self.rules[datatype][category]

    return self.rules[datatype]

# =====================================================
  def defineRules( self, rule, datatype="", category="" ):
    '''

    Save rules for various name generation.

    :param dict rule: Rule(s) to make file names, directory name, or directory meta values
                       Example will be found in c'tor statements
    :param str datatype: data tytpe to which the defined rule is applied. For example, "sim", "rec", or "dst"
    :param str category: a kind of rule is whether it is for file name("file"), directory name("dir"),
       or directory meta values ("meta")

    '''
    if datatype == "":
      self.rules = copy.deepcopy(rule)
    elif category   == "":
      self.rules.update(rule)
    else:
      self.rules[datatype][category] = rule

# =====================================================
  def convert( self, datatype, category, values ):
    '''

    Calls file name, directory converter, or meta value maker depending on the input arguments
    and returns filename/directory name as a string or directory meta key and value as a dict object

    :param str datatype: datatype defined by rules, for example sim, rec, dst, ...
    :param str category: Type of output converted.  file, dir (directory) or meta ( meta values )
    :param dict  values: Dictionary object for key word replacement

    :returns: file name or directory name as a string or a directory meta value if category is "meta"

    '''

    if category == "meta":
      return makeDirMetaData( self.rules[datatype]["meta"], values )

    return makeFilename( self.rules[datatype][category], values )

# =====================================================
def decodeFilename( fullpath, separator="." ):
  '''

  Decode a file name to Key and Value according to the DBD file name convention.
  File name is splitted by ".", each item is decoded assuming it consits of
  1 character of key followed by key value.  Exception seen in DBD generator
  files are also handled. Only basename of fullpath is used, even if direcories
  are included in fullpath.

  :param str fullpath: File name in fullpath is decoded.
  :returns: returns a dict object containing key and its value.

  '''

  filename = os.path.basename( fullpath )
  if filename.count('_gen_'):
    ftemp    = re.sub(r'([0-9a-zA-Z])_gen_([0-9]+_[0-9]+_[0-9]+).(stdhep|slcio)', r'\1.d_gen_\2.\3', filename) # Special treatment for ILDDirac old files.
  else:
    ftemp    = re.sub(r'-(\d+).slcio', r'.j\1.slcio', filename)  # Special treatment for DBD sim files.
    ftemp    = re.sub(r'([0-9a-zA-Z])_(sim|rec|dst)_(\d+_\d+).slcio', r'\1.d_\2_\3.slcio', ftemp) # Special treatment for ILDDirac old files.

  replaceList=[ ["Gwhizard-1.95", "Gwhizard-1_95"] ]
  for replacement in replaceList:
    ftemp=ftemp.replace(replacement[0], replacement[1])

  filemeta = {}
  for token in ftemp.split(separator) :
    conv=re.sub(r'^(\d)',r'n\1',token)

    conv=conv.replace("stdhep", "Fstdhep")
    conv=conv.replace("slcio" , "Fslcio")
    key=conv[0:1]
    value=conv[1:]
    if key == "E":
      if value[0:1] == "0":
        value=value[1:]
    if key == "d" :  # special treatment for old file names with prodID and job number
      if value[0:1] == "_":
        dsplit = value[1:].split('_')
        value=dsplit[0]
        filemeta['t'] = dsplit[1]
        if len(dsplit) == 4 and value == 'gen':
          filemeta['n'] = '%3.3d_%3.3d' % ( int(dsplit[2]), int(dsplit[3]) )
        else:
          filemeta['j'] = dsplit[2]

    filemeta[key]=value

  return filemeta

# =================================================
def makeFilename( fileformat, filemeta, preonly=True ):
  '''

  Make a filename, namely, Replace fileformat according to the filemeta.
  Filemeta is a `dict` objects, each entry being key and value.
  As a default, key is one character and "%[key]" in fileformat is
  replaced by "value".  If preonly is "False", "%[key]%" is replaced by value.
  In this case, [key] can be more than one character.
  Filename could be any string, like a fullpath.

  :param str fileformat: A string consisting of keys
  :param dict filemeta:  A dict object of keys and values

  :returns: filename
  :rtype: str
  '''
  filename=fileformat
  for key, value in filemeta.iteritems():
    target="%"+key
    if not preonly :
      target="%"+key+"%"

    filename=filename.replace(target, value)

  return filename

# =================================================
def makeDirMetaData( metaformat, items ):
  '''

  Returns a `dict` object which should be used for directory meta key and value
  definition.  "%[key]" strings in a dict object, metaformat, is
  replaced according to items and a reusltant `dict` object is returned.

  :param dict metaformat: Dictionary object defining the format. The key's and value's of
       metaformat should be string containing "%[key]"
  :param dict items: "%[key]"s in metaformat are replaced by a matching key-value in items

  :returns: Dictionary of directories and their metadata keys and values
  :rtype: dict

  '''
  meta={}
#   pprint.pprint(metaformat)

  for key, value in metaformat.iteritems():
    newkey=key
    newvalue=value
#       print newkey
    for kitem, kvalue in items.iteritems():
      newkey=newkey.replace("%"+kitem, kvalue)

    itemmeta={}
    for varkey, varvalue in newvalue.iteritems():
      vnew=varkey
      vval=varvalue
      for kitem, kvalue in items.iteritems():
        vnew=vnew.replace("%"+kitem, kvalue)
        vval=vval.replace("%"+kitem, kvalue)
        itemmeta[vnew]=vval

    meta[newkey]=itemmeta

  return meta
