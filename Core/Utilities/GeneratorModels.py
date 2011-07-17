###########################################################################
# $HeadURL: $
###########################################################################

""" Contains the list of models and their properties
"""
__RCSID__ = " $Id: $ "

class GeneratorModels(dict):
  def __init__(self):
    self["ms"] = None
    self["slsqhh"] = "LesHouches_slsqhh.msugra_1.in"
    self["chne"] = "LesHouches_chne.msugra_1.in"
