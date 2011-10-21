###########################################################################
# $HeadURL: $
###########################################################################

""" Contains the list of models and their properties
"""
__RCSID__ = " $Id: $ "

from DIRAC import gConfig,S_OK,S_ERROR

class GeneratorModels():
  """ Contains the list of known models
  """
  def __init__(self):
    self.models = {}
    res = gConfig.getOptionsDict("/Operations/Models")
    if res['OK']:
      self.models = res['Value']

  def hasModel(self,model):
    if self.models.has_key(model):
      return S_OK()
    else:
      return S_ERROR("Model %s is not defined, use any of %s"%(model, self.models.keys()))

  def getFile(self,model):
    res = self.hasModel(model)
    if not res['OK']:
      return res
    if not self.models[model]:
      return S_ERROR("No file attached to model %s"%model)
    return S_OK(self.models[model])