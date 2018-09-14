########################################################################
# $Id: ResolveSE.py 21477 2010-02-16 14:06:06Z paterson $
########################################################################
""" Resolve SE takes the workflow SE description and returns the list
    of destination storage elements for uploading an output file.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.SiteSEMapping                   import getSEsForSite
from DIRAC.Core.Utilities.List                            import uniqueElements
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

LOG = gLogger.getSubLogger(__name__)


def getDestinationSEList(outputSE, site, outputmode='Any'):
  """ Evaluate the output SE list from a workflow and return the concrete list
      of SEs to upload output data.
  """
  # Add output SE defined in the job description
  LOG.info('Resolving workflow output SE description: %s' % outputSE)

  # Check if the SE is defined explicitly for the site
  prefix = site.split('.')[0]
  country = site.split('.')[-1]
  # Concrete SE name
  result = gConfig.getOptions('/Resources/StorageElements/' + outputSE)
  if result['OK']:
    LOG.info('Found concrete SE %s' % outputSE)
    return S_OK([outputSE])
  # There is an alias defined for this Site
  alias_se = gConfig.getValue('/Resources/Sites/%s/%s/AssociatedSEs/%s' % (prefix, site, outputSE), [])
  if alias_se:
    LOG.info('Found associated SE for site %s' % (alias_se))
    return S_OK(alias_se)

  localSEs = getSEsForSite(site)['Value']
  LOG.verbose('Local SE list is: %s' % (localSEs))
  groupSEs = gConfig.getValue('/Resources/StorageElementGroups/' + outputSE, [])
  LOG.verbose('Group SE list is: %s' % (groupSEs))
  if not groupSEs:
    return S_ERROR('Failed to resolve SE ' + outputSE)

  if outputmode.lower() == "local":
    for se in localSEs:
      if se in groupSEs:
        LOG.info('Found eligible local SE: %s' % (se))
        return S_OK([se])

    #check if country is already one with associated SEs
    associatedSE = gConfig.getValue('/Resources/Countries/%s/AssociatedSEs/%s' % (country, outputSE), '')
    if associatedSE:
      LOG.info('Found associated SE %s in /Resources/Countries/%s/AssociatedSEs/%s' % (associatedSE, country, outputSE))
      return S_OK([associatedSE])

    # Final check for country associated SE
    count = 0
    assignedCountry = country
    while count < 10:
      LOG.verbose('Loop count = %s' % (count))
      LOG.verbose("/Resources/Countries/%s/AssignedTo" % assignedCountry)
      opt = gConfig.getOption("/Resources/Countries/%s/AssignedTo" % assignedCountry)
      if opt['OK'] and opt['Value']:
        assignedCountry = opt['Value']
        LOG.verbose('/Resources/Countries/%s/AssociatedSEs' % assignedCountry)
        assocCheck = gConfig.getOption('/Resources/Countries/%s/AssociatedSEs' % assignedCountry)
        if assocCheck['OK'] and assocCheck['Value']:
          break
      count += 1

    if not assignedCountry:
      return S_ERROR('Could not determine associated SE list for %s' % country)

    alias_se = gConfig.getValue('/Resources/Countries/%s/AssociatedSEs/%s' % (assignedCountry, outputSE), [])
    if alias_se:
      LOG.info('Found alias SE for site: %s' % alias_se)
      return S_OK(alias_se)
    else:
      LOG.error('Could not establish alias SE for country %s from section: /Resources/Countries/%s/AssociatedSEs/%s' %
                (country, assignedCountry, outputSE))
      return S_ERROR('Failed to resolve SE ' + outputSE)

  # For collective Any and All modes return the whole group

  # Make sure that local SEs are passing first
  newSEList = []
  for se in groupSEs:
    if se in localSEs:
      newSEList.append(se)
  uniqueSEs = uniqueElements(newSEList + groupSEs)
  LOG.verbose('Found unique SEs: %s' % (uniqueSEs))
  return S_OK(uniqueSEs)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
