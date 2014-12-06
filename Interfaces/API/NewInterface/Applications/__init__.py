"""
Applications module
"""
__RCSID__ = "$Id"

__all__ = ['GenericApplication', 'GetSRMFile', '_Root', 'RootScript', 'RootMacro',
           'Whizard']

from ILCDIRAC.Interfaces.API.NewInterface.Applications.GenericApplication import GenericApplication
from ILCDIRAC.Interfaces.API.NewInterface.Applications.GetSRMFile import GetSRMFile
from ILCDIRAC.Interfaces.API.NewInterface.Applications._Root import _Root
from ILCDIRAC.Interfaces.API.NewInterface.Applications.RootScript import RootScript
from ILCDIRAC.Interfaces.API.NewInterface.Applications.RootMacro import RootMacro
from ILCDIRAC.Interfaces.API.NewInterface.Applications.Whizard import Whizard
