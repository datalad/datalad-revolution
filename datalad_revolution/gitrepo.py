"""Amendment of the DataLad `GitRepo` base class"""
__docformat__ = 'restructuredtext'

from . import utils as ut
from datalad.support.gitrepo import (
    GitRepo as RevolutionGitRepo
)

obsolete_methods = (
    'is_dirty',
)

# remove deprecated methods from API
for m in obsolete_methods:
    if hasattr(RevolutionGitRepo, m):
        setattr(RevolutionGitRepo, m, ut.nothere)
