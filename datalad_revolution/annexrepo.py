__docformat__ = 'restructuredtext'

import logging

from datalad.support.annexrepo import AnnexRepo

from datalad_revolution.gitrepo import (
    RevolutionGitRepo,
    obsolete_methods as gitrepo_obsolete_methods,
)
from datalad_revolution.utils import nothere

lgr = logging.getLogger('datalad_revolution.annexrepo')

obsolete_methods = (
    'get_status',
    'untracked_files',
    'is_dirty',
)


class RevolutionAnnexRepo(AnnexRepo, RevolutionGitRepo):
    pass


# remove deprecated methods from API
for m in obsolete_methods + gitrepo_obsolete_methods:
    if hasattr(RevolutionAnnexRepo, m):
        setattr(RevolutionAnnexRepo, m, nothere)
