__docformat__ = 'restructuredtext'

import logging

from datalad.support.gitrepo import GitRepo

from datalad_revolution.utils import nothere

lgr = logging.getLogger('datalad_revolution.gitrepo')

obsolete_methods = (
    'dirty',
    'is_dirty',
)


class RevolutionGitRepo(GitRepo):
    pass


# remove deprecated methods from API
for m in obsolete_methods:
    if hasattr(RevolutionGitRepo, m):
        setattr(RevolutionGitRepo, m, nothere)
