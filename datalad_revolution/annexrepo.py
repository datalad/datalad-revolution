"""Amendment of the DataLad `AnnexRepo` base class"""
__docformat__ = 'restructuredtext'

from . import utils as ut

from datalad.support.annexrepo import AnnexRepo as RevolutionAnnexRepo

from .gitrepo import (
    obsolete_methods as gitrepo_obsolete_methods,
)

obsolete_methods = (
    # next two are only needed by 'ok_clean_git'
    # 'untracked_files',
    # 'get_status',
    'is_dirty',
)

# remove deprecated methods from API
for m in obsolete_methods + gitrepo_obsolete_methods:
    if hasattr(RevolutionAnnexRepo, m):
        setattr(RevolutionAnnexRepo, m, ut.nothere)
