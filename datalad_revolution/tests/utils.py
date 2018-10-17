import os
import os.path as op
from six import iteritems

from datalad.api import (
    create,
)

from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
from datalad_revolution.dataset import RevolutionDataset as Dataset

from datalad.tests.utils import (
    assert_is,
    create_tree,
    eq_,
    SkipTest,
)

import datalad_revolution.utils as ut


def assert_repo_status(path, annex=None, untracked_mode='normal', **kwargs):
    """Compare a repo status against (optional) exceptions.

    Anything file/directory that is not explicitly indicated must have
    state 'clean', i.e. no modifications and recorded in Git.

    This is an alternative to the traditional `ok_clean_git` helper.

    Parameters
    ----------
    path: str or Repo
      in case of a str: path to the repository's base dir;
      Note, that passing a Repo instance prevents detecting annex. This might
      be useful in case of a non-initialized annex, a GitRepo is pointing to.
    annex: bool or None
      explicitly set to True or False to indicate, that an annex is (not)
      expected; set to None to autodetect, whether there is an annex.
      Default: None.
    untracked_mode: {'no', 'normal', 'all'}
      If and how untracked content is reported. The specification of untracked
      files that are OK to be found must match this mode. See `Repo.status()`
    **kwargs
      Files/directories that are OK to not be in 'clean' state. Each argument
      must be one of 'added', 'untracked', 'deleted', 'modified' and each
      value must be a list of filenames (relative to the root of the
      repository, in POSIX convention).
    """
    r = None
    if isinstance(path, AnnexRepo):
        if annex is None:
            annex = True
        # if `annex` was set to False, but we find an annex => fail
        assert_is(annex, True)
        r = path
    elif isinstance(path, GitRepo):
        if annex is None:
            annex = False
        # explicitly given GitRepo instance doesn't make sense with
        # 'annex' True
        assert_is(annex, False)
        r = path
    else:
        # 'path' is an actual path
        try:
            r = AnnexRepo(path, init=False, create=False)
            if annex is None:
                annex = True
            # if `annex` was set to False, but we find an annex => fail
            assert_is(annex, True)
        except Exception:
            # Instantiation failed => no annex
            try:
                r = GitRepo(path, init=False, create=False)
            except Exception:
                raise AssertionError("Couldn't find an annex or a git "
                                     "repository at {}.".format(path))
            if annex is None:
                annex = False
            # explicitly given GitRepo instance doesn't make sense with
            # 'annex' True
            assert_is(annex, False)

    status = r.status(untracked=untracked_mode)
    # for any file state that indicates some kind of change (all but 'clean)
    for state in ('added', 'untracked', 'deleted', 'modified'):
        oktobefound = sorted(r.pathobj.joinpath(ut.PurePosixPath(p))
                             for p in kwargs.get(state, []))
        state_files = sorted(k for k, v in iteritems(status)
                             if v.get('state', None) == state)
        eq_(state_files, oktobefound,
            'unexpected content of state "%s": %r != %r'
            % (state, state_files, oktobefound))


def get_convoluted_situation(path, repocls=AnnexRepo):
    if 'APPVEYOR' in os.environ:
        # issue only happens on appveyor, Python itself implodes
        # cannot be reproduced on a real windows box
        raise SkipTest(
            'get_convoluted_situation() causes appveyor to crash, '
            'reason unknown')
    repo = repocls(path, create=True)
    ds = Dataset(repo.path)
    # base content
    create_tree(
        ds.path,
        {
            'subdir': {
                'file_clean': 'file_clean',
                'file_deleted': 'file_deleted',
                'file_modified': 'file_clean',
            },
            'file_clean': 'file_clean',
            'file_deleted': 'file_deleted',
            'file_staged_deleted': 'file_staged_deleted',
            'file_modified': 'file_clean',
        }
    )
    if isinstance(ds.repo, AnnexRepo):
        create_tree(
            ds.path,
            {
                'subdir': {
                    'file_dropped_clean': 'file_dropped_clean',
                },
                'file_dropped_clean': 'file_dropped_clean',
            }
        )
    ds.add('.')
    if isinstance(ds.repo, AnnexRepo):
        # some files straight in git
        create_tree(
            ds.path,
            {
                'subdir': {
                    'file_ingit_clean': 'file_ingit_clean',
                    'file_ingit_modified': 'file_ingit_clean',
                },
                'file_ingit_clean': 'file_ingit_clean',
                'file_ingit_modified': 'file_ingit_clean',
            }
        )
        ds.add('.', to_git=True)
        ds.drop([
            'file_dropped_clean',
            op.join('subdir', 'file_dropped_clean')],
            check=False)
    # clean and proper subdatasets
    ds.create('subds_clean')
    ds.create(op.join('subdir', 'subds_clean'))
    ds.create('subds_unavailable_clean')
    ds.create(op.join('subdir', 'subds_unavailable_clean'))
    # uninstall some subdatasets (still clean)
    ds.uninstall([
        'subds_unavailable_clean',
        op.join('subdir', 'subds_unavailable_clean')],
        check=False)
    assert_repo_status(ds.path)
    # make a dirty subdataset
    ds.create('subds_modified')
    ds.create(op.join('subds_modified', 'someds'))
    ds.create(op.join('subds_modified', 'someds', 'dirtyds'))
    # make a subdataset with additional commits
    ds.create(op.join('subdir', 'subds_modified'))
    pdspath = op.join(ds.path, 'subdir', 'subds_modified', 'progressedds')
    ds.create(pdspath)
    create_tree(
        pdspath,
        {'file_clean': 'file_ingit_clean'}
    )
    Dataset(pdspath).add('.')
    assert_repo_status(pdspath)
    # staged subds, and files
    create(op.join(ds.path, 'subds_added'))
    ds.repo.add_submodule('subds_added')
    create(op.join(ds.path, 'subdir', 'subds_added'))
    ds.repo.add_submodule(op.join('subdir', 'subds_added'))
    # some more untracked files
    create_tree(
        ds.path,
        {
            'subdir': {
                'file_untracked': 'file_untracked',
                'file_added': 'file_added',
            },
            'file_untracked': 'file_untracked',
            'file_added': 'file_added',
            'dir_untracked': {
                'file_untracked': 'file_untracked',
            },
            'subds_modified': {
                'someds': {
                    "dirtyds": {
                        'file_untracked': 'file_untracked',
                    },
                },
            },
        }
    )
    ds.repo.add(['file_added', op.join('subdir', 'file_added')])
    # untracked subdatasets
    create(op.join(ds.path, 'subds_untracked'))
    create(op.join(ds.path, 'subdir', 'subds_untracked'))
    # deleted files
    os.remove(op.join(ds.path, 'file_deleted'))
    os.remove(op.join(ds.path, 'subdir', 'file_deleted'))
    # staged deletion
    ds.repo.remove('file_staged_deleted')
    # modified files
    if isinstance(ds.repo, AnnexRepo):
        ds.repo.unlock(['file_modified', op.join('subdir', 'file_modified')])
        create_tree(
            ds.path,
            {
                'subdir': {
                    'file_ingit_modified': 'file_ingit_modified',
                },
                'file_ingit_modified': 'file_ingit_modified',
            }
        )
    create_tree(
        ds.path,
        {
            'subdir': {
                'file_modified': 'file_modified',
            },
            'file_modified': 'file_modified',
        }
    )
    return ds
