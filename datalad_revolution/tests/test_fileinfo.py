# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test file info getters"""

import os
import os.path as op
import datalad_revolution.utils as ut

from datalad.tests.utils import (
    with_tempfile,
    create_tree,
    assert_equal,
    assert_dict_equal,
    assert_in,
    assert_not_in,
)

from datalad.api import (
    create,
)

from datalad_revolution.dataset import RevolutionDataset
from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.tests.utils import assert_repo_status


def _get_convoluted_situation(path):
    # TODO remove when `create` is RF to return the new Dataset
    ds = RevolutionDataset(Dataset(path).create(force=True).path)
    # base content, all into the annex
    create_tree(
        ds.path,
        {
            'subdir': {
                'file_clean': 'file_clean',
                'file_dropped_clean': 'file_dropped_clean',
                'file_deleted': 'file_deleted',
                'file_modified': 'file_clean',
            },
            'file_clean': 'file_clean',
            'file_dropped_clean': 'file_dropped_clean',
            'file_deleted': 'file_deleted',
            'file_modified': 'file_clean',
        }
    )
    ds.add('.')
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
            }
        }
    )
    ds.repo.add(['file_added', op.join('subdir', 'file_added')])
    # untracked subdatasets
    create(op.join(ds.path, 'subds_untracked'))
    create(op.join(ds.path, 'subdir', 'subds_untracked'))
    # deleted files
    os.remove(op.join(ds.path, 'file_deleted'))
    os.remove(op.join(ds.path, 'subdir', 'file_deleted'))
    # modified files
    ds.repo.unlock(['file_modified', op.join('subdir', 'file_modified')])
    create_tree(
        ds.path,
        {
            'subdir': {
                'file_modified': 'file_modified',
                'file_ingit_modified': 'file_ingit_modified',
            },
            'file_modified': 'file_modified',
            'file_ingit_modified': 'file_ingit_modified',
        }
    )
    return ds


@with_tempfile
def test_get_content_info(path):
    repo = GitRepo(path)
    assert_equal(repo.get_content_info(), {})

    ds = _get_convoluted_situation(path)
    repopath = ds.repo.pathobj

    assert_equal(ds.pathobj, repopath)
    assert_equal(ds.pathobj, ut.Path(path))

    # with no reference, the worktree is the reference, hence no deleted
    # files are reported
    for f in ds.repo.get_content_annexinfo(init={}, ref=None):
        assert_not_in('deleted', f)
    # with a Git reference, nothing staged can be reported
    for f in ds.repo.get_content_annexinfo(init={}, ref='HEAD'):
        assert_not_in('added', f)

    # verify general rules on fused info records that are incrementally
    # assembled: for git content info, ammended with annex info on 'HEAD'
    # (to get the last commited stage and with it possibly vanished
    # content), and lastly annex info wrt to the present worktree, to
    # also get info on added/staged content
    # this fuses the info reported from
    # - git ls-files
    # - git annex findref HEAD
    # - git annex find --include '*'
    for f, r in ds.repo.get_content_annexinfo(
            init=ds.repo.get_content_annexinfo(
                ref='HEAD')).items():
        if f.match('*_untracked'):
            assert(r['gitshasum'] is None)
        if f.match('*_deleted'):
            assert(not f.exists() and not f.is_symlink() is None)
        if f.match('subds_*'):
            assert(r['type'] == 'dataset' if r['gitshasum'] else 'directory')
        if f.match('file_*'):
            # which one exactly depends on many things
            assert_in(r['type'], ('file', 'symlink'))
        if f.match('file_ingit*'):
            assert(r['type'] == 'file')
        elif '.datalad' not in f.parts and not f.match('.git*') and \
                r['gitshasum'] and not f.match('subds*'):
            # this should be known to annex, one way or another
            # regardless of whether things add deleted or staged
            # or anything inbetween
            assert_in('key', r, f)
            assert_in('keyname', r, f)
            assert_in('backend', r, f)
            assert_in('bytesize', r, f)
            # no duplication with path
            assert_not_in('file', r, f)

    # query a single absolute path
    res = ds.repo.get_content_info(
        [op.join(ds.path, 'subdir', 'file_clean')])
    assert_equal(len(res), 1)
    assert_in(repopath.joinpath('subdir', 'file_clean'), res)

    # query full untracked report
    res = ds.repo.get_content_info()
    assert_in(repopath.joinpath('dir_untracked', 'file_untracked'), res)
    assert_not_in(repopath.joinpath('dir_untracked'), res)
    # query for compact untracked report
    res = ds.repo.get_content_info(untracked='normal')
    assert_not_in(repopath.joinpath('dir_untracked', 'file_untracked'), res)
    assert_in(repopath.joinpath('dir_untracked'), res)
    # query no untracked report
    res = ds.repo.get_content_info(untracked='no')
    assert_not_in(repopath.joinpath('dir_untracked', 'file_untracked'), res)
    assert_not_in(repopath.joinpath('dir_untracked'), res)

    # git status integrity
    status = ds.repo.status()
    for t in ('subds', 'file'):
        for s in ('untracked', 'added', 'deleted', 'clean',
                  'ingit_clean', 'dropped_clean', 'modified',
                  'ingit_modified'):
            for l in ('', ut.PurePosixPath('subdir', '')):
                if t == 'subds' and 'ingit' in s or 'dropped' in s:
                    # invalid combination
                    continue
                if t == 'subds' and s == 'deleted':
                    # same as subds_unavailable -> clean
                    continue
                if t == 'subds' and s == 'modified':
                    # GitRepo.status() doesn't do that ATM, needs recursion
                    continue
                p = repopath.joinpath(l, '{}_{}'.format(t, s))
                assert p.match('*_{}'.format(status[p]['state'])), p
                if t == 'subds':
                    assert_in(status[p]['type'], ('dataset', 'directory'), p)
                else:
                    assert_in(status[p]['type'], ('file', 'symlink'), p)

    # git annex status integrity
    annexstatus = ds.repo.annexstatus()
    for t in ('file',):
        for s in ('untracked', 'added', 'deleted', 'clean',
                  'ingit_clean', 'dropped_clean', 'modified', 'ingit_modified'):
            for l in ('', ut.PurePosixPath('subdir', '')):
                p = repopath.joinpath(l, '{}_{}'.format(t, s))
                if s in ('untracked', 'ingit_clean', 'ingit_modified'):
                    # annex knows nothing about these things
                    assert_not_in('key', annexstatus[p])
                    continue
                assert_in('key', annexstatus[p])
                # dear future,
                # if the next one fails, git-annex might have changed the
                # nature of the path that are being reported by
                # `annex find --json`
                # when this was written `hashir*` was a native path, but
                # `file` was a POSIX path
                assert_equal(annexstatus[p]['has_content'], 'dropped' not in s)


@with_tempfile
def test_compare_content_info(path):
    # TODO remove when `create` is RF to return the new Dataset
    ds = RevolutionDataset(Dataset(path).create().path)
    assert_repo_status(path)

    # for a clean repo HEAD and worktree query should yield identical results
    wt = ds.repo.get_content_info(ref=None)
    assert_dict_equal(wt, ds.repo.get_content_info(ref='HEAD'))
