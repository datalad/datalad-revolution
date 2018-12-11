# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""test dataset diff

"""

__docformat__ = 'restructuredtext'

import os.path as op
from datalad.support.exceptions import (
    NoDatasetArgumentFound,
)

from datalad.consts import PRE_INIT_COMMIT_SHA
from datalad.cmd import GitRunner

from datalad.tests.utils import (
    with_tempfile,
    create_tree,
    eq_,
    ok_,
    assert_raises,
    assert_status,
    chpwd,
    assert_result_count,
)

from .. import utils as ut
from ..dataset import RevolutionDataset as Dataset
from datalad.api import (
    rev_save as save,
    rev_create as create,
    rev_diff as diff,
)
from .utils import (
    assert_repo_status,
)


def test_magic_number():
    # we hard code the magic SHA1 that represents the state of a Git repo
    # prior to the first commit -- used to diff from scratch to a specific
    # commit
    # given the level of dark magic, we better test whether this stays
    # constant across Git versions (it should!)
    out, err = GitRunner().run('cd ./ | git hash-object --stdin -t tree')
    eq_(out.strip(), PRE_INIT_COMMIT_SHA)


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_repo_diff(path, norepo):
    ds = Dataset(path).rev_create()
    assert_repo_status(ds.path)
    assert_raises(ValueError, ds.repo.diff, fr='WTF', to='MIKE')
    # no diff
    eq_(ds.repo.diff('HEAD', None), {})
    # bogus path makes no difference
    eq_(ds.repo.diff('HEAD', None, paths=['THIS']), {})
    # let's introduce a known change
    create_tree(ds.path, {'new': 'empty'})
    ds.rev_save(to_git=True)
    assert_repo_status(ds.path)
    eq_(ds.repo.diff(fr='HEAD~1', to='HEAD'),
        {ut.Path(ds.repo.pathobj / 'new'): {
            'state': 'added',
            'type': 'file',
            'gitshasum': '7b4d68d70fcae134d5348f5e118f5e9c9d3f05f6'}})
    # modify known file
    create_tree(ds.path, {'new': 'notempty'})
    eq_(ds.repo.diff(fr='HEAD', to=None),
        {ut.Path(ds.repo.pathobj / 'new'): {
            'state': 'modified',
            'type': 'file'}})
    # per path query gives the same result
    eq_(ds.repo.diff(fr='HEAD', to=None),
        ds.repo.diff(fr='HEAD', to=None, paths=['new']))
    # also given a directory as a constraint does the same
    eq_(ds.repo.diff(fr='HEAD', to=None),
        ds.repo.diff(fr='HEAD', to=None, paths=['.']))
    # but if we give another path, it doesn't show up
    eq_(ds.repo.diff(fr='HEAD', to=None, paths=['other']), {})

    # make clean
    ds.rev_save()
    assert_repo_status(ds.path)

    # untracked stuff
    create_tree(ds.path, {'deep': {'down': 'untracked', 'down2': 'tobeadded'}})
    # default is to report all files
    eq_(ds.repo.diff(fr='HEAD', to=None),
        {
            ut.Path(ds.repo.pathobj / 'deep' / 'down'): {
                'state': 'untracked',
                'type': 'file'},
            ut.Path(ds.repo.pathobj / 'deep' / 'down2'): {
                'state': 'untracked',
                'type': 'file'}})
    # but can be made more compact
    eq_(ds.repo.diff(fr='HEAD', to=None, untracked='normal'),
        {
            ut.Path(ds.repo.pathobj / 'deep'): {
                'state': 'untracked',
                'type': 'directory'}})

    # again a unmatching path constrainted will give an empty report
    eq_(ds.repo.diff(fr='HEAD', to=None, paths=['other']), {})
    # perfect match and anything underneath will do
    eq_(ds.repo.diff(fr='HEAD', to=None, paths=['deep']),
        {
            ut.Path(ds.repo.pathobj / 'deep' / 'down'): {
                'state': 'untracked',
                'type': 'file'},
            ut.Path(ds.repo.pathobj / 'deep' / 'down2'): {
                'state': 'untracked',
                'type': 'file'}})


def _dirty_results(res):
    return [r for r in res if r.get('state', None) != 'clean']


# this is an extended variant of `test_repo_diff()` above
# that focuses on the high-level command API
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_diff(path, norepo):
    with chpwd(norepo):
        assert_raises(NoDatasetArgumentFound, diff)
    ds = Dataset(path).rev_create()
    assert_repo_status(ds.path)
    # reports stupid revision input
    assert_result_count(
        ds.rev_diff(fr='WTF', on_failure='ignore'),
        1,
        status='impossible',
        message="Git reference 'WTF' invalid")
    # no diff
    assert_result_count(_dirty_results(ds.rev_diff()), 0)
    assert_result_count(_dirty_results(ds.rev_diff(fr='HEAD')), 0)
    # bogus path makes no difference
    assert_result_count(_dirty_results(ds.rev_diff(path='THIS', fr='HEAD')), 0)
    # let's introduce a known change
    create_tree(ds.path, {'new': 'empty'})
    ds.rev_save(to_git=True)
    assert_repo_status(ds.path)
    res = _dirty_results(ds.rev_diff(fr='HEAD~1'))
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, action='diff', path=op.join(ds.path, 'new'), state='added')
    # we can also find the diff without going through the dataset explicitly
    with chpwd(ds.path):
        assert_result_count(
            _dirty_results(diff(fr='HEAD~1')), 1,
            action='diff', path=op.join(ds.path, 'new'), state='added')
    # no diff against HEAD
    assert_result_count(_dirty_results(ds.rev_diff()), 0)
    # modify known file
    create_tree(ds.path, {'new': 'notempty'})
    res = _dirty_results(ds.rev_diff())
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, action='diff', path=op.join(ds.path, 'new'),
        state='modified')
    # but if we give another path, it doesn't show up
    assert_result_count(ds.rev_diff(path='otherpath'), 0)
    # giving the right path must work though
    assert_result_count(
        ds.rev_diff(path='new'), 1,
        action='diff', path=op.join(ds.path, 'new'), state='modified')
    # stage changes
    ds.repo.add('.', git=True)
    # no change in diff, staged is not commited
    assert_result_count(_dirty_results(ds.rev_diff()), 1)
    ds.rev_save()
    assert_repo_status(ds.path)
    assert_result_count(_dirty_results(ds.rev_diff()), 0)

    # untracked stuff
    create_tree(ds.path, {'deep': {'down': 'untracked', 'down2': 'tobeadded'}})
    # a plain diff should report the untracked file
    # but not directly, because the parent dir is already unknown
    res = _dirty_results(ds.rev_diff())
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, state='untracked', type='directory',
        path=op.join(ds.path, 'deep'))
    # report of individual files is also possible
    assert_result_count(
        ds.rev_diff(untracked='all'), 2, state='untracked', type='file')
    # an unmatching path will hide this result
    assert_result_count(ds.rev_diff(path='somewhere'), 0)
    # perfect match and anything underneath will do
    assert_result_count(
        ds.rev_diff(path='deep'), 1, state='untracked',
        path=op.join(ds.path, 'deep'),
        type='directory')
    assert_result_count(
        ds.rev_diff(path='deep'), 1,
        state='untracked', path=op.join(ds.path, 'deep'))
    ds.repo.add(op.join('deep', 'down2'), git=True)
    # now the remaining file is the only untracked one
    assert_result_count(
        ds.rev_diff(), 1, state='untracked',
        path=op.join(ds.path, 'deep', 'down'),
        type='file')


@with_tempfile(mkdir=True)
def test_diff_recursive(path):
    ds = Dataset(path).rev_create()
    sub = ds.rev_create('sub')
    # look at the last change, and confirm a dataset was added
    res = ds.diff(revision='HEAD~1..HEAD')
    assert_result_count(
        res, 1, action='diff', state='added', path=sub.path, type='dataset')
    # now recursive
    res = ds.diff(recursive=True, revision='HEAD~1..HEAD')
    # we also get the entire diff of the subdataset from scratch
    assert_status('ok', res)
    ok_(len(res) > 3)
    # one specific test
    assert_result_count(
        res, 1, action='diff', state='added',
        path=op.join(sub.path, '.datalad', 'config'))

    # now we add a file to just the parent
    create_tree(
        ds.path,
        {'onefile': 'tobeadded', 'sub': {'twofile': 'tobeadded'}})
    res = ds.diff(recursive=True, report_untracked='all')
    assert_result_count(res, 3)
    assert_result_count(
        res, 1,
        action='diff', state='untracked', path=op.join(ds.path, 'onefile'),
        type='file')
    assert_result_count(
        res, 1,
        action='diff', state='modified', path=sub.path, type='dataset')
    assert_result_count(
        res, 1,
        action='diff', state='untracked', path=op.join(sub.path, 'twofile'),
        type='file')
    # intentional save in two steps to make check below easier
    ds.rev_save('sub', recursive=True)
    ds.rev_save()
    assert_repo_status(ds.path)
    # look at the last change, only one file was added
    res = ds.diff(revision='HEAD~1..HEAD')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        action='diff', state='added', path=op.join(ds.path, 'onefile'),
        type='file')

    # now the exact same thing with recursion, must not be different from the
    # call above
    res = ds.diff(recursive=True, revision='HEAD~1..HEAD')
    assert_result_count(res, 1)
    # last change in parent
    assert_result_count(
        res, 1, action='diff', state='added', path=op.join(ds.path, 'onefile'),
        type='file')

    # one further back brings in the modified subdataset, and the added file
    # within it
    res = ds.diff(recursive=True, revision='HEAD~2..HEAD')
    assert_result_count(res, 3)
    assert_result_count(
        res, 1,
        action='diff', state='added', path=op.join(ds.path, 'onefile'),
        type='file')
    assert_result_count(
        res, 1,
        action='diff', state='added', path=op.join(sub.path, 'twofile'),
        type='file')
    assert_result_count(
        res, 1,
        action='diff', state='modified', path=sub.path, type='dataset')
