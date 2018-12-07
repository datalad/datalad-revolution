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

from datalad.support.exceptions import CommandError

from datalad.consts import PRE_INIT_COMMIT_SHA
from datalad.cmd import GitRunner

from datalad.tests.utils import (
    with_tempfile,
    create_tree,
    eq_,
    assert_raises,
)

from .. import utils as ut
from ..dataset import RevolutionDataset as Dataset
from datalad.api import (
    rev_save as save,
    rev_create as create,
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


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
def test_diff(path, norepo):
    with chpwd(norepo):
        assert_status('impossible', diff(on_failure='ignore'))
    ds = Dataset(path).create()
    ok_clean_git(ds.path)
    # reports stupid revision input
    assert_result_count(
        ds.diff(revision='WTF', on_failure='ignore'),
        1,
        status='impossible',
        message="fatal: bad revision 'WTF'")
    assert_result_count(ds.diff(), 0)
    # no diff
    assert_result_count(ds.diff(), 0)
    assert_result_count(ds.diff(revision='HEAD'), 0)
    # bogus path makes no difference
    assert_result_count(ds.diff(path='THIS', revision='HEAD'), 0)
    # comparing to a previous state we should get a diff in most cases
    # for this test, let's not care what exactly it is -- will do later
    assert len(ds.diff(revision='HEAD~1')) > 0
    # let's introduce a known change
    create_tree(ds.path, {'new': 'empty'})
    ds.add('.', to_git=True)
    ok_clean_git(ds.path)
    res = ds.diff(revision='HEAD~1')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, action='diff', path=opj(ds.path, 'new'), state='added')
    # we can also find the diff without going through the dataset explicitly
    with chpwd(ds.path):
        assert_result_count(
            diff(revision='HEAD~1'), 1,
            action='diff', path=opj(ds.path, 'new'), state='added')
    # no diff against HEAD
    assert_result_count(ds.diff(), 0)
    # modify known file
    create_tree(ds.path, {'new': 'notempty'})
    for diffy in (None, 'HEAD'):
        res = ds.diff(revision=diffy)
        assert_result_count(res, 1)
        assert_result_count(
            res, 1, action='diff', path=opj(ds.path, 'new'), state='modified')
    # but if we give another path, it doesn't show up
    assert_result_count(ds.diff('otherpath'), 0)
    # giving the right path must work though
    assert_result_count(
        ds.diff('new'), 1,
        action='diff', path=opj(ds.path, 'new'), state='modified')
    # stage changes
    ds.add('.', to_git=True, save=False)
    # no diff, because we staged the modification
    assert_result_count(ds.diff(), 0)
    # but we can get at it
    assert_result_count(
        ds.diff(staged=True), 1,
        action='diff', path=opj(ds.path, 'new'), state='modified')
    # OR
    assert_result_count(
        ds.diff(revision='HEAD'), 1,
        action='diff', path=opj(ds.path, 'new'), state='modified')
    ds.save()
    ok_clean_git(ds.path)

    # untracked stuff
    create_tree(ds.path, {'deep': {'down': 'untracked', 'down2': 'tobeadded'}})
    # a plain diff should report the untracked file
    # but not directly, because the parent dir is already unknown
    res = ds.diff()
    assert_result_count(res, 1)
    assert_result_count(
        res, 1, state='untracked', type='directory', path=opj(ds.path, 'deep'))
    # report of individual files is also possible
    assert_result_count(
        ds.diff(report_untracked='all'), 2, state='untracked', type='file')
    # an unmatching path will hide this result
    assert_result_count(ds.diff(path='somewhere'), 0)
    # perfect match and anything underneath will do
    assert_result_count(
        ds.diff(path='deep'), 1, state='untracked', path=opj(ds.path, 'deep'),
        type='directory')
    assert_result_count(
        ds.diff(path='deep'), 1,
        state='untracked', path=opj(ds.path, 'deep'))
    # now we stage on of the two files in deep
    ds.add(opj('deep', 'down2'), to_git=True, save=False)
    # without any reference it will ignore the staged stuff and report the remaining
    # untracked file
    assert_result_count(
        ds.diff(), 1, state='untracked', path=opj(ds.path, 'deep', 'down'),
        type='file')
    res = ds.diff(staged=True)
    assert_result_count(
        res, 1, state='untracked', path=opj(ds.path, 'deep', 'down'), type='file')
    assert_result_count(
        res, 1, state='added', path=opj(ds.path, 'deep', 'down2'), type='file')


