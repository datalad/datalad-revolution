# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test save command"""

import os
import os.path as op

from datalad.tests.utils import (
    assert_status,
    assert_raises,
    create_tree,
    with_tempfile,
    with_tree,
    with_testrepos,
    eq_,
    ok_,
    chpwd,
    known_failure_windows,
)

from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.dataset import RevolutionDataset
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
from datalad.api import (
    rev_save as save,
    create,
    add,
)

from datalad_revolution.tests.utils import (
    assert_repo_status,
)


@with_testrepos('.*git.*', flavors=['clone'])
def test_save(path):

    ds = RevolutionDataset(path)

    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("something")

    ds.repo.add("new_file.tst", git=True)
    ok_(ds.repo.dirty)

    ds.rev_save("add a new file")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("modify")

    ok_(ds.repo.dirty)
    ds.rev_save("modified new_file.tst")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # save works without ds and files given in the PWD
    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("rapunzel")
    with chpwd(path):
        save("love rapunzel")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # and also without `-a` when things are staged
    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("exotic")
    ds.repo.add("new_file.tst", git=True)
    with chpwd(path):
        save("love marsians")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    files = ['one.txt', 'two.txt']
    for fn in files:
        with open(op.join(path, fn), "w") as f:
            f.write(fn)

    ds.add([op.join(path, f) for f in files])
    # superfluous call to save (add saved it already), should not fail
    # but report that nothing was saved
    assert_status('notneeded', ds.rev_save("set of new files"))
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # create subdataset
    subds = ds.create('subds')
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))
    # modify subds
    with open(op.join(subds.path, "some_file.tst"), "w") as f:
        f.write("something")
    subds.add('.')
    assert_repo_status(subds.path, annex=isinstance(subds.repo, AnnexRepo))
    # ensure modified subds is committed
    ds.rev_save()
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # now introduce a change downstairs
    subds.create('someotherds')
    assert_repo_status(subds.path, annex=isinstance(subds.repo, AnnexRepo))
    ok_(ds.repo.dirty)
    # and save via subdataset path
    ds.rev_save('subds')
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))


@with_tempfile()
def test_save_message_file(path):
    ds = RevolutionDataset(Dataset(path).create().path)
    with assert_raises(ValueError):
        ds.rev_save("blah", message="me", message_file="and me")

    create_tree(path, {"foo": "x",
                       "msg": "add foo"})
    ds.add("foo", save=False)
    ds.rev_save(message_file=op.join(ds.path, "msg"))
    eq_(ds.repo.repo.git.show("--format=%s", "--no-patch"),
        "add foo")


def test_renamed_file():
    @with_tempfile()
    def check_renamed_file(recursive, no_annex, path):
        ds = RevolutionDataset(Dataset(path).create(no_annex=no_annex).path)
        create_tree(path, {'old': ''})
        ds.add('old')
        ds.repo._git_custom_command(['old', 'new'], ['git', 'mv'])
        ds.rev_save(recursive=recursive)
        assert_repo_status(path)

    for recursive in False,:  #, True TODO when implemented
        for no_annex in True, False:
            yield check_renamed_file, recursive, no_annex


@with_tempfile(mkdir=True)
def test_subdataset_save(path):
    parent = RevolutionDataset(Dataset(path).create().path)
    sub = RevolutionDataset(parent.create('sub').path)
    assert_repo_status(parent.path)
    create_tree(parent.path, {
        "untracked": 'ignore',
        'sub': {
            "new": "wanted"}})
    sub.add('new')
    # defined state: one untracked, modified (but clean in itself) subdataset
    assert_repo_status(sub.path)
    assert_repo_status(parent.path, untracked=['untracked'], modified=['sub'])

    # `save sub` does not save the parent!!
    with chpwd(parent.path):
        assert_status('notneeded', save(dataset=sub.path))
    assert_repo_status(parent.path, untracked=['untracked'], modified=['sub'])
    # `save -u .` saves the state change in the subdataset,
    # but leaves any untracked content alone
    with chpwd(parent.path):
        assert_status('ok', parent.rev_save(updated=True))
    assert_repo_status(parent.path, untracked=['untracked'])

    # get back to the original modified state and check that -S behaves in
    # exactly the same way
    create_tree(parent.path, {
        'sub': {
            "new2": "wanted2"}})
    sub.add('new2')
    assert_repo_status(parent.path, untracked=['untracked'], modified=['sub'])


@known_failure_windows  # there are no symlinks in a POSIX sense
@with_tempfile(mkdir=True)
def test_symlinked_relpath(path):
    # initially ran into on OSX https://github.com/datalad/datalad/issues/2406
    os.makedirs(op.join(path, "origin"))
    dspath = op.join(path, "linked")
    os.symlink('origin', dspath)
    ds = RevolutionDataset(Dataset(dspath).create().path)
    create_tree(dspath, {
        "mike1": 'mike1',  # will be added from topdir
        "later": "later",  # later from within subdir
        "d": {
            "mike2": 'mike2', # to be added within subdir
        }
    })

    # in the root of ds
    with chpwd(dspath):
        ds.repo.add("mike1", git=True)
        ds.rev_save("committing", path="./mike1")

    # Let's also do in subdirectory
    with chpwd(op.join(dspath, 'd')):
        ds.repo.add("mike2", git=True)
        ds.rev_save("committing", path="./mike2")

        later = op.join(op.pardir, "later")
        ds.repo.add(later, git=True)
        ds.rev_save("committing", path=later)

    assert_repo_status(dspath)


@with_tempfile(mkdir=True)
def test_bf1886(path):
    parent = RevolutionDataset(Dataset(path).create().path)
    parent.create('sub')
    assert_repo_status(parent.path)
    # create a symlink pointing down to the subdataset, and add it
    os.symlink('sub', op.join(parent.path, 'down'))
    parent.add('down')
    assert_repo_status(parent.path)
    # now symlink pointing up
    os.makedirs(op.join(parent.path, 'subdir', 'subsubdir'))
    os.symlink(op.join(op.pardir, 'sub'), op.join(parent.path, 'subdir', 'up'))
    parent.add(op.join('subdir', 'up'))
    # 'all' to avoid the empty dir being listed
    assert_repo_status(parent.path, untracked_mode='all')
    # now symlink pointing 2xup, as in #1886
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup'))
    parent.add(op.join('subdir', 'subsubdir', 'upup'))
    assert_repo_status(parent.path)
    # simulatenously add a subds and a symlink pointing to it
    # create subds, but don't register it
    create(op.join(parent.path, 'sub2'))
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub2'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup2'))
    parent.add(['sub2', op.join('subdir', 'subsubdir', 'upup2')])
    assert_repo_status(parent.path)
    # full replication of #1886: the above but be in subdir of symlink
    # with no reference dataset
    create(op.join(parent.path, 'sub3'))
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub3'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup3'))
    # need to use absolute paths
    with chpwd(op.join(parent.path, 'subdir', 'subsubdir')):
        add([op.join(parent.path, 'sub3'),
             op.join(parent.path, 'subdir', 'subsubdir', 'upup3')])
    # here is where we need to disagree with the repo in #1886
    # we would not expect that `add` registers sub3 as a subdataset
    # of parent, because no reference dataset was given and the
    # command cannot decide (with the current semantics) whether
    # it should "add anything in sub3 to sub3" or "add sub3 to whatever
    # sub3 is in"
    assert_repo_status(parent.path, untracked=['sub3/'])


@with_tree({
    '1': '',
    '2': '',
    '3': ''})
def test_gh2043p1(path):
    # this tests documents the interim agreement on what should happen
    # in the case documented in gh-2043
    ds = RevolutionDataset(Dataset(path).create(force=True).path)
    ds.add('1')
    assert_repo_status(ds.path, untracked=['2', '3'])
    ds.unlock('1')
    assert_repo_status(ds.path, modified=['1'], untracked=['2', '3'])
    # save(.) should recommit unlocked file, and not touch anything else
    # this tests the second issue in #2043
    with chpwd(path):
        # only save modified bits
        save(path='.', updated=True)
    # state of the file (unlocked/locked) is committed as well, and the
    # test doesn't lock the file again
    assert_repo_status(ds.path, untracked=['2', '3'])
    with chpwd(path):
        # but when a path is given, anything that matches this path
        # untracked or not is added/saved
        save(path='.')
    # state of the file (unlocked/locked) is committed as well, and the
    # test doesn't lock the file again
    assert_repo_status(ds.path)


@with_tree({
    'staged': 'staged',
    'untracked': 'untracked'})
def test_bf2043p2(path):
    ds = RevolutionDataset(Dataset(path).create(force=True).path)
    ds.add('staged', save=False)
    assert_repo_status(ds.path, added=['staged'], untracked=['untracked'])
    # save -u does not commit untracked content
    # this tests the second issue in #2043
    with chpwd(path):
        save(updated=True)
    assert_repo_status(ds.path, untracked=['untracked'])
