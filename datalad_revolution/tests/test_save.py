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
from six import iteritems

from datalad.utils import (
    on_windows,
    assure_list,
)
from datalad.tests.utils import (
    assert_status,
    assert_in,
    assert_not_in,
    assert_raises,
    create_tree,
    with_tempfile,
    with_tree,
    with_testrepos,
    eq_,
    ok_,
    chpwd,
    known_failure_windows,
    OBSCURE_FILENAME,
)
from datalad.distribution.tests.test_add import tree_arg

from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
from datalad.api import (
    rev_save as save,
    rev_create as create,
    install,
)

from datalad_revolution.tests.utils import (
    assert_repo_status,
)


@with_testrepos('.*git.*', flavors=['clone'])
def test_save(path):

    ds = Dataset(path)

    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("something")

    ds.repo.add("new_file.tst", git=True)
    ok_(ds.repo.dirty)

    ds.rev_save(message="add a new file")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("modify")

    ok_(ds.repo.dirty)
    ds.rev_save(message="modified new_file.tst")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # save works without ds and files given in the PWD
    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("rapunzel")
    with chpwd(path):
        save(message="love rapunzel")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # and also without `-a` when things are staged
    with open(op.join(path, "new_file.tst"), "w") as f:
        f.write("exotic")
    ds.repo.add("new_file.tst", git=True)
    with chpwd(path):
        save(message="love marsians")
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    files = ['one.txt', 'two.txt']
    for fn in files:
        with open(op.join(path, fn), "w") as f:
            f.write(fn)

    ds.rev_save([op.join(path, f) for f in files])
    # superfluous call to save (alll saved it already), should not fail
    # but report that nothing was saved
    assert_status('notneeded', ds.rev_save(message="set of new files"))
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # create subdataset
    subds = ds.rev_create('subds')
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))
    # modify subds
    with open(op.join(subds.path, "some_file.tst"), "w") as f:
        f.write("something")
    subds.rev_save()
    assert_repo_status(subds.path, annex=isinstance(subds.repo, AnnexRepo))
    # ensure modified subds is committed
    ds.rev_save()
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))

    # now introduce a change downstairs
    subds.rev_create('someotherds')
    assert_repo_status(subds.path, annex=isinstance(subds.repo, AnnexRepo))
    ok_(ds.repo.dirty)
    # and save via subdataset path
    ds.rev_save('subds')
    assert_repo_status(path, annex=isinstance(ds.repo, AnnexRepo))


@with_tempfile()
def test_save_message_file(path):
    ds = Dataset(path).rev_create()
    with assert_raises(ValueError):
        ds.rev_save("blah", message="me", message_file="and me")

    create_tree(path, {"foo": "x",
                       "msg": "add foo"})
    ds.repo.add("foo")
    ds.rev_save(message_file=op.join(ds.path, "msg"))
    eq_(ds.repo.repo.git.show("--format=%s", "--no-patch"),
        "add foo")


def test_renamed_file():
    @with_tempfile()
    def check_renamed_file(recursive, no_annex, path):
        ds = Dataset(path).rev_create(no_annex=no_annex)
        create_tree(path, {'old': ''})
        ds.repo.add('old')
        ds.repo._git_custom_command(['old', 'new'], ['git', 'mv'])
        ds.rev_save(recursive=recursive)
        assert_repo_status(path)

    for recursive in False,:  #, True TODO when implemented
        for no_annex in True, False:
            yield check_renamed_file, recursive, no_annex


@with_tempfile(mkdir=True)
def test_subdataset_save(path):
    parent = Dataset(path).rev_create()
    sub = parent.rev_create('sub')
    assert_repo_status(parent.path)
    create_tree(parent.path, {
        "untracked": 'ignore',
        'sub': {
            "new": "wanted"}})
    sub.rev_save('new')
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
    sub.rev_save('new2')
    assert_repo_status(parent.path, untracked=['untracked'], modified=['sub'])


@known_failure_windows  # there are no symlinks in a POSIX sense
@with_tempfile(mkdir=True)
def test_symlinked_relpath(path):
    # initially ran into on OSX https://github.com/datalad/datalad/issues/2406
    os.makedirs(op.join(path, "origin"))
    dspath = op.join(path, "linked")
    os.symlink('origin', dspath)
    ds = Dataset(dspath).rev_create()
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
        ds.rev_save(message="committing", path="./mike1")

    # Let's also do in subdirectory
    with chpwd(op.join(dspath, 'd')):
        ds.repo.add("mike2", git=True)
        ds.rev_save(message="committing", path="./mike2")

        later = op.join(op.pardir, "later")
        ds.repo.add(later, git=True)
        ds.rev_save(message="committing", path=later)

    assert_repo_status(dspath)


@known_failure_windows  # there are no symlinks in a POSIX sense
@with_tempfile(mkdir=True)
def test_bf1886(path):
    parent = Dataset(path).rev_create()
    parent.rev_create('sub')
    assert_repo_status(parent.path)
    # create a symlink pointing down to the subdataset, and add it
    os.symlink('sub', op.join(parent.path, 'down'))
    parent.rev_save('down')
    assert_repo_status(parent.path)
    # now symlink pointing up
    os.makedirs(op.join(parent.path, 'subdir', 'subsubdir'))
    os.symlink(op.join(op.pardir, 'sub'), op.join(parent.path, 'subdir', 'up'))
    parent.rev_save(op.join('subdir', 'up'))
    # 'all' to avoid the empty dir being listed
    assert_repo_status(parent.path, untracked_mode='all')
    # now symlink pointing 2xup, as in #1886
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup'))
    parent.rev_save(op.join('subdir', 'subsubdir', 'upup'))
    assert_repo_status(parent.path)
    # simulatenously add a subds and a symlink pointing to it
    # create subds, but don't register it
    create(op.join(parent.path, 'sub2'))
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub2'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup2'))
    parent.rev_save(['sub2', op.join('subdir', 'subsubdir', 'upup2')])
    assert_repo_status(parent.path)
    # full replication of #1886: the above but be in subdir of symlink
    # with no reference dataset
    create(op.join(parent.path, 'sub3'))
    os.symlink(
        op.join(op.pardir, op.pardir, 'sub3'),
        op.join(parent.path, 'subdir', 'subsubdir', 'upup3'))
    # need to use absolute paths
    with chpwd(op.join(parent.path, 'subdir', 'subsubdir')):
        save([op.join(parent.path, 'sub3'),
              op.join(parent.path, 'subdir', 'subsubdir', 'upup3')])
    assert_repo_status(parent.path)


@with_tree({
    '1': '',
    '2': '',
    '3': ''})
def test_gh2043p1(path):
    # this tests documents the interim agreement on what should happen
    # in the case documented in gh-2043
    ds = Dataset(path).rev_create(force=True)
    ds.rev_save('1')
    assert_repo_status(ds.path, untracked=['2', '3'])
    ds.unlock('1')
    assert_repo_status(
        ds.path,
        # on windows we are in an unlocked branch by default, hence
        # we would see no change
        modified=[] if on_windows else ['1'],
        untracked=['2', '3'])
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
    ds = Dataset(path).rev_create(force=True)
    ds.repo.add('staged')
    assert_repo_status(ds.path, added=['staged'], untracked=['untracked'])
    # save -u does not commit untracked content
    # this tests the second issue in #2043
    with chpwd(path):
        save(updated=True)
    assert_repo_status(ds.path, untracked=['untracked'])


@with_tree(**tree_arg)
def test_add_files(path):
    ds = Dataset(path).rev_create(force=True)

    test_list_1 = ['test_annex.txt']
    test_list_2 = ['test.txt']
    test_list_3 = ['test1.dat', 'test2.dat']
    test_list_4 = [op.join('dir', 'testindir'),
                   op.join('dir', OBSCURE_FILENAME)]

    for arg in [(test_list_1[0], False),
                (test_list_2[0], True),
                (test_list_3, False),
                (test_list_4, False)]:
        # special case 4: give the dir:
        if arg[0] == test_list_4:
            result = ds.rev_save('dir', to_git=arg[1])
            status = ds.repo.annexstatus(['dir'])
        else:
            result = ds.rev_save(
                arg[0], to_git=arg[1],
                result_xfm='relpaths',
                return_type='item-or-list')
            # order depends on how annex processes it, so let's sort
            eq_(sorted(result), sorted(arg[0]))
            status = ds.repo.get_content_annexinfo(assure_list(arg[0]))
        for f, p in iteritems(status):
            if arg[1]:
                assert p.get('key', None) is None, f
            else:
                assert p.get('key', None) is not None, f


@with_tree(**tree_arg)
@with_tempfile(mkdir=True)
def test_add_subdataset(path, other):
    subds = create(op.join(path, 'dir'), force=True)
    ds = create(path, force=True)
    ok_(subds.repo.dirty)
    ok_(ds.repo.dirty)
    assert_not_in('dir', ds.subdatasets(result_xfm='relpaths'))
    # "add everything in subds to subds"
    save(dataset=subds.path)
    assert_repo_status(subds.path)
    assert_not_in('dir', ds.subdatasets(result_xfm='relpaths'))
    # but with a base directory we add the dataset subds as a subdataset
    # to ds
    ds.rev_save(subds.path)
    assert_in('dir', ds.subdatasets(result_xfm='relpaths'))
    #  create another one
    other = create(other)
    # install into superdataset, but don't add
    other_clone = install(source=other.path, path=op.join(ds.path, 'other'))
    ok_(other_clone.is_installed)
    assert_not_in('other', ds.subdatasets(result_xfm='relpaths'))
    # now add, it should pick up the source URL
    ds.rev_save('other')
    # and that is why, we can reobtain it from origin
    ds.uninstall('other')
    ok_(other_clone.is_installed)
    ds.get('other')
    ok_(other_clone.is_installed)


@with_tree(tree={
    'file.txt': 'some text',
    'empty': '',
    'file2.txt': 'some text to go to annex',
    '.gitattributes': '* annex.largefiles=(not(mimetype=text/*))'}
)
def test_add_mimetypes(path):
    ds = Dataset(path).rev_create(force=True)
    ds.repo.add('.gitattributes')
    ds.repo.commit('added attributes to git explicitly')
    # now test that those files will go into git/annex correspondingly
    __not_tested__ = ds.rev_save(['file.txt', 'empty'])
    assert_repo_status(path, untracked=['file2.txt'])
    # But we should be able to force adding file to annex when desired
    ds.rev_save('file2.txt', to_git=False)
    # check annex file status
    annexinfo = ds.repo.get_content_annexinfo()
    for path, in_annex in (
           # Empty one considered to be  application/octet-stream
           # i.e. non-text
           ('empty', True),
           ('file.txt', False),
           ('file2.txt', True)):
        p = ds.pathobj / path
        assert_in(p, annexinfo)
        if in_annex:
            assert_in('key', annexinfo[p], p)
        else:
            assert_not_in('key', annexinfo[p], p)


@with_tempfile(mkdir=True)
def test_gh1597_simpler(path):
    ds = Dataset(path).rev_create()
    # same goes for .gitattributes
    with open(op.join(ds.path, '.gitignore'), 'a') as f:
        f.write('*.swp\n')
    ds.rev_save('.gitignore')
    assert_repo_status(ds.path)
    # put .gitattributes in some subdir and add all, should also go into Git
    attrfile = op.join ('subdir', '.gitattributes')
    ds.repo.set_gitattributes(
        [('*', dict(mycustomthing='this'))],
        attrfile)
    assert_repo_status(ds.path, untracked=[attrfile], untracked_mode='all')
    ds.rev_save()
    assert_repo_status(ds.path)
    # no annex key, not in annex
    assert_not_in(
        'key',
        ds.repo.get_content_annexinfo([attrfile]).popitem()[1])
