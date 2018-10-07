# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test save command"""

import os.path as op
from six import iteritems

from datalad.tests.utils import (
    assert_in,
    assert_not_in,
    assert_status,
    create_tree,
    with_tempfile,
    with_testrepos,
    eq_,
    ok_,
    chpwd,
)

from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.dataset import RevolutionDataset
from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
from datalad.api import rev_save as save

from datalad_revolution.tests.utils import (
    assert_repo_status,
    get_convoluted_situation,
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
