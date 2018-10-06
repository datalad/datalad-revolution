# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test save command"""

from six import iteritems

from datalad.tests.utils import (
    assert_in,
    assert_not_in,
    create_tree,
    with_tempfile,
    eq_,
)

from datalad_revolution.dataset import RevolutionDataset as Dataset
from datalad_revolution.dataset import RevolutionDataset
from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
from datalad_revolution.revsave import RevSave as Save

from datalad_revolution.tests.utils import (
    assert_repo_status,
    get_convoluted_situation,
)


@with_tempfile
def test_save_basics(path):
    Save()

