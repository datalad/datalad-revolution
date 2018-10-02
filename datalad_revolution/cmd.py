__docformat__ = 'restructuredtext'

import os.path as op
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.distribution.dataset import datasetmethod
from datalad.interface.utils import eval_results
from datalad.interface.results import get_status_dict
from datalad.support.param import Parameter
from datalad.support.constraints import EnsureNone

from datalad_revolution.dataset import (
    EnsureDataset,
    RevolutionDataset,
)

# decoration auto-generates standard help
@build_doc
# all commands must be derived from Interface
class RevolutionCommand(Interface):
    # first docstring line is used a short description in the cmdline help
    # the rest is put in the verbose help and manpage
    """Short description of the command

    Long description of arbitrary volume.
    """
    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""specify a dataset""",
            constraints=EnsureDataset() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='rev_cmd')
    @eval_results
    def __call__(dataset=None):
        ds = EnsureDataset()(dataset)
        assert isinstance(ds, RevolutionDataset)
        from datalad.tests.utils import assert_raises
        assert_raises(NotImplementedError, ds.repo.dirty)
        yield get_status_dict(
            action='demo',
            path=op.abspath(op.curdir),
            status='ok',
        )
