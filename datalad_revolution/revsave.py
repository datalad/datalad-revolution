# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Interface to add content, and save modifications to a dataset

"""

__docformat__ = 'restructuredtext'

import logging

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag,
    save_message_opt,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone,
)

from datalad_revolution.dataset import (
    EnsureDataset,
    datasetmethod,
)

lgr = logging.getLogger('datalad.revolution.save')


@build_doc
class RevSave(Interface):
    """Save the current state of a dataset

    Saving the state of a dataset records changes that have been made to it.
    This change record is annotated with a user-provided description.
    Optionally, an additional tag, such as a version, can be assigned to the
    saved state. Such tag enables straightforward retrieval of past versions at
    a later point in time.

    Examples:

      Save any content underneath the current directory, without altering
      any potential subdataset (use --recursive for that)::

        % datalad save .

      Save any modification of known dataset content, but leave untracked
      files (e.g. temporary files) untouched::

        % dataset save -d <path_to_dataset>

      Tag the most recent saved state of a dataset::

        % dataset save -d <path_to_dataset> --version-tag bestyet
    """

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""specify the dataset to save""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar='PATH',
            doc="""path/name of the dataset component to save. If given, only
            changes made to those components are recorded in the new state.""",
            nargs='*',
            constraints=EnsureStr() | EnsureNone()),
        message=save_message_opt,
        message_file=Parameter(
            args=("-F", "--message-file"),
            doc="""take the commit message from this file. This flag is
            mutually exclusive with -m.""",
            constraints=EnsureStr() | EnsureNone()),
        version_tag=Parameter(
            args=("--version-tag",),
            metavar='ID',
            doc="""an additional marker for that state.""",
            constraints=EnsureStr() | EnsureNone()),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
    )

    @staticmethod
    @datasetmethod(name='rev_save')
    @eval_results
    def __call__(message=None, path=None, dataset=None,
                 version_tag=None,
                 recursive=False, recursion_limit=None,
                 message_file=None
                 ):
        refds_path = Interface.get_refds_path(dataset)

        if message and message_file:
            yield get_status_dict(
                'save',
                status='error',
                path=refds_path,
                message="Both a message and message file were specified",
                logger=lgr)
            return

        if message_file:
            with open(message_file) as mfh:
                message = mfh.read()
