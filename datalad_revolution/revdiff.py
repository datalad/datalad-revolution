# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Report differences between two states of a dataset (hierarchy)"""

__docformat__ = 'restructuredtext'


import logging

from datalad.interface.base import (
    build_doc,
)
from datalad.interface.utils import eval_results

from .dataset import (
    rev_datasetmethod,
)

from datalad.core.local.diff import (
    Diff,
)

lgr = logging.getLogger('datalad.revolution.diff')

import traceback
_tb = [t[2] for t in traceback.extract_stack()]
if '_generate_extension_api' not in _tb:  # pragma: no cover
    lgr.warn(
        "The module 'datalad_revolution.revdiff' is deprecated. "
        'The `RevDiff` class can be imported with: '
        '`from datalad.core.local.diff import Diff as RevDiff')


@build_doc
class RevDiff(Diff):
    """Report differences between two states of a dataset (hierarchy)

    The two to-be-compared states are given via to --from and --to options.
    These state identifiers are evaluated in the context of the (specified
    or detected) dataset. In case of a recursive report on a dataset
    hierarchy corresponding state pairs for any subdataset are determined
    from the subdataset record in the respective superdataset. Only changes
    recorded in a subdataset between these two states are reported, and so on.

    Any paths given as additional arguments will be used to constrain the
    difference report. As with Git's diff, it will not result in an error when
    a path is specified that does not exist on the filesystem.

    Reports are very similar to those of the `rev-status` command, with the
    distinguished content types and states being identical.
    """

    @staticmethod
    @rev_datasetmethod(name='rev_diff')
    @eval_results
    def __call__(
            fr='HEAD',
            to=None,
            path=None,
            dataset=None,
            annex=None,
            untracked='normal',
            recursive=False,
            recursion_limit=None):

        for r in Diff.__call__(
                fr=fr,
                to=to,
                path=path,
                dataset=dataset,
                annex=annex,
                untracked=untracked,
                recursive=recursive,
                recursion_limit=recursion_limit,
                result_renderer=None,
                on_failure="ignore",
                return_type='generator'):
            yield r
