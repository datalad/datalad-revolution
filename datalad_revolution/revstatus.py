# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Report status of a dataset (hierarchy)'s work tree"""

__docformat__ = 'restructuredtext'


import logging

from datalad.interface.base import (
    build_doc,
)
from datalad.interface.utils import eval_results
from .dataset import (
    rev_datasetmethod,
)

from datalad.core.local.status import Status

lgr = logging.getLogger('datalad.revolution.status')


# Note: We're keeping this docstring around because if we used core's
# it would say "datalad status ...".


@build_doc
class RevStatus(Status):
    """Report on the state of dataset content.

    This is an analog to `git status` that is simultaneously crippled and more
    powerful. It is crippled, because it only supports a fraction of the
    functionality of its counter part and only distinguishes a subset of the
    states that Git knows about. But it is also more powerful as it can handle
    status reports for a whole hierarchy of datasets, with the ability to
    report on a subset of the content (selection of paths) across any number
    of datasets in the hierarchy.

    *Path conventions*

    All reports are guaranteed to use absolute paths that are underneath the
    given or detected reference dataset, regardless of whether query paths are
    given as absolute or relative paths (with respect to the working directory,
    or to the reference dataset, when such a dataset is given explicitly).
    Moreover, so-called "explicit relative paths" (i.e. paths that start with
    '.' or '..') are also supported, and are interpreted as relative paths with
    respect to the current working directory regardless of whether a reference
    dataset with specified.

    When it is necessary to address a subdataset record in a superdataset
    without causing a status query for the state _within_ the subdataset
    itself, this can be achieved by explicitly providing a reference dataset
    and the path to the root of the subdataset like so::

      datalad rev-status --dataset . subdspath

    In contrast, when the state of the subdataset within the superdataset is
    not relevant, a status query for the content of the subdataset can be
    obtained by adding a trailing path separator to the query path (rsync-like
    syntax)::

      datalad rev-status --dataset . subdspath/

    When both aspects are relevant (the state of the subdataset content
    and the state of the subdataset within the superdataset), both queries
    can be combined::

      datalad rev-status --dataset . subdspath subdspath/

    When performing a recursive status query, both status aspects of subdataset
    are always included in the report.


    *Content types*

    The following content types are distinguished:

    - 'dataset' -- any top-level dataset, or any subdataset that is properly
      registered in superdataset
    - 'directory' -- any directory that does not qualify for type 'dataset'
    - 'file' -- any file, or any symlink that is placeholder to an annexed
      file
    - 'symlink' -- any symlink that is not used as a placeholder for an annexed
      file

    *Content states*

    The following content states are distinguished:

    - 'clean'
    - 'added'
    - 'modified'
    - 'deleted'
    - 'untracked'
    """

    @staticmethod
    @rev_datasetmethod(name='rev_status')
    @eval_results
    def __call__(
            path=None,
            dataset=None,
            annex=None,
            untracked='normal',
            recursive=False,
            recursion_limit=None):
        for r in Status.__call__(
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
