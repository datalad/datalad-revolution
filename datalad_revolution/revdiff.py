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
import os.path as op
from six import (
    iteritems,
    text_type,
)
from datalad.dochelpers import exc_str
from datalad.utils import (
    assure_list,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.utils import eval_results

from datalad_revolution.dataset import (
    RevolutionDataset as Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
    resolve_path,
    path_under_dataset,
    get_dataset_root,
)
import datalad_revolution.utils as ut
from datalad_revolution.revstatus import (
    RevStatus,
)

from datalad.support.constraints import EnsureNone
from datalad.support.constraints import EnsureStr
from datalad.support.param import Parameter
from datalad.consts import PRE_INIT_COMMIT_SHA

lgr = logging.getLogger('datalad.revolution.diff')


@build_doc
class RevDiff(Interface):
    """
    """
    # make the custom renderer the default one, as the global default renderer
    # does not yield meaningful output for this command
    result_renderer = 'tailored'

    _params_ = dict(
        RevStatus._params_,
        fr=Parameter(
            args=("-f", "--from",),
            dest='fr',
            metavar="REVISION",
            doc="""original state to compare to, as given by any identifier
            that Git understands.""",
            nargs=1,
            constraints=EnsureStr()),
        to=Parameter(
            args=("-t", "--to",),
            metavar="REVISION",
            doc="""state to compare against the original state, as given by
            any identifier that Git understands. If none is specified,
            the state of the worktree will be used compared.""",
            nargs=1,
            constraints=EnsureStr() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='rev_diff')
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
        ds = require_dataset(
            dataset, check_installed=True, purpose='difference reporting')

        # convert cmdline args into plain labels
        if isinstance(fr, list):
            fr = fr[0]
        if isinstance(to, list):
            to = to[0]

        # TODO we cannot really perform any sorting of paths into subdatasets
        # or rejecting paths based on the state of the filesystem, as
        # we need to be able to compare with states that are not represented
        # in the worktree (anymore)

        # TODO we might want to move away from the single-pass+immediate-yield
        # paradigm for this command. If we gather all information first, we
        # could do post-processing and detect when a file (same gitsha, or same
        # key) was copied/moved from another dataset. Another command (e.g.
        # rev-save) could act on this information and also move/copy
        # availability information or at least enhance the respective commit
        # message with cross-dataset provenance info

        # cache to help avoid duplicate status queries
        content_info_cache = {}
        # TODO loop over results and dive into subdatasets with --recursive
        # do this inside the loop to go depth-first
        # https://github.com/datalad/datalad/issues/2161
        for res in _diff_ds(
                ds,
                fr,
                to,
                # TODO recode paths to repo path reference
                paths=None if not path else assure_list(path),
                untracked=untracked,
                cache=content_info_cache):
            res.update(
                refds=ds.path,
                logger=lgr,
                action='diff',
            )
            yield res


def _diff_ds(ds, fr, to, paths, untracked, cache):
    repo_path = ds.repo.pathobj
    try:
        lgr.debug("diff %s from '%s' to '%s'", ds, fr, to)
        diff_state = ds.repo.diffstatus(
            fr,
            to,
            paths=paths,
            untracked=untracked,
            ignore_submodules='other',
            _cache=cache)
    except ValueError as e:
        msg_tmpl = "reference '{}' invalid"
        # not looking for a debug repr of the exception, just the message
        estr = str(e)
        if msg_tmpl.format(fr) in estr or msg_tmpl.format(to) in estr:
            yield dict(
                path=ds.path,
                status='impossible',
                message=estr,
            )
            return

    for path, props in iteritems(diff_state):
        path = ds.pathobj / path.relative_to(repo_path)
        yield dict(
            props,
            path=str(path),
            # report the dataset path rather than the repo path to avoid
            # realpath/symlink issues
            parentds=ds.path,
            status='ok',
        )
        if props.get('type', None) == 'dataset':
            subds_state = props.get('state', None)
            if subds_state in ('clean', 'deleted'):
                # no need to look into the subdataset
                continue
            elif subds_state in ('added', 'modified'):
                # dive
                subds = Dataset(str(path))
                for r in _diff_ds(
                        subds,
                        # from before time or from the reported state
                        # TODO repo.diff() does not report the original state
                        PRE_INIT_COMMIT_SHA
                        if subds_state == 'added'
                        else props['prev_gitshasum'],
                        # to the last recorded state, or the worktree
                        None if to is None else props['gitshasum'],
                        # it should not be necessary to further mangle the path
                        # TOOD maybe kill those that are not underneath the
                        # dataset root
                        paths=paths,
                        untracked=untracked,
                        cache=cache):
                    yield r
            else:
                raise RuntimeError(
                    "Unexpected subdataset state '{}'. That sucks!".format(subds_state))
