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
import os.path as op
from six import iteritems
from collections import OrderedDict

from datalad.utils import (
    assure_list,
    get_dataset_root,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.utils import eval_results
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag,
)

from datalad_revolution.dataset import (
    RevolutionDataset as Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
    resolve_path,
)
import datalad_revolution.utils as ut

from datalad.support.constraints import EnsureNone
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureChoice
from datalad.support.param import Parameter

lgr = logging.getLogger('datalad.revolution.status')


def _yield_status(ds, paths, untracked, recursion_limit, queried):
    # take the datase that went in first
    repo_path = ds.repo.pathobj
    lgr.debug('query %s.status() for paths: %s', ds.repo, paths)
    for path, props in iteritems(ds.repo.status(
            paths=paths if paths else None,
            untracked=untracked,
            # TODO think about potential optimizations in case of
            # recursive processing, as this will imply a semi-recursive
            # look into subdatasets
            ignore_submodules='other')):
        cpath = ds.pathobj / path.relative_to(repo_path)
        yield dict(
            props,
            path=cpath,
        )
        queried.add(ds.pathobj)
        if recursion_limit and props.get('type', None) == 'dataset':
            subds = Dataset(str(cpath))
            if subds.is_installed():
                for r in _yield_status(
                        subds,
                        None,
                        untracked,
                        recursion_limit - 1,
                        queried):
                    yield r


@build_doc
class RevStatus(Interface):
    """
    """

    # make the custom renderer the default one, as the global default renderer
    # does not yield meaningful output for this command
    result_renderer = 'tailored'

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to query.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the input and/or the current working directory""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="""path to be evaluated""",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        untracked=Parameter(
            args=('--untracked',),
            constraints=EnsureChoice('no', 'normal', 'all'),
            doc="""If and how untracked content is reported when comparing
            a revision to the state of the work tree. 'no': no untracked files
            are reported; 'normal': untracked files and entire untracked
            directories are reported as such; 'all': report individual files
            even in fully untracked directories."""),
        recursive=recursion_flag,
        recursion_limit=recursion_limit)

    @staticmethod
    @datasetmethod(name='rev_status')
    @eval_results
    def __call__(
            path=None,
            dataset=None,
            untracked='normal',
            recursive=False,
            recursion_limit=None):
        ds = require_dataset(
            dataset, check_installed=True, purpose='status reporting')

        paths = []
        if path:
            # convert to pathobjs, decoding datalad path semantics
            path = [resolve_path(p, dataset) for p in assure_list(path)]

            # error on non-dataset paths
            for p in path:
                try:
                    relp = p.resolve().relative_to(ds.repo.pathobj)
                    paths.append(ds.pathobj / relp)
                except ValueError as e:
                    yield dict(
                        action='status',
                        path=p,
                        refds=ds.pathobj,
                        status='error',
                        message='path not underneath this dataset',
                        logger=lgr)
                    # exit early at the cost of not reporting potential
                    # further errors
                    return

        paths_by_ds = OrderedDict()
        if paths:
            for p in sorted(paths):
                root = ut.Path(get_dataset_root(p))
                ps = paths_by_ds.get(root, [])
                if p != root:
                    ps.append(p)
                paths_by_ds[root] = ps
        else:
            paths_by_ds[ds.pathobj] = None

        queried = set()
        while paths_by_ds:
            qdspath, qpaths = paths_by_ds.popitem(last=False)
            if qdspath in queried:
                continue
            qds = Dataset(str(qdspath))
            for r in _yield_status(
                    qds,
                    qpaths,
                    untracked,
                    recursion_limit
                    if recursion_limit is not None else -1
                    if recursive else 0,
                    queried):
                yield dict(
                    r,
                    refds=ds.pathobj,
                    action='status',
                    status='ok',
                )

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        from datalad.ui import ui
        if not res['status'] == 'ok' or res.get('state', None) == 'clean':
            # logging reported already
            return
        path = op.relpath(res['path'], start=res['refds']) \
            if res.get('refds', None) else res['path']
        type_ = res.get('type', res.get('type_src', ''))
        max_len = len('untracked(directory)')
        state_msg = '{}{}'.format(
            res['state'],
            '({})'.format(type_ if type_ else ''))
        ui.message('{fill}{state_msg}: {path}'.format(
            fill=' ' * max(0, max_len - len(state_msg)),
            state_msg=state_msg,
            path=path))
