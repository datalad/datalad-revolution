# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""High-level interface for dataset creation

"""

import os
import logging
import random
import uuid
from six import iteritems

from os import listdir
import os.path as op
from os.path import isdir
from os.path import join as opj

from datalad import cfg
from datalad import _seed
from datalad.interface.base import Interface
from datalad.interface.annotate_paths import AnnotatePaths
from datalad.interface.utils import eval_results
from datalad.interface.base import build_doc
from datalad.interface.common_opts import git_opts
from datalad.interface.common_opts import annex_opts
from datalad.interface.common_opts import annex_init_opts
from datalad.interface.common_opts import location_description
from datalad.interface.common_opts import nosave_opt
from datalad.interface.common_opts import shared_access_opt
from datalad.interface.results import ResultXFM
from datalad.support.constraints import EnsureStr
from datalad.support.constraints import EnsureNone
from datalad.support.constraints import EnsureKeyChoice
from datalad.support.constraints import EnsureDType
from datalad.support.param import Parameter
from datalad.utils import getpwd
from datalad.utils import get_dataset_root
from datalad.distribution.subdatasets import Subdatasets

# required to get the binding of `add` as a dataset method
from datalad.distribution.add import Add

from datalad_revolution.dataset import (
    RevolutionDataset as Dataset,
    datasetmethod,
    EnsureDataset,
)
from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo
import datalad_revolution.utils as ut


__docformat__ = 'restructuredtext'

lgr = logging.getLogger('datalad.distribution.create')


# TODO for now carry a copy of this one, until datalad-core returns
# next-gen RevolutionDataset instances
class YieldDatasets(ResultXFM):
    """Result transformer to return a Dataset instance from matching result.

    If the `success_only` flag is given only dataset with 'ok' or 'notneeded'
    status are returned'.

    `None` is returned for any other result.
    """
    def __init__(self, success_only=False):
        self.success_only = success_only

    def __call__(self, res):
        if res.get('type', None) == 'dataset':
            if not self.success_only or \
                    res.get('status', None) in ('ok', 'notneeded'):
                ds = Dataset(res['path'])
                return ds
        else:
            lgr.debug('rejected by return value configuration: %s', res)



@build_doc
class RevCreate(Interface):
    """Create a new dataset from scratch.

    This command initializes a new :term:`dataset` at a given location, or the
    current directory. The new dataset can optionally be registered in an
    existing :term:`superdataset` (the new dataset's path needs to be located
    within the superdataset for that, and the superdataset needs to be given
    explicitly). It is recommended to provide a brief description to label
    the dataset's nature *and* location, e.g. "Michael's music on black
    laptop". This helps humans to identify data locations in distributed
    scenarios.  By default an identifier comprised of user and machine name,
    plus path will be generated.

    This command only creates a new dataset, it does not add any content to it,
    even if the target directory already contains additional files or
    directories.

    Plain Git repositories can be created via the [PY: `no_annex` PY][CMD: --no-annex CMD] flag.
    However, the result will not be a full dataset, and, consequently,
    not all features are supported (e.g. a description).

    || REFLOW >>
    To create a local version of a remote dataset use the
    :func:`~datalad.api.install` command instead.
    << REFLOW ||

    .. note::
      Power-user info: This command uses :command:`git init` and
      :command:`git annex init` to prepare the new dataset. Registering to a
      superdataset is performed via a :command:`git submodule add` operation
      in the discovered superdataset.
    """

    # in general this command will yield exactly one result
    return_type = 'item-or-list'
    # in general users expect to get an instance of the created dataset
    # TODO switch back
    # result_xfm = 'datasets'
    result_xfm = YieldDatasets()
    # result filter
    result_filter = EnsureKeyChoice('action', ('create',)) & \
                    EnsureKeyChoice('status', ('ok', 'notneeded'))

    _params_ = dict(
        path=Parameter(
            args=("path",),
            metavar='PATH',
            doc="""path where the dataset shall be created, directories
            will be created as necessary. If no location is provided, a dataset
            will be created in the current working directory. Either way the
            command will error if the target directory is not empty.
            Use `force` to create a dataset in a non-empty directory.""",
            nargs='?',
            # put dataset 2nd to avoid useless conversion
            constraints=EnsureStr() | EnsureDataset() | EnsureNone()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            metavar='PATH',
            doc="""specify the dataset to perform the create operation on. If
            a dataset is given, a new subdataset will be created in it.""",
            constraints=EnsureDataset() | EnsureNone()),
        force=Parameter(
            args=("-f", "--force",),
            doc="""enforce creation of a dataset in a non-empty directory""",
            action='store_true'),
        description=location_description,
        # TODO could move into cfg_annex plugin
        no_annex=Parameter(
            args=("--no-annex",),
            doc="""if set, a plain Git repository will be created without any
            annex""",
            action='store_true'),
        text_no_annex=Parameter(
            args=("--text-no-annex",),
            doc="""if set, all text files in the future would be added to Git,
            not annex. Achieved by adding an entry to `.gitattributes` file. See
            http://git-annex.branchable.com/tips/largefiles/ and `no_annex`
            DataLad plugin to establish even more detailed control over which
            files are placed under annex control.""",
            action='store_true'),
        save=nosave_opt,
        # TODO could move into cfg_annex plugin
        annex_version=Parameter(
            args=("--annex-version",),
            doc="""select a particular annex repository version. The
            list of supported versions depends on the available git-annex
            version. This should be left untouched, unless you know what
            you are doing""",
            constraints=EnsureDType(int) | EnsureNone()),
        # TODO could move into cfg_annex plugin
        annex_backend=Parameter(
            args=("--annex-backend",),
            constraints=EnsureStr() | EnsureNone(),
            # not listing choices here on purpose to avoid future bugs
            doc="""set default hashing backend used by the new dataset.
            For a list of supported backends see the git-annex
            documentation. The default is optimized for maximum compatibility
            of datasets across platforms (especially those with limited
            path lengths)"""),
        # TODO could move into cfg_metadata plugin
        native_metadata_type=Parameter(
            args=('--native-metadata-type',),
            metavar='LABEL',
            action='append',
            constraints=EnsureStr() | EnsureNone(),
            doc="""Metadata type label. Must match the name of the respective
            parser implementation in DataLad (e.g. "xmp").[CMD:  This option
            can be given multiple times CMD]"""),
        # TODO could move into cfg_access/permissions plugin
        shared_access=shared_access_opt,
        git_opts=git_opts,
        annex_opts=annex_opts,
        annex_init_opts=annex_init_opts,
        fake_dates=Parameter(
            args=('--fake-dates',),
            action='store_true',
            doc="""Configure the repository to use fake dates. The date for a
            new commit will be set to one second later than the latest commit
            in the repository. This can be used to anonymize dates."""),
    )

    @staticmethod
    @datasetmethod(name='rev_create')
    @eval_results
    def __call__(
            path=None,
            force=False,
            description=None,
            dataset=None,
            no_annex=False,
            save=True,
            annex_version=None,
            annex_backend='MD5E',
            native_metadata_type=None,
            shared_access=None,
            git_opts=None,
            annex_opts=None,
            annex_init_opts=None,
            text_no_annex=None,
            fake_dates=False
    ):

        # two major cases
        # 1. we got a `dataset` -> we either want to create it (path is None),
        #    or another dataset in it (path is not None)
        # 2. we got no dataset -> we want to create a fresh dataset at the
        #    desired location, either at `path` or PWD
        if path and dataset:
            # Given a path and a dataset (path) not pointing to installed
            # dataset
            if not dataset.is_installed():
                msg = "No installed dataset at %s found." % dataset.path
                dsroot = get_dataset_root(dataset.path)
                if dsroot:
                    msg += " If you meant to add to the %s dataset, use that path " \
                           "instead but remember that if dataset is provided, " \
                           "relative paths are relative to the top of the " \
                           "dataset." % dsroot
                raise ValueError(msg)

        # sanity check first
        if git_opts:
            lgr.warning(
                "`git_opts` argument is presently ignored, please complain!")
        if no_annex:
            if description:
                raise ValueError("Incompatible arguments: cannot specify "
                                 "description for annex repo and declaring "
                                 "no annex repo.")
            if annex_opts:
                raise ValueError("Incompatible arguments: cannot specify "
                                 "options for annex and declaring no "
                                 "annex repo.")
            if annex_init_opts:
                raise ValueError("Incompatible arguments: cannot specify "
                                 "options for annex init and declaring no "
                                 "annex repo.")

        if not isinstance(force, bool):
            raise ValueError("force should be bool, got %r.  Did you mean to provide a 'path'?" % force)
        annotated_paths = AnnotatePaths.__call__(
            # nothing given explicitly, assume create fresh right here
            path=path if path else getpwd() if dataset is None else None,
            dataset=dataset,
            recursive=False,
            action='create',
            # we need to know whether we have to check for potential
            # subdataset collision
            force_parentds_discovery=False,
            force_subds_discovery=False,
            force_no_revision_change_discovery=True,
            force_untracked_discovery=False,
            # it is absolutely OK to have something that does not exist
            unavailable_path_status='',
            unavailable_path_msg=None,
            # if we have a dataset given that actually exists, we want to
            # fail if the requested path is not in it
            nondataset_path_status='error' \
                if isinstance(dataset, Dataset) and dataset.is_installed() else '',
            on_failure='ignore')
        path = None
        for r in annotated_paths:
            if r['status']:
                # this is dealt with already
                yield r
                continue
            if path is not None:
                raise ValueError("`create` can only handle single target path or dataset")
            path = r

        if len(annotated_paths) and path is None:
            # we got something, we complained already, done
            return

        # we know that we need to create a dataset at `path`
        assert(path is not None)

        # prep for yield
        path.update({'logger': lgr, 'type': 'dataset'})
        # just discard, we have a new story to tell
        path.pop('message', None)

        # try to locate a parent dataset
        # we want to know this (irrespective of whether we plan on adding
        # this new dataset to a parent) in order to avoid conflicts with
        # a potentially absent/uninstalled subdataset of the parent
        # in this location
        # it will cost some filesystem traversal though...
        parentds_path = get_dataset_root(
            op.normpath(op.join(path['path'], os.pardir)))
        if parentds_path:
            # we cannot get away with a simple
            # GitRepo.get_content_info(), as we need to detect
            # uninstalled/added subdatasets too
            subds_status = {k for k, v in iteritems(
                GitRepo(parentds_path).status(untracked='no'))
                if v.get('type', None) == 'dataset'}
            check_paths = [ut.Path(path['path'])]
            check_paths.extend(ut.Path(path['path']).parents)
            if any(p in subds_status for p in check_paths):
                conflict = [p for p in check_paths if p in subds_status]
                path.update({
                    'status': 'error',
                    'message': (
                        'collision with %s (dataset) in dataset %s',
                        str(conflict[0]),
                        parentds_path)})
                yield path
                return

        # TODO here we need a further test that if force=True, we need to look if
        # there is a superdataset (regardless of whether we want to create a
        # subdataset or not), and if that superdataset tracks anything within
        # this directory -- if so, we need to stop right here and whine, because
        # the result of creating a repo here will produce an undesired mess

        if git_opts is None:
            git_opts = {}
        if shared_access:
            # configure `git --shared` value
            git_opts['shared'] = shared_access

        # important to use the given Dataset object to avoid spurious ID
        # changes with not-yet-materialized Datasets
        tbds = dataset if isinstance(dataset, Dataset) and dataset.path == path['path'] \
            else Dataset(path['path'])

        # don't create in non-empty directory without `force`:
        if isdir(tbds.path) and listdir(tbds.path) != [] and not force:
            path.update({
                'status': 'error',
                'message':
                    'will not create a dataset in a non-empty directory, use '
                    '`force` option to ignore'})
            yield path
            return

        # stuff that we create and want to have tracked with git (not annex)
        add_to_git = {}

        if no_annex:
            lgr.info("Creating a new git repo at %s", tbds.path)
            GitRepo(
                tbds.path,
                url=None,
                create=True,
                git_opts=git_opts,
                fake_dates=fake_dates)
        else:
            # always come with annex when created from scratch
            lgr.info("Creating a new annex repo at %s", tbds.path)
            tbrepo = AnnexRepo(
                tbds.path,
                url=None,
                create=True,
                # do not set backend here, to avoid a dedicated commit
                backend=None,
                version=annex_version,
                description=description,
                git_opts=git_opts,
                annex_opts=annex_opts,
                annex_init_opts=annex_init_opts,
                fake_dates=fake_dates
            )
            # set the annex backend in .gitattributes as a staged change
            tbrepo.set_default_backend(
                annex_backend, persistent=True, commit=False)
            add_to_git[tbds.repo.pathobj / '.gitattributes'] = {
                'type': 'file',
                'state': 'added'}

            if text_no_annex:
                attrs = tbrepo.get_gitattributes('.')
                # some basic protection against useless duplication
                # on rerun with --force
                if not attrs.get('.', {}).get('annex.largefiles', None) == '(not(mimetype=text/*))':
                    tbrepo.set_gitattributes([
                        ('*', {'annex.largefiles': '(not(mimetype=text/*))'})])
                    add_to_git[tbrepo.pathobj / '.gitattributes'] = {
                        'type': 'file',
                        'state': 'untracked'}

        if native_metadata_type is not None:
            if not isinstance(native_metadata_type, list):
                native_metadata_type = [native_metadata_type]
            for nt in native_metadata_type:
                tbds.config.add(
                    'datalad.metadata.nativetype', nt,
                    reload=False)

        # record an ID for this repo for the afterlife
        # to be able to track siblings and children
        id_var = 'datalad.dataset.id'
        if id_var in tbds.config:
            # make sure we reset this variable completely, in case of a re-create
            tbds.config.unset(id_var, where='dataset')

        if _seed is None:
            # just the standard way
            uuid_id = uuid.uuid1().urn.split(':')[-1]
        else:
            # Let's generate preseeded ones
            uuid_id = str(uuid.UUID(int=random.getrandbits(128)))
        tbds.config.add(
            id_var,
            tbds.id if tbds.id is not None else uuid_id,
            where='dataset')

        # must use the repo.pathobj as this will have resolved symlinks
        add_to_git[tbds.repo.pathobj / '.datalad'] = {
            'type': 'directory',
            'state': 'untracked'}

        # make sure that v6 annex repos never commit content under .datalad
        attrs_cfg = (
            ('config', 'annex.largefiles', 'nothing'),
            ('metadata/aggregate*', 'annex.largefiles', 'nothing'),
            ('metadata/objects/**', 'annex.largefiles',
             '({})'.format(cfg.obtain(
                 'datalad.metadata.create-aggregate-annex-limit'))))
        attrs = tbds.repo.get_gitattributes(
            [op.join('.datalad', i[0]) for i in attrs_cfg])
        set_attrs = []
        for p, k, v in attrs_cfg:
            if not attrs.get(
                    op.join('.datalad', p), {}).get(k, None) == v:
                set_attrs.append((p, {k: v}))
        if set_attrs:
            tbds.repo.set_gitattributes(
                set_attrs,
                attrfile=op.join('.datalad', '.gitattributes'))

        # prevent git annex from ever annexing .git* stuff (gh-1597)
        attrs = tbds.repo.get_gitattributes('.git')
        if not attrs.get('.git', {}).get('annex.largefiles', None) == 'nothing':
            tbds.repo.set_gitattributes([
                ('**/.git*', {'annex.largefiles': 'nothing'})])
            # must use the repo.pathobj as this will have resolved symlinks
            add_to_git[tbds.repo.pathobj / '.gitattributes'] = {
                'type': 'file',
                'state': 'untracked'}

        # save everything, we need to do this now and cannot merge with the
        # call below, because we may need to add this subdataset to a parent
        # but cannot until we have a first commit
        tbds.repo.save(
            message='[DATALAD] new dataset',
            git=True,
            # we have to supply our own custom status, as the repo does
            # not have a single commit yet and the is no HEAD reference
            # TODO make `GitRepo.status()` robust to this state.
            _status=add_to_git,
        )

        # the next only makes sense if we saved the created dataset,
        # otherwise we have no committed state to be registered
        # in the parent
        if isinstance(dataset, Dataset) and dataset.path != tbds.path:
            # we created a dataset in another dataset
            # -> make submodule
            # TODO this will not be able to handle saving
            # a subdataset that is itself in a subdataset
            # of `dataset`, yet...
            for r in dataset.repo.save_(
                    paths=[tbds.path],
            ):
                yield r

        path.update({'status': 'ok'})
        yield path

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        from datalad.ui import ui
        if res.get('action', None) == 'create' and \
               res.get('status', None) == 'ok' and \
               res.get('type', None) == 'dataset':
            ui.message("Created dataset at {}.".format(res['path']))
        else:
            ui.message("Nothing was created")