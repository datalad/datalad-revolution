# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Set and query metadata of datasets and their components"""

__docformat__ = 'restructuredtext'


import glob
import logging
import os
import os.path as op
from six import (
    iteritems,
    text_type,
)
from datalad.interface.base import Interface
from datalad.interface.results import (
    get_status_dict,
    success_status_map,
)
from datalad.interface.utils import eval_results
from datalad.interface.base import build_doc
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureChoice,
)
from datalad.support.param import Parameter
import datalad.support.ansi_colors as ac
from datalad.support.json_py import (
    load as json_load,
    load_stream as json_streamload,
)
from datalad.interface.common_opts import (
    recursion_flag,
)
from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
    rev_resolve_path,
)
import datalad.utils as ut
from datalad.utils import (
    assure_list,
)
from datalad.ui import ui
from . import (
    aggregate_layout_version,
    location_keys,
)

lgr = logging.getLogger('datalad.metadata.metadata')


def get_ds_aggregate_db_locations(dspath, version='default', warn_absent=True):
    """Returns the location of a dataset's aggregate metadata DB

    Parameters
    ----------
    dspath : Path
      Path to a dataset to query
    version : str
      DataLad aggregate metadata layout version. At the moment only a single
      version exists. 'default' will return the locations for the current
      default layout version.
    warn_absent : bool
      If True, warn if the desired DB version is not present and give hints on
      what else is available. This is useful when using this function from
      a user-facing command.

    Returns
    -------
    db_location, db_object_base_path
      Absolute paths to the DB itself, and to the basepath to resolve relative
      object references in the database. Either path may not exist in the
      queried dataset.
    """
    layout_version = aggregate_layout_version \
        if version == 'default' else version

    agginfo_relpath_template = op.join(
        '.datalad',
        'metadata',
        'aggregate_v{}.json')
    agginfo_relpath = agginfo_relpath_template.format(layout_version)
    info_fpath = dspath / agginfo_relpath
    agg_base_path = info_fpath.parent
    # not sure if this is the right place with these check, better move then to
    # a higher level
    if warn_absent and not info_fpath.exists():  # pragma: no cover
        # legacy code from a time when users could not be aware of whether
        # they were doing metadata extraction, or querying aggregated metadata
        # nowadays, and error is triggered long before it reaches this code
        if version == 'default':
            from datalad.consts import (
                OLDMETADATA_DIR,
                OLDMETADATA_FILENAME,
            )
            # caller had no specific idea what metadata version is
            # needed/available This dataset does not have aggregated metadata.
            # Does it have any other version?
            info_glob = op.join(
                text_type(dspath), agginfo_relpath_template).format('*')
            info_files = glob.glob(info_glob)
            msg = "Found no aggregated metadata info file %s." \
                  % info_fpath
            old_metadata_file = op.join(
                text_type(dspath), OLDMETADATA_DIR, OLDMETADATA_FILENAME)
            if op.exists(old_metadata_file):
                msg += " Found metadata generated with pre-0.10 version of " \
                       "DataLad, but it will not be used."
            upgrade_msg = ""
            if info_files:
                msg += " Found following info files, which might have been " \
                       "generated with newer version(s) of datalad: %s." \
                       % (', '.join(info_files))
                upgrade_msg = ", upgrade datalad"
            msg += " You will likely need to either update the dataset from its " \
                   "original location%s or reaggregate metadata locally." \
                   % upgrade_msg
            lgr.warning(msg)
    return info_fpath, agg_base_path


# TODO add test
def get_ds_aggregate_db(dspath, version='default', warn_absent=True):
    """Load a dataset's aggregate metadata database

    Parameters
    ----------
    dspath : Path
      Path to a dataset to query
    version : str
      DataLad aggregate metadata layout version. At the moment only a single
      version exists. 'default' will return the content of the current default
      aggregate database version.
    warn_absent : bool
      If True, warn if the desired DB version is not present and give hints on
      what else is available. This is useful when using this function from
      a user-facing command.

    Returns
    -------
    dict
      A dictionary with the database content is return. All paths in the
      dictionary (datasets, metadata object archives) are
      absolute.
    """
    info_fpath, agg_base_path = get_ds_aggregate_db_locations(
        dspath, version, warn_absent)

    # save to call even with a non-existing location
    agginfos = json_load(text_type(info_fpath)) if info_fpath.exists() else {}

    return {
        # paths in DB on disk are always relative
        # make absolute to ease processing during aggregation
        dspath / p:
        {k: agg_base_path / v if k in location_keys else v
         for k, v in props.items()}
        for p, props in agginfos.items()
    }


@build_doc
class QueryMetadata(Interface):
    """Metadata reporting for files and entire datasets

    Two types of metadata are supported:

    1. metadata describing a dataset as a whole (dataset-global metadata), and

    2. metadata for files in a dataset (content metadata).

    Both types can be accessed with this command.

    Examples:

      Report the metadata of a single file, as aggregated into the closest
      locally available dataset, containing the query path::

        % datalad metadata somedir/subdir/thisfile.dat

      Sometimes it is helpful to get metadata records formatted in a more accessible
      form, here as pretty-printed JSON::

        % datalad -f json_pp metadata somedir/subdir/thisfile.dat

      Same query as above, but specify which dataset to query (must be
      containing the query path)::

        % datalad metadata -d . somedir/subdir/thisfile.dat

      Report any metadata record of any dataset known to the queried dataset::

        % datalad metadata --recursive --reporton datasets 

      Get a JSON-formatted report of aggregated metadata in a dataset, incl.
      information on enabled metadata extractors, dataset versions, dataset IDs,
      and dataset paths::

        % datalad -f json metadata --reporton aggregates
    """
    # make the custom renderer the default, path reporting isn't the top
    # priority here
    result_renderer = 'tailored'

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""dataset to query. If given, metadata will be reported
            as stored in this dataset. Otherwise, the closest available
            dataset containing a query path will be consulted.""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="path(s) to query metadata for",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        reporton=Parameter(
            args=('--reporton',),
            constraints=EnsureChoice('all', 'datasets', 'files', 'aggregates'),
            doc="""choose on what type metadata to report on: dataset-global
            metadata only ('datasets'), metadata on dataset content/files only
            ('files'), or both ('all', default). There is an additional
            category 'aggregates' that reports on which datasets aggregate
            metadata is recorded in the queried dataset."""),
        recursive=recursion_flag)
        # what kind of metadata to report

    @staticmethod
    @datasetmethod(name='query_metadata')
    @eval_results
    def __call__(
            path=None,
            dataset=None,
            reporton='all',
            recursive=False):
        # prep results
        res_kwargs = dict(action='metadata', logger=lgr)
        ds = require_dataset(
            dataset=dataset,
            check_installed=True,
            purpose='aggregate metadata query')
        if dataset:
            res_kwargs['refds'] = ds.path

        agginfos = get_ds_aggregate_db(
            ds.pathobj,
            version=str(aggregate_layout_version),
            # we are handling errors below
            warn_absent=False,
        )
        if not agginfos:
            # if there has ever been an aggregation run, this file would
            # exist, hence there has not been and we need to tell this
            # to people
            yield get_status_dict(
                ds=ds,
                status='impossible',
                message='metadata aggregation has never been performed in '
                'this dataset',
                **res_kwargs)
            return

        if not path:
            # implement https://github.com/datalad/datalad/issues/3282
            path = ds.pathobj if isinstance(dataset, Dataset) else os.getcwd()

        # check for paths that are not underneath this dataset
        resolved_paths = set()
        for p in assure_list(path):
            p = rev_resolve_path(p, dataset)
            if p != ds.pathobj and ds.pathobj not in p.parents:
                raise ValueError(
                    'given path {} is not underneath dataset {}'.format(
                        p, ds))
            resolved_paths.add(p)

        # sort paths into their containing dataset aggregate records
        paths_by_ds = {}
        while resolved_paths:
            resolved_path = resolved_paths.pop()
            # find the first dataset that matches
            for aggdspath in sorted(agginfos, reverse=True):
                if recursive and resolved_path in aggdspath.parents:
                    ps = paths_by_ds.get(aggdspath, set())
                    ps.add(aggdspath)
                    paths_by_ds[aggdspath] = ps
                elif aggdspath == resolved_path \
                        or aggdspath in resolved_path.parents:
                    ps = paths_by_ds.get(aggdspath, set())
                    ps.add(resolved_path)
                    paths_by_ds[aggdspath] = ps
                    # stop when the containing dataset is found
                    break

        # which files do we need to have locally to perform the query
        info_keys = \
            ('dataset_info', 'content_info') if reporton == 'all' else \
            ('dataset_info',) if reporton == 'datasets' else \
            ('content_info',) if reporton == 'files' else \
            []
        objfiles = [
            text_type(agginfos[d][t])
            for d in paths_by_ds
            for t in info_keys
        ]
        lgr.debug(
            'Verifying/achieving local availability of %i metadata objects',
            len(objfiles))
        if objfiles:
            for r in ds.get(
                    path=objfiles,
                    result_renderer='disabled',
                    return_type='generator'):
                # report only of not a success as this is an internal operation
                # that a user would not (need to) expect
                if success_status_map.get(r['status'], False) != 'success':  # pragma: no cover
                    yield r

        parentds = []
        # loop over all records to get complete parentds relationships
        for aggdspath in sorted(agginfos):
            while parentds and parentds[-1] not in aggdspath.parents:
                parentds.pop()
            if aggdspath not in paths_by_ds:
                # nothing to say about this
                parentds.append(aggdspath)
                continue
            agg_record = agginfos[aggdspath]
            if reporton == 'aggregates':
                # we do not need to loop over the actual query paths, as
                # the aggregates of the containing dataset will contain
                # the desired info, if any exists

                # convert pathobj before emitting until we became more clever
                info = {k: text_type(v) if isinstance(v, ut.PurePath) else v
                        for k, v in iteritems(agg_record)}
                info.update(
                    path=text_type(aggdspath),
                    type='dataset',
                )
                if aggdspath == ds.pathobj:
                    info['layout_version'] = aggregate_layout_version
                if parentds:
                    info['parentds'] = text_type(parentds[-1])
                yield dict(
                    info,
                    status='ok',
                    **res_kwargs
                )
                parentds.append(aggdspath)
                continue
            dsmeta = None
            if reporton in ('datasets', 'all'):
                # we do not need path matching here, we already know
                # that something in this dataset is relevant
                objfile = text_type(agg_record['dataset_info'])
                # TODO if it doesn't exist but is requested say impossible?
                dsmeta = json_load(objfile)
                info = dict(
                    path=text_type(aggdspath),
                    status='ok',
                    type='dataset',
                    metadata=dsmeta,
                    # some things that should be there, but maybe not
                    # -- make optional to be more robust
                    dsid=agg_record.get('id', None),
                    refcommit=agg_record.get('refcommit', None),
                    datalad_version=agg_record.get('datalad_version', None),
                    **res_kwargs
                )
                if parentds:
                    info['parentds'] = text_type(parentds[-1])
                yield info
            if reporton in ('files', 'all'):
                query_paths = paths_by_ds[aggdspath]
                objfile = text_type(agg_record['content_info'])
                # TODO if it doesn't exist but is requested say impossible?
                for file_record in json_streamload(objfile):
                    if 'path' not in file_record:  # pragma: no cover
                        yield dict(
                            status='error',
                            message=(
                                "content metadata contains record "
                                "without a 'path' specification: %s",
                                agg_record),
                            type='dataset',
                            path=aggdspath,
                            **res_kwargs
                        )
                        continue
                    # absolute path for this file record
                    # metadata record always uses POSIX conventions
                    fpath = aggdspath / ut.PurePosixPath(file_record['path'])
                    if not any(p == fpath or p in fpath.parents
                               for p in query_paths):
                        # ignore any file record that doesn't match any query
                        # path (direct hit or git-annex-like recursion within a
                        # dataset)
                        continue
                    if dsmeta is not None and \
                            '@context' in dsmeta and \
                            '@context' not in file_record:
                        file_record['@context'] = dsmeta['@context']
                    info = dict(
                        path=text_type(fpath),
                        parentds=text_type(aggdspath),
                        status='ok',
                        type='file',
                        metadata={k: v for k, v in iteritems(file_record)
                                  if k not in ('path',)},
                        dsid=agg_record['id'],
                        refcommit=agg_record['refcommit'],
                        datalad_version=agg_record['datalad_version'],
                        **res_kwargs
                    )
                    yield info

            parentds.append(aggdspath)

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        if res['status'] != 'ok' or not res.get('action', None) == 'metadata':
            # logging complained about this already
            return
        # list the path, available metadata keys, and tags
        path = op.relpath(res['path'],
                       res['refds']) if res.get('refds', None) else res['path']
        meta = res.get('metadata', {})
        ui.message('{path}{type}:{spacer}{meta}{tags}'.format(
            path=ac.color_word(path, ac.BOLD),
            type=' ({})'.format(
                ac.color_word(res['type'], ac.MAGENTA)) if 'type' in res else '',
            spacer=' ' if len([m for m in meta if m != 'tag']) else '',
            meta=','.join(k for k in sorted(meta.keys())
                          if k not in ('tag', '@context', '@id'))
                 if meta else ' -' if 'metadata' in res else
                 ' {}'.format(
                     ','.join(e for e in res['extractors']
                              if e not in ('datalad_core', 'annex'))
                 ) if 'extractors' in res else '',
            tags='' if 'tag' not in meta else ' [{}]'.format(
                 ','.join(assure_list(meta['tag'])))))
