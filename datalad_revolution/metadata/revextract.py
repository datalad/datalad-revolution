# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Run one or more metadata extractors on a dataset or file(s)"""

__docformat__ = 'restructuredtext'

from os import curdir
import os.path as op
import logging
from six import (
    iteritems,
)
from collections import (
    Mapping,
)

from datalad import cfg
from datalad.interface.base import Interface
from datalad.interface.base import build_doc
from datalad.interface.results import (
    get_status_dict,
    success_status_map,
)
from datalad.interface.utils import eval_results
from ..dataset import (
    rev_datasetmethod as datasetmethod,
    EnsureRevDataset as EnsureDataset,
    require_rev_dataset as require_dataset,
)
from .extractors.base import MetadataExtractor

from datalad.support.annexrepo import AnnexRepo
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureChoice,
    EnsureBool,
)
from datalad.metadata.metadata import (
    exclude_from_metadata,
    get_metadata_type,
)
from datalad.metadata.definitions import version as vocabulary_version
from datalad.utils import (
    assure_list,
    as_unicode,
)
from datalad.dochelpers import exc_str
from datalad.log import log_progress

# API commands needed
from datalad.distribution.subdatasets import Subdatasets
from .. import revstatus

lgr = logging.getLogger('datalad.metadata.metadata')


@build_doc
class RevExtractMetadata(Interface):
    """Run one or more of DataLad's metadata extractors on a dataset or file.

    The result(s) are structured like the metadata DataLad would extract
    during metadata aggregation. There is one result per dataset/file.

    Examples:

      Extract metadata with two extractors from a dataset in the current
      directory and also from all its files::

        $ datalad extract-metadata -d . --source xmp --source datalad_core

      Extract XMP metadata from a single PDF that is not part of any dataset::

        $ datalad extract-metadata --source xmp Downloads/freshfromtheweb.pdf
    """

    _params_ = dict(
        sources=Parameter(
            args=("--source",),
            dest="sources",
            metavar=("NAME"),
            action='append',
            doc="""Name of a metadata extractor to be executed.
            If none is given, a set of default configured extractors,
            plus any extractors enabled in a dataset's configuration
            and invoked.
            [CMD: This option can be given more than once CMD]"""),
        process_type=Parameter(
            args=("--process-type",),
            doc="""dataset component type to process. If 'all',
            metadata will be extracted for the entire dataset and its content.
            If not specified, the dataset's configuration will determine
            the selection, and will default to 'all'. Note that not processing
            content can influence the dataset metadata content (e.g. report
            of total size).""",
            constraints=EnsureChoice(None, 'all', 'dataset', 'content')),
        path=Parameter(
            args=("path",),
            metavar="FILE",
            nargs="*",
            doc="Path of a file to extract metadata from.",
            constraints=EnsureStr() | EnsureNone()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""Dataset to extract metadata from. If no path is given,
            metadata is extracted from all files of the dataset.""",
            constraints=EnsureDataset() | EnsureNone()),
    )

    @staticmethod
    @datasetmethod(name='rev_extract_metadata')
    @eval_results
    def __call__(dataset=None, path=None, sources=None, process_type=None):
        # TODO verify that we need this ds vs dataset distinction
        ds = dataset = require_dataset(
            dataset or curdir,
            purpose="extract metadata",
            check_installed=not path)

        # check what extractors we want as sources, and whether they are
        # available
        if not sources:
            sources = ['datalad_core', 'annex'] \
                + assure_list(get_metadata_type(dataset))
        # keep local, who knows what some extractors might pull in
        from pkg_resources import iter_entry_points  # delayed heavy import
        extractors = {
            ep.name: ep
            for ep in iter_entry_points('datalad.metadata.extractors')}
        for msrc in sources:
            if msrc not in extractors:
                # we said that we want to fail, rather then just moan about
                # less metadata
                raise ValueError(
                    'Enabled metadata extractor %s not available'.format(msrc),
                )

        res_props = dict(
            action='extract_metadata',
            logger=lgr,
        )

        # build a representation of the dataset's content (incl subds
        # records)
        # go through a high-level command (not just the repo methods) to
        # get all the checks and sanitization of input arguments
        # this call is relatively expensive, but already anticipates
        # demand for information by our core extractors that always run
        # unconditionally, hence no real slowdown here
        # TODO this could be a dict, but MIH cannot think of an access
        # pattern that does not involve iteration over all items
        status = []
        if ds.is_installed():
            # we can make use of status
            res_props.update(refds=dataset.path)

            for r in ds.rev_status(
                    # let status sort out all path arg handling
                    # but this will likely make it impossible to use this
                    # command to just process an individual file independent
                    # of a dataset
                    path=path,
                    # it is safe to ask for annex info even when a dataset is
                    # plain Git
                    annex='availability',
                    # TODO we never want to aggregate metadata from untracked
                    # content, but we might just want to see what we can get
                    # from a file
                    untracked='no',
                    # this command cannot and will not work recursively
                    recursive=False,
                    result_renderer='disabled'):
                # path reports are always absolute and anchored on the dataset
                # (no repo) path
                if any(r['path'].startswith(str(ds.pathobj / e) + op.sep)
                       for e in exclude_from_metadata):
                    # this needs to be ignore for any further processing
                    continue
                # strip useless context information
                status.append(
                    {k: v for k, v in iteritems(r)
                     if (k not in ('refds', 'parentds', 'action', 'status')
                         and not k.startswith('prev_'))})
        else:
            # no dataset at hand, take path arg at face value and hope
            # for the best
            # TODO we have to resolve the given path to make it match what
            # status is giving (abspath with ds (not repo) anchor)
            status = [dict(path=p, type='file') for p in assure_list(path)]

        try:
            for res in _proc(
                    ds,
                    sources,
                    status,
                    extractors,
                    process_type):
                res.update(**res_props)
                yield res
        finally:
            # extractors can come from any source with no guarantee for
            # proper implementation. Let's make sure that we bring the
            # dataset back into a sane state (e.g. no batch processes
            # hanging around). We should do this here, as it is not
            # clear whether extraction results will be saved to the
            # dataset(which would have a similar sanitization effect)
            if ds.repo:
                ds.repo.precommit()


def _proc(ds, sources, status, extractors, process_type):
    dsmeta = dict()
    contentmeta = {}

    # this block can be removed when we stop supporting partial
    # extraction from datasets with some unavailable content
    # using old-style extractors that cannot report errors
    # properly
    fullstatus = status
    if status and isinstance(ds.repo, AnnexRepo):
        status = [p for p in status if p.get('has_content', True)]
        nocontent = len(fullstatus) - len(status)
        if nocontent and process_type in ('content', 'all'):
            # TODO better fail, or support incremental and label this file as
            # no present
            lgr.warn(
                '{} files have no content present, '
                'some extractors will not operate on {}'.format(
                    nocontent,
                    'them' if nocontent > 10
                    else [
                        p['path'] for p in fullstatus
                        if not p.get('has_content', True)])
            )

    log_progress(
        lgr.info,
        'metadataextractors',
        'Start metadata extraction from %s', ds,
        total=len(sources),
        label='Metadata extraction',
        unit=' extractors',
    )
    for msrc in sources:
        msrc_key = msrc
        log_progress(
            lgr.info,
            'metadataextractors',
            'Engage %s metadata extractor', msrc_key,
            update=1,
            increment=True)

        # load the extractor class, no instantiation yet
        try:
            extractor_cls = extractors[msrc].load()
        except Exception as e:  # pragma: no cover
            msg = ('Failed %s metadata extraction from %s: %s',
                   msrc, ds, exc_str(e))
            log_progress(lgr.error, 'metadataextractors', *msg)
            raise ValueError(msg[0] % msg[1:])

        # desired setup for generation of unique metadata values
        want_unique = ds.config.obtain(
            'datalad.metadata.generate-unique-{}'.format(
                msrc_key.replace('_', '-')),
            default=True,
            valtype=EnsureBool())
        unique_cm = {}
        extractor_unique_exclude = getattr(
            extractor_cls,
            "_unique_exclude",
            set())

        # actually pull the metadata records out of the extractor
        for res in _run_extractor(
                extractor_cls,
                msrc,
                ds,
                status
                if getattr(extractor_cls, 'NEEDS_CONTENT', False)
                else fullstatus,
                process_type):
            # always have a path
            # TODO add check the make sure that any 'file' report already
            # has a path
            res.update(
                path=op.join(ds.path, res['path'])
                if 'path' in res else ds.path,
            )

            # the following two conditionals are untested, as a test would
            # require a metadata extractor to yield broken metadata, and in
            # order to have such one, we need a mechanism to have the test
            # inject one on the fly MIH thinks that the code neeeded to do that
            # is more chances to be broken then the code it would test
            # TODO verify that is has worked once by manually breaking an
            # extractor
            if success_status_map.get(res['status'], False) != 'success':  # pragma: no cover
                yield res
                # no further processing of broken stuff
                continue
            if success_status_map.get(res['status'], False) == 'success':  # pragma: no cover
                # if the extractor was happy check the result
                if not _ok_metadata(res, msrc, ds, None):
                    res.update(
                        # this will prevent further processing a few lines down
                        status='error',
                        # TODO have _ok_metadata report the real error
                        message=('Invalid metadata (%s)', msrc),
                    )
                    yield res
                    continue

            # we also want to report info that there was no metadata if there
            # is an issue that a extractor needlessly produces empty records,
            # the extractor should be fixed and not a general switch.
            #if not res['metadata']:
            #    # after checks and filters nothing is left, nothing to report
            #    continue

            if res['type'] == 'dataset':
                # TODO warn if two dataset records are generated by the same
                # extractor
                dsmeta[msrc_key] = res['metadata']
            else:
                # this is file metadata, _ok_metadata() checks unknown types
                # assign only ask each metadata extractor once, hence no
                # conflict possible
                loc_dict = contentmeta.get(res['path'], {})
                loc_dict[msrc_key] = res['metadata']
                contentmeta[res['path']] = loc_dict
                if want_unique:
                    # go through content metadata and inject report of
                    # unique keys and values into `unique_cm`
                    _update_unique_cm(
                        unique_cm,
                        msrc_key,
                        dsmeta,
                        res['metadata'],
                        extractor_unique_exclude,
                    )
        if unique_cm:
            # produce final unique record in dsmeta for this extractor
            _finalize_unique_cm(unique_cm, msrc_key, dsmeta)

    log_progress(
        lgr.info,
        'metadataextractors',
        'Finished metadata extraction from %s', ds,
    )

    if dsmeta:
        # always identify the effective vocabulary - JSON-LD style
        dsmeta['@context'] = {
            '@vocab': 'http://docs.datalad.org/schema_v{}.json'.format(
                vocabulary_version)}

    if process_type in ('all', 'dataset') and \
            dsmeta and ds is not None and ds.is_installed():
        yield get_status_dict(
            ds=ds,
            metadata=dsmeta,
            # any errors will have been reported before
            status='ok',
        )

    for p in contentmeta:
        res = get_status_dict(
            # TODO avoid is_installed() call
            path=op.join(ds.path, p) if ds.is_installed() else p,
            metadata=contentmeta[p],
            type='file',
            # any errors will have been reported before
            status='ok',
        )
        # TODO avoid is_installed() call, check if such info is
        # useful and accurate at all
        if ds.is_installed():
            res['parentds'] = ds.path
        yield res


def _run_extractor(extractor_cls, name, ds, status, process_type):
    """Helper to control extractor using the right API

    Central switch to deal with alternative/future APIs is inside
    """
    try:
        # detect supported API and interface as needed
        if issubclass(extractor_cls, MetadataExtractor):
            # new-style, command-like extractors
            extractor = extractor_cls()
            for r in extractor(
                    dataset=ds,
                    status=status,
                    process_type=process_type):
                yield r
        elif hasattr(extractor_cls, 'get_metadata'):
            # old-style
            for res in _yield_res_from_pre2019_extractor(
                    ds,
                    name,
                    extractor_cls,
                    process_type,
                    # old extractors only take a list of relative paths
                    # and cannot benefit from outside knowledge
                    # TODO avoid is_installed() call
                    [op.relpath(p['path'], start=ds.path) if ds.is_installed()
                     else p['path']
                     for p in status]):
                yield res
        else:
            raise RuntimeError(
                '{} does not have a recognised extractor API'.format(
                    extractor_cls))
    except Exception as e:  # pragma: no cover
        if cfg.get('datalad.runtime.raiseonerror'):
            log_progress(
                lgr.error,
                'metadataextractors',
                'Failed %s metadata extraction from %s', name, ds,
            )
            raise
        yield get_status_dict(
            ds=ds,
            # any errors will have been reported before
            status='error',
            message=('Failed to get %s metadata (%s): %s',
                     ds, name, exc_str(e)),
        )


def _yield_res_from_pre2019_extractor(
        ds, name, extractor_cls, process_type, paths):
    """This implements dealing with our first extractor class concept"""

    want_dataset_meta = process_type in ('all', 'dataset') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-dataset-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())
    want_content_meta = process_type in ('all', 'content') \
        if process_type else ds.config.obtain(
            'datalad.metadata.extract-content-{}'.format(
                name.replace('_', '-')),
            default=True,
            valtype=EnsureBool())

    if not (want_dataset_meta or want_content_meta):
        log_progress(
            lgr.info,
            'metadataextractors',
            'Skipping %s metadata extraction from %s, '
            'disabled by configuration',
            name, ds,
        )
        return

    try:
        extractor = extractor_cls(ds, paths)
    except Exception as e:  # pragma: no cover
        log_progress(
            lgr.error,
            'metadataextractors',
            'Failed %s metadata extraction from %s', name, ds,
        )
        raise ValueError(
            "Failed to load metadata extractor for '%s', "
            "broken dataset configuration (%s)?: %s",
            name, ds, exc_str(e))

    # this is the old way of extractor operation
    dsmeta_t, contentmeta_t = extractor.get_metadata(
        dataset=want_dataset_meta,
        content=want_content_meta,
    )
    # fake the new way of reporting results directly
    # extractors had no way to report errors, hence
    # everything is unconditionally 'ok'
    for loc, meta in contentmeta_t:
        yield dict(
            status='ok',
            path=loc,
            type='file',
            metadata=meta,
        )
    yield dict(
        status='ok',
        path=ds.path,
        type='dataset',
        metadata=dsmeta_t,
    )


def _update_unique_cm(unique_cm, msrc_key, dsmeta, cnmeta, exclude_keys):
    """Sift through a new content metadata set and update the unique value
    record

    Parameters
    ----------
    unique_cm : dict
      unique value records for an individual extractor, modified
      in place
    msrc_key : str
      key of the extractor currently processed
    dsmeta : dict
      dataset metadata record. To lookup conflicting field.
    cnmeta : dict
      Metadata to sift through.
    exclude_keys : iterable
      Keys of fields to exclude from processing.
    """
    # go through content metadata and inject report of unique keys
    # and values into `dsmeta`
    for k, v in iteritems(cnmeta):
        if k in dsmeta.get(msrc_key, {}):
            # XXX untested, needs a provoked conflict of content and dsmeta
            # relatively hard to fake in a test
            #
            # if the dataset already has a dedicated idea
            # about a key, we skip it from the unique list
            # the point of the list is to make missing info about
            # content known in the dataset, not to blindly
            # duplicate metadata. Example: list of samples data
            # were recorded from. If the dataset has such under
            # a 'sample' key, we should prefer that, over an
            # aggregated list of a hopefully-kinda-ok structure
            continue
        elif k in exclude_keys:
            # XXX this is untested ATM and waiting for
            # https://github.com/datalad/datalad/issues/3135
            #
            # the extractor thinks this key is worthless for the purpose
            # of discovering whole datasets
            # we keep the key (so we know that some file is providing this
            # key), but ignore any value it came with
            unique_cm[k] = None
            continue
        vset = unique_cm.get(k, set())
        vset.add(_val2hashable(v))
        unique_cm[k] = vset


def _finalize_unique_cm(unique_cm, msrc_key, dsmeta):
    """Convert harvested unique values in a serializable, ordered
    representation, and inject it into the dataset metadata

    Parameters
    ----------
    unique_cm : dict
      unique value records for an individual extractor
    msrc_key : str
      key of the extractor currently processed
    dsmeta : dict
      dataset metadata record to inject unique value report into,
      modified in place
    """
    # per source storage here too
    ucp = dsmeta.get('datalad_unique_content_properties', {})
    # important: we want to have a stable order regarding
    # the unique values (a list). we cannot guarantee the
    # same order of discovery, hence even when not using a
    # set above we would still need sorting. the callenge
    # is that any value can be an arbitrarily complex nested
    # beast
    # we also want to have each unique value set always come
    # in a top-level list, so we known if some unique value
    # was a list, os opposed to a list of unique values

    def _ensure_serializable(val):
        # XXX special cases are untested, need more convoluted metadata
        if isinstance(val, ReadOnlyDict):
            return {k: _ensure_serializable(v) for k, v in iteritems(val)}
        if isinstance(val, (tuple, list)):
            return [_ensure_serializable(v) for v in val]
        else:
            return val

    ucp[msrc_key] = {
        k: [_ensure_serializable(i)
            for i in sorted(
                v,
                key=_unique_value_key)] if v is not None else None
        for k, v in iteritems(unique_cm)
        # v == None (disable unique, but there was a value at some point)
        # otherwise we only want actual values, and also no single-item-lists
        # of a non-value
        # those contribute no information, but bloat the operation
        # (inflated number of keys, inflated storage, inflated search index,
        # ...)
        if v is None or (v and not v == {''})}
    dsmeta['datalad_unique_content_properties'] = ucp


def _val2hashable(val):
    """Small helper to convert incoming mutables to something hashable

    The goal is to be able to put the return value into a set, while
    avoiding conversions that would result in a change of representation
    in a subsequent JSON string.
    """
    # XXX special cases are untested, need more convoluted metadata
    if isinstance(val, dict):
        return ReadOnlyDict(val)
    elif isinstance(val, list):
        return tuple(map(_val2hashable, val))
    else:
        return val


def _unique_value_key(x):
    """Small helper for sorting unique content metadata values"""
    if isinstance(x, ReadOnlyDict):
        # XXX special case untested, needs more convoluted metadata
        #
        # turn into an item tuple with keys sorted and values plain
        # or as a hash if *dicts
        x = [(k,
              hash(x[k])
              if isinstance(x[k], ReadOnlyDict) else x[k])
             for k in sorted(x)]
    # we need to force str, because sorted in PY3 refuses to compare
    # any heterogeneous type combinations, such as str/int,
    # tuple(int)/tuple(str)
    return as_unicode(x)


def _ok_metadata(res, msrc, ds, loc):
    restype = res.get('type', None)
    if restype not in ('dataset', 'file'):
        # XXX untested, needs broken extractor
        lgr.error(
            'metadata report for something other than a file or dataset: %s',
            restype
        )
        return False

    meta = res.get('metadata', None)
    if meta is None or isinstance(meta, dict):
        return True

    # XXX untested, needs broken extractord
    msg = (
        "Metadata extractor '%s' yielded something other than a dictionary "
        "for dataset %s%s -- this is likely a bug, please consider "
        "reporting it. "
        "This type of native metadata will be ignored. Got: %s",
        msrc,
        ds,
        '' if loc is None else ' content {}'.format(loc),
        repr(meta))
    if cfg.get('datalad.runtime.raiseonerror'):
        raise RuntimeError(*msg)

    lgr.error(*msg)
    return False


class ReadOnlyDict(Mapping):
    # Taken from https://github.com/slezica/python-frozendict
    # License: MIT

    # XXX entire class is untested

    """
    An immutable wrapper around dictionaries that implements the complete
    :py:class:`collections.Mapping` interface. It can be used as a drop-in
    replacement for dictionaries where immutability is desired.
    """
    dict_cls = dict

    def __init__(self, *args, **kwargs):
        self._dict = self.dict_cls(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def copy(self, **add_or_replace):
        return self.__class__(self, **add_or_replace)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self._dict)

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key, value in iteritems(self._dict):
                h ^= hash((key, _val2hashable(value)))
            self._hash = h
        return self._hash
