# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Interface for aggregating metadata
"""

__docformat__ = 'restructuredtext'

import logging
import tempfile
from six import (
    iteritems,
    text_type,
)
from collections import OrderedDict

import os.path as op

import shutil

# API commands we need
from .revextract import RevExtractMetadata
from ..revstatus import RevStatus as Status
from ..revsave import RevSave as Save
from ..revdiff import (
    RevDiff as Diff,
    PRE_INIT_COMMIT_SHA,
)

import datalad
from datalad.interface.base import Interface
from datalad.interface.utils import (
    eval_results,
)
from datalad.interface.base import build_doc
from datalad.interface.common_opts import (
    recursion_limit,
    recursion_flag,
)
from datalad.interface.results import (
    success_status_map,
)
from datalad.metadata.metadata import (
    get_ds_aggregate_db_locations,
    load_ds_aggregate_db,
)
from datalad.metadata.metadata import (
    exclude_from_metadata,
    location_keys,
)
from ..dataset import (
    RevolutionDataset as Dataset,
    rev_datasetmethod as datasetmethod,
    EnsureRevDataset as EnsureDataset,
    require_rev_dataset as require_dataset,
    sort_paths_by_datasets,
)
from .. import utils as ut
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone,
)
from datalad.support.constraints import EnsureChoice
from datalad.support import json_py
from datalad.support.digests import Digester
from datalad.utils import (
    assure_list,
    rmtree,
)

lgr = logging.getLogger('datalad.metadata.aggregate')


@build_doc
class RevAggregateMetadata(Interface):
    """Aggregate metadata of one or more datasets for later query.

    Metadata aggregation refers to a procedure that extracts metadata present
    in a dataset into a portable representation that is stored a single
    standardized format. Moreover, metadata aggregation can also extract
    metadata in this format from one dataset and store it in another
    (super)dataset. Based on such collections of aggregated metadata it is
    possible to discover particular datasets and specific parts of their
    content, without having to obtain the target datasets first (see the
    DataLad 'search' command).

    To enable aggregation of metadata that are contained in files of a dataset,
    one has to enable one or more metadata extractor for a dataset. DataLad
    supports a number of common metadata standards, such as the Exchangeable
    Image File Format (EXIF), Adobe's Extensible Metadata Platform (XMP), and
    various audio file metadata systems like ID3. DataLad extension packages
    can provide metadata data extractors for additional metadata sources. For
    example, the neuroimaging extension provides extractors for scientific
    (meta)data standards like BIDS, DICOM, and NIfTI1.  Some metadata
    extractors depend on particular 3rd-party software. The list of metadata
    extractors available to a particular DataLad installation is reported by
    the 'wtf' command ('datalad wtf').

    Enabling a metadata extractor for a dataset is done by adding its name to the
    'datalad.metadata.nativetype' configuration variable -- typically in the
    dataset's configuration file (.datalad/config), e.g.::

      [datalad "metadata"]
        nativetype = exif
        nativetype = xmp

    If an enabled metadata extractor is not available in a particular DataLad
    installation, metadata extraction will not succeed in order to avoid
    inconsistent aggregation results.

    Enabling multiple extractors is supported. In this case, metadata are
    extracted by each extractor individually, and stored alongside each other.
    Metadata aggregation will also extract DataLad's own metadata (extractors
    'datalad_core', and 'annex').

    Metadata aggregation can be performed recursively, in order to aggregate all
    metadata across all subdatasets, for example, to be able to search across
    any content in any dataset of a collection. Aggregation can also be performed
    for subdatasets that are not available locally. In this case, pre-aggregated
    metadata from the closest available superdataset will be considered instead.

    Depending on the versatility of the present metadata and the number of dataset
    or files, aggregated metadata can grow prohibitively large. A number of
    configuration switches are provided to mitigate such issues.

    datalad.metadata.aggregate-content-<extractor-name>
      If set to false, content metadata aggregation will not be performed for
      the named metadata extractor (a potential underscore '_' in the extractor name must
      be replaced by a dash '-'). This can substantially reduce the runtime for
      metadata extraction, and also reduce the size of the generated metadata
      aggregate. Note, however, that some extractors may not produce any metadata
      when this is disabled, because their metadata might come from individual
      file headers only. 'datalad.metadata.store-aggregate-content' might be
      a more appropriate setting in such cases.

    datalad.metadata.aggregate-ignore-fields
      Any metadata key matching any regular expression in this configuration setting
      is removed prior to generating the dataset-level metadata summary (keys
      and their unique values across all dataset content), and from the dataset
      metadata itself. This switch can also be used to filter out sensitive
      information prior aggregation.

    datalad.metadata.generate-unique-<extractor-name>
      If set to false, DataLad will not auto-generate a summary of unique content
      metadata values for a particular extractor as part of the dataset-global metadata
      (a potential underscore '_' in the extractor name must be replaced by a dash '-').
      This can be useful if such a summary is bloated due to minor uninformative (e.g.
      numerical) differences, or when a particular extractor already provides a
      carefully designed content metadata summary.

    datalad.metadata.maxfieldsize
      Any metadata value that exceeds the size threshold given by this configuration
      setting (in bytes/characters) is removed.

    datalad.metadata.store-aggregate-content
      If set, extracted content metadata are still used to generate a dataset-level
      summary of present metadata (all keys and their unique values across all
      files in a dataset are determined and stored as part of the dataset-level
      metadata aggregate, see datalad.metadata.generate-unique-<extractor-name>),
      but metadata on individual files are not stored.
      This switch can be used to avoid prohibitively large metadata files. Discovery
      of datasets containing content matching particular metadata properties will
      still be possible, but such datasets would have to be obtained first in order
      to discover which particular files in them match these properties.
    """
    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""topmost dataset metadata will be aggregated into. All dataset
            between this dataset and any given path will receive updated
            aggregated metadata from all given paths.""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="""path to datasets that shall be aggregated.
            When a given path is pointing into a dataset, the metadata of the
            containing dataset will be aggregated.  If no paths given, current
            dataset metadata is aggregated.""",
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
        update_mode=Parameter(
            args=('--update-mode',),
            constraints=EnsureChoice('target'),
            doc="""which datasets to update with newly aggregated metadata:
            all datasets from any leaf dataset to the top-level target dataset
            including all intermediate datasets (all), or just the top-level
            target dataset (target)."""),
        force_extraction=Parameter(
            args=('--force-extraction',),
            action='store_true',
            doc="""If set, all enabled extractors will be engaged regardless of
            whether change detection indicates that metadata has already been
            extracted for a given dataset state."""),
    )

    @staticmethod
    @datasetmethod(name='rev_aggregate_metadata')
    @eval_results
    def __call__(
            path=None,
            dataset=None,
            recursive=False,
            recursion_limit=None,
            update_mode='target',
            force_extraction=False):

        ds = require_dataset(
            dataset, check_installed=True, purpose='metadata aggregation')
        # path args could be
        # - installed datasets
        # - names of pre-aggregated dataset that are not around
        # - -like rev-status they should match anything underneath them

        # Step 1: figure out which available dataset is closest to a given path
        if path:
            extract_from_ds, errors = sort_paths_by_datasets(
                dataset, assure_list(path))
            for e in errors:
                e.update(
                    logger=lgr,
                    refds=ds.path,
                )
                yield e
        else:
            extract_from_ds = OrderedDict({ds.pathobj: []})

        # convert the values into sets to ease processing below
        extract_from_ds = {
            Dataset(k): set(assure_list(v))
            for k, v in iteritems(extract_from_ds)
        }

        #
        # Step 1: figure out which available dataset need to be processed
        #
        ds_with_pending_changes = set()
        # note that depending on the recursion settings, we may not
        # actually get a report on each dataset in question
        # TODO implement an alternative detector that uses rev-diff to
        # find datasets that have changes since the last recorded refcommit
        detector = Status()(
            # pass arg in as-is to get proper argument semantics
            dataset=dataset,
            # query on all paths to get desired result with recursion
            # enables
            path=path,
            # never act on anything untracked, we cannot record its identity
            untracked='no',
            recursive=recursive,
            recursion_limit=recursion_limit,
            result_renderer='disabled',
            return_type='generator',
            # let the top-level caller handle failure
            on_failure='ignore')
        for s in detector:
            # TODO act on generator errors?

            # path reports are always absolute and anchored on the dataset
            # (no repo) path
            ds_candidate = Dataset(s['parentds'])

            # ignore anything that isn't all clean, otherwise we have no
            # reliable record of an ID
            if s['state'] != 'clean':
                if ds_candidate not in ds_with_pending_changes:
                    yield dict(
                        action='aggregate_metadata',
                        path=ds_candidate.path,
                        logger=lgr,
                        status='impossible',
                        message='dataset has pending changes',
                    )
                    ds_with_pending_changes.add(ds_candidate)
                continue

            # we always know that the parent was modified too
            fromds = extract_from_ds.get(ds_candidate, set())
            if s['type'] == 'dataset':
                # record that this dataset could have info on this subdataset
                # TODO at the moment this unconditional inclusion makes it
                # impossible to just pick a single unavailable dataset from the
                # aggregated metadata in an available dataset, it will always
                # include all of them as candidates. We likely need at least
                # some further testing below which metadata aggregates are
                # better then those already present in the aggregation target
                # related issue: when no subdataset is present, but a path to
                # an unavailable subsub...dataset is given, the most top-level
                # subdataset and the identified subdataset that is contained by
                # the top-level sub will have a record and their metadata will
                # be aggregated. This likely is not an actual issue, though...
                fromds.add(Dataset(s['path']))
            else:
                # extract from this available dataset information on
                # itself
                fromds.add(ds_candidate)
            extract_from_ds[ds_candidate] = fromds

        # shed all records of datasets with pending changes, those have either
        # led to warning and depending on the user's desired might have stopped
        # the aggregation machinery
        extract_from_ds = {
            # remove all aggregation subjects for datasets that are actually
            # available
            k: [i for i in v if i not in extract_from_ds]
            for k, v in iteritems(extract_from_ds)
            # remove all datasets that have been found to have pending
            # modifications
            if k not in ds_with_pending_changes}

        # at this point extract_from_ds is a dict where the keys are
        # locally available datasets (as Dataset() instances), and
        # values are lists of dataset for which to extract aggregated
        # metadata. The key/source dataset is not listed and always
        # implied. Values can be Dataset() instances, which identified
        # registered (possibly unavailable) subdatasets. Values can also
        # be Path object that correspond to input arguments that have to
        # be matched against path of dataset on which there is aggregated
        # metadata in the source/key dataset. Such Paths are always assigned
        # to the closest containing available dataset

        # load the info that we have on the top-level dataset's aggregated
        # metadata
        # RF load_ds_aggregate_db() to give pathlib keys already
        top_agginfo_db = {ut.Path(k): v for k, v in iteritems(
            load_ds_aggregate_db(ds, abspath=True, warn_absent=False)
        )}

        # XXX keep in mind that recursion can
        # - traverse the file system
        # - additionally end up recursion into pre-aggregated metadata

        # this will assemble all aggregation records
        agginfo_db = {}
        # TODO this for loop does the heavy lifting (extraction/aggregation)
        # wrap in progress bar
        for aggsrc, aggsubjs in iteritems(extract_from_ds):
            # check extraction is actually needed, by running a diff on the
            # dataset against the last known refcommit, to see whether it had
            # any metadata relevant changes
            last_refcommit = top_agginfo_db.get(
                aggsrc.pathobj, {}).get('refcommit', None)
            have_diff = False
            # TODO should we fall back on the PRE_COMMIT_SHA in case there is
            # no recorded refcommit. This might turn out to be more efficient,
            # as it could avoid working with dataset that have no
            # metadata-relevant content
            if last_refcommit:
                for res in aggsrc.rev_diff(
                        fr=last_refcommit,
                        to='HEAD',
                        # query on all paths to get desired result with
                        # recursion enables
                        path=None,
                        # not possible here, but turn off detection anyways
                        untracked='no',
                        recursive=False,
                        result_renderer='disabled',
                        return_type='generator',
                        # let the top-level caller handle failure
                        on_failure='ignore'):
                    if res.get('action', None) != 'diff':
                        # something unexpected, send upstairs
                        yield res
                    if res['state'] == 'clean':
                        # not an actual diff
                        continue
                    if all(
                            not res['path'].startswith(
                                op.join(res['parentds'], e) + op.sep)

                            for e in exclude_from_metadata):
                        # this is a difference that could have an impact on
                        # metadata stop right here and proceed to extraction
                        have_diff = True
                        break
            if last_refcommit is None or have_diff:
                # really _extract_ metadata for aggsrc
                agginfo = {}
                for res in _extract_metadata(aggsrc, ds):
                    if res.get('action', None) == 'extract_metadata' \
                            and res.get('status', None) == 'ok' \
                            and 'info' in res:
                        agginfo = res['info']
                    # always also report
                    yield res
                # logic based on the idea that there will only be one
                # record per dataset (extracted or from pre-aggregate)
                assert(aggsrc.pathobj not in agginfo_db)
                # place in DB under full path, needs to become relative
                # to any updated dataset later on
                agginfo_db[aggsrc.pathobj] = agginfo
            else:
                # we already have what we need in the toplevel dataset
                # for this locally available dataset
                yield dict(
                    action="extract_metadata",
                    path=aggsrc.path,
                    status='notneeded',
                    type='dataset',
                    logger=lgr,
                )

            # if there is a path in aggsubjs match it against all datasets on
            # which we have aggregated metadata, and expand aggsubjs with a
            # list of such dataset instances
            subjs = []
            for subj in aggsubjs:
                if not isinstance(subj, Dataset):
                    subjs.extend(
                        Dataset(aggds) for aggds in top_agginfo_db
                        # TODO think about distinguishing a direct match
                        # vs this match of any parent (maybe the
                        # latter/current only with --recursive)
                        if ut.Path(aggds) == subj \
                        or subj in ut.Path(aggds).parents
                    )
                else:
                    subjs.append(subj)

            # loop over aggsubjs and pull aggregated metadata for them
            for dssubj in subjs:
                agginfo = top_agginfo_db.get(dssubj.path, None)
                if agginfo is None:
                    # TODO proper error/warning result
                    continue
                # logic based on the idea that there will only be one
                # record per dataset (extracted or from pre-aggregate)
                assert(dssubj.pathobj not in agginfo_db)
                agginfo_db[dssubj.pathobj] = agginfo

        # at this point top_agginfo_db has everything on the previous
        # aggregation state, and agginfo_db everything on what was found in
        # this run

        # procedure
        # 1. whatever is in agginfo_db goes into top_agginfo_db
        # 2. if top_agginfo_db gets an entry replaced, we delete the associated
        #    files (regular unlink)
        # 3. stuff that moved into the object tree gets checksumed and placed
        #    at the target destination
        # 4. update DB file

        # this is where incoming metadata objects would be
        aggtmp_basedir = _get_aggtmp_basedir(ds, mkdir=False)

        obsolete_objs = []
        # top_agginfo_db has the status quo, agginfo_db has all
        # potential changes
        for srcds, agginfo in iteritems(agginfo_db):
            # Check of object files have to be slurped in from TMP
            for objtype in ('dataset_info', 'content_info'):
                obj_path = agginfo.get(objtype, None)
                if obj_path is None:
                    # nothing to act on
                    continue
                obj_path = ut.Path(obj_path)
                if aggtmp_basedir not in obj_path.parents:
                    # this is not in the tempdir where we would know what to
                    # do with it. Trust the integrity of the status quo and
                    # leave as is
                    continue
                # checksum and place in obj tree
                shasum = Digester(
                    digests=['sha1'])(text_type(obj_path))['sha1']
                target_obj_location = _get_obj_location(
                    ds, obj_path, shasum)
                # already update location in incoming DB now
                # a potential file move is happening next
                agginfo[objtype] = text_type(target_obj_location)

                if op.exists(text_type(target_obj_location)):
                    # we checksum by content, if it exists, it is identical
                    # use exist() to be already satisfied by a dangling symlink
                    lgr.debug(
                        "Metadata object already exists at %s, skipped",
                        target_obj_location)
                    continue
                # move srcfile into the object store
                target_obj_location.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(
                    # in TMP
                    text_type(obj_path),
                    # in object store
                    text_type(target_obj_location)
                )

            if srcds in top_agginfo_db:
                old_srcds_info = top_agginfo_db[srcds]
                # we already know something about this dataset
                # check if referenced objects need to be deleted
                # replace this record with the incoming one
                for objtype in ('dataset_info', 'content_info'):
                    if objtype not in old_srcds_info:
                        # all good
                        continue
                    if agginfo[objtype] != old_srcds_info[objtype]:
                        # the old record references another file
                        # -> mark for deletion
                        # Rational: it could be that the exact some dataset
                        # (very same commit) appears as two sub(sub)dataset at
                        # different locations. This would mean we have to scan
                        # the entire DB for potential match and not simply
                        # delete it here instead we have to gather candidate
                        # for deletion and check that they are no longer
                        # referenced at the very end of the DB update
                        obsolete_objs.append(ut.Path(old_srcds_info[objtype]))
            # replace the record
            top_agginfo_db[srcds] = agginfo_db[srcds]

        # we are done with moving new stuff into the store, clean our act up
        if aggtmp_basedir.exists():
            rmtree(text_type(aggtmp_basedir))

        # TODO THIS NEEDS A TEST
        obsolete_objs = [
            obj for obj in obsolete_objs
            if all(all(dinfo.get(objtype, None) != obj
                       for objtype in ('dataset_info', 'content_info'))
                   for d, dinfo in iteritems(top_agginfo_db))
        ]
        for obsolete_obj in obsolete_objs:
            # remove from the object store
            # there is no need to fiddle with `remove()`, rev-save will do that
            # just fine on its own
            lgr.debug("Remove obsolete metadata object %s", obsolete_obj)
            obsolete_obj.unlink()
            try:
                # make an attempt to kill the parent dir too, to leave the
                # object store clean(er) -- although git won't care
                # catch error, in case there is more stuff in the dir
                obsolete_obj.parent.rmdir()
            except OSError:
                # this would be expected and nothing to make a fuzz about
                pass
        # store the updated DB
        _store_agginfo_db(ds, top_agginfo_db)

        # and finally save the beast
        for res in Save()(
                dataset=ds,
                # be explicit, because we have to take in untracked content,
                # and there might be cruft lying around
                path=[
                    ds.pathobj / '.datalad' / 'metadata' / 'aggregate_v1.json',
                    ds.pathobj / '.datalad' / 'metadata' / 'objects',
                ],
                message="Update aggregated metadata",
                # never recursive, this call might be triggered from a more
                # complex algorithm that does a "better" recursion and there
                # should be nothing to recurse into for the given paths
                recursive=False,
                # we need to capture new/untracked content
                updated=False,
                # leave this decision to the dataset config
                to_git=None):
            # TODO inspect these results to figure out if anything was actually
            # done
            yield res
        # TODO yield OK or NOTNEEDED result


def _get_aggtmp_basedir(ds, mkdir=False):
    """Return a pathlib Path for a temp directory to put aggregated metadata"""
    tmp_basedir = ds.pathobj / '.git' / 'tmp' / 'aggregate-metadata'
    if mkdir:
        tmp_basedir.mkdir(parents=True, exist_ok=True)
    return tmp_basedir


def _extract_metadata(fromds, tods):
    """Extract metadata from a dataset into a temporary location in a dataset

    Parameters
    ----------
    fromds : Dataset
      To extract from
    tods : Dataset
      Aggregate into
    info : dict
      Will be modified in-place and receive the info on the aggregation
      (version, extractors, dataset and content metadata object file locations)

    Yields
    ------
    dict
      Any extraction error status results will be re-yielded
    """
    # this will gather information on the extraction result
    info = {}
    meta = {
        'dataset': None,
        'content': [],
    }
    extracted_metadata_sources = set()

    # perform the actual extraction
    for res in fromds.rev_extract_metadata(
            # just let it do its thing
            path=None,
            # None indicates to honor a datasets per-extractor configuration
            # and to be on by default
            process_type=None,
            # error handlingis done upstairs
            on_failure='ignore',
            return_type='generator'):
        if success_status_map.get(res['status'], False) != 'success':
            yield res
            continue
        restype = res.get('type', None)
        extracted_metadata_sources = extracted_metadata_sources.union(
            # assumes that any non-JSONLD-internal key is a metadata
            # extractor, which should be valid
            (k for k in res.get('metadata', {}) if not k.startswith('@')))
        if restype == 'dataset':
            if meta['dataset'] is not None:
                res.update(
                    message=(
                        'Metadata extraction from %s yielded second dataset '
                        'metadata set, ignored',
                        fromds),
                    status='error',
                )
                yield res
                continue
            meta['dataset'] = res['metadata']
        elif restype == 'file':
            meta['content'].append(
                dict(
                    res['metadata'],
                    path=op.relpath(res['path'], start=fromds.path)
                )
            )
        else:
            res.update(
                message=(
                    'Metadata extraction from %s yielded unexpected '
                    'result type (%s), ignored record',
                    fromds, restype),
                status='error',
            )
            yield res
            continue
    # store esssential extraction config in dataset record
    info['datalad_version'] = datalad.__version__
    # instead of reporting what was enabled, report what was actually retrieved
    info['extractors'] = sorted(extracted_metadata_sources)

    # place recorded refcommit in info dict to facilitate subsequent
    # change detection
    refcommit = \
        meta.get('dataset', {}).get('datalad_core', {}).get('refcommit', None) \
        if meta.get('dataset', None) is not None else None
    if refcommit:
        info['refcommit'] = refcommit

    # create a tempdir for this dataset under .git/tmp
    tmp_basedir = _get_aggtmp_basedir(tods, mkdir=True)
    tmpdir = tempfile.mkdtemp(
        dir=text_type(tmp_basedir),
        prefix=fromds.id + '_')

    # for both types of metadata
    for label, props in iteritems(meta):
        if not meta[label]:
            # we got nothing from extraction
            continue

        tmp_obj_fname = op.join(tmpdir, '{}.xz'.format(label))
        # place JSON dump of the metadata into this dir
        (json_py.dump if label == 'dataset' else json_py.dump2stream)(
            meta[label], tmp_obj_fname, compressed=True)

        # place info on objects into info dict
        info['{}_info'.format(label)] = tmp_obj_fname

    # do not place the files anywhere, just report where they are
    yield dict(
        path=fromds.path,
        type='dataset',
        action='extract_metadata',
        status='ok',
        info=info,
        logger=lgr,
    )


def _get_obj_location(ds, srcfile, hash_str):
    """Determine the location of a metadata object in a dataset's object store

    Parameters
    ----------
    ds : Dataset
      The reference dataset whose store shall be used.
    srcfile : Path
      The path of the object's sourcefile (to determine the correct
      file extension.
    hash_str : str
      The hash the object should be stored under.

    Returns
    -------
    Path
      pathlib Path instance to the absolute location for the metadata object
    """
    objpath = \
        ds.pathobj / '.datalad' / 'metadata' / 'objects' / \
        hash_str[:2] / (hash_str[2:] + srcfile.suffix)

    return objpath


def _store_agginfo_db(ds, db):
    # base path in which aggregate.json and objects is located
    # TODO avoid this call
    agginfo_path, agg_base_path = get_ds_aggregate_db_locations(
        ds, warn_absent=False)
    # make DB paths on disk always relative
    json_py.dump(
        {
            op.relpath(p, start=ds.path):
            {k: op.relpath(v, start=agg_base_path) if k in location_keys else v
             for k, v in props.items()}
            for p, props in db.items()
        },
        agginfo_path
    )
