# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for Datalad's own core storage"""

# TODO dataset metadata
# - known annex UUIDs
# - avoid anything that is specific to a local clone
#   (repo mode, etc.) limit to description of dataset(-network)

from .base import MetadataExtractor
from datalad.utils import (
    Path,
)

import logging
lgr = logging.getLogger('datalad.metadata.extractors.datalad_core')
from datalad.log import log_progress
from datalad.support.constraints import EnsureBool

import os.path as op


class DataladCoreExtractor(MetadataExtractor):
    # reporting unique file sizes has no relevant use case that I can think of
    # identifiers are included explicitly
    _unique_exclude = {'contentbytesize', 'identifier'}

    def __call__(self, dataset, process_type, status):
        # shortcut
        ds = dataset

        log_progress(
            lgr.info,
            'extractordataladcore',
            'Start core metadata extraction from %s', ds,
            total=len(status) + 1,
            label='Core metadata extraction',
            unit=' Files',
        )
        total_content_bytesize = 0
        if process_type in ('all', 'content'):
            for res in self._get_contentmeta(ds, status):
                total_content_bytesize += res['metadata'].get(
                    'contentbytesize', 0)
                yield dict(
                    res,
                    type='file',
                    status='ok',
                )
        if process_type in ('all', 'dataset'):
            dsmeta = self._get_dsmeta(ds, status, process_type)
            log_progress(
                lgr.info,
                'extractordataladcore',
                'Extracted core metadata from %s', ds.path,
                update=1,
                increment=True)
            if total_content_bytesize:
                dsmeta['contentbytesize'] = total_content_bytesize
            yield dict(
                metadata=dsmeta,
                type='dataset',
                status='ok',
            )
        log_progress(
            lgr.info,
            'extractordataladcore',
            'Finished core metadata extraction from %s', ds
        )

    def _get_dsmeta(self, ds, status, process_type):
        meta = {
            # the desired ID
            'identifier': ds.id,
        }
        meta.update(_get_commit_info(ds, status))
        parts = [{
            # this version would change anytime we aggregate metadata,
            # let's not do this for now
            #'@id': sds['gitshasum'],
            # TODO check that this is the right type term
            # TODO should this be @type
            'type': 'Dataset' if part['type'] == 'dataset' else 'File',
            # relativ path within dataset, always POSIX
            'name': Path(part['path']).relative_to(ds.pathobj).as_posix(),
        }
            for part in status
            # if we are processing everything we do not need to know about
            # files, they will have their own reports
            # but if we are only looking at the dataset, we report the files
            # here, to have at least their names
            if process_type == 'dataset' or part['type'] == 'dataset'
        ]
        if parts:
            meta['hasPart'] = parts
        if ds.config.obtain(
                'datalad.metadata.datalad-core.report-remotes',
                True, valtype=EnsureBool()):
            remote_names = ds.repo.get_remotes()
            distributions = []
            for r in remote_names:
                url = ds.config.obtain('remote.{}.url'.format(r), None)
                if url:
                    distributions.append({
                        'name': r,
                        'url': url,
                        # not very informative
                        #'description': 'DataLad dataset sibling',
                    })
            if len(distributions):
                meta['distribution'] = distributions
        return meta

    def _get_contentmeta(self, ds, status):
        """Get ALL metadata for all dataset content.

        Returns
        -------
        generator((location, metadata_dict))
        """
        for rec in status:
            log_progress(
                lgr.info,
                'extractordataladcore',
                'Extracted core metadata from %s', rec['path'],
                update=1,
                increment=True)
            if rec['type'] == 'dataset':
                # subdatasets have been dealt with in the dataset metadata
                continue
            yield dict(
                path=rec['path'],
                metadata=self._describe_file(rec),
            )

    def _describe_file(self, rec):
        info = {
            # prefer the annex key, but fall back on the git shasum that is
            # always around, identify the GITSHA as such in a similar manner
            # to git-annex's style
            'identifier': rec['key']
            if 'key' in rec else 'SHA1-s{}--{}'.format(
                rec['bytesize'],
                rec['gitshasum']),
            # schema.org doesn't have a useful term, only contentSize
            # and fileSize which seem to be geared towards human consumption
            # not numerical accuracy
            # TODO define the term
            'contentbytesize': rec['bytesize']
            if 'bytesize' in rec else op.getsize(rec['path']),
            # TODO the following list are optional enhancement that should come
            # with individual ON/OFF switches
            # TODO run `git log` to find earliest and latest commit to determine
            # 'dateModified' and 'dateCreated'
            # TODO determine per file 'contributor' from git log
        }
        return info


def _get_commit_info(ds, status):
    """Get info about all commits, up to (and incl. the refcommit)"""
    #- get all the commit info with git log --pretty='%aN%x00%aI%x00%H'
    #  - use all first-level paths other than .datalad and .git for the query
    #- from this we can determine all modification timestamps, described refcommit
    #- do a subsequent git log query for the determined refcommit to determine
    #  a version by counting all commits since inception up to the refcommit
    #  - we cannot use the first query, because it will be constrained by the
    #    present paths that may not have existed previously at all

    # determine the commit that we are describing
    # build a compact path list (take all top-level paths)
    # `status` will already not contain any to be ignored content
    # but if we call git-log without paths, we do not get the
    # desired answer
    refcommit = ds.repo.get_last_commit_hash(
        list(set(
            Path(s['path']).relative_to(ds.pathobj).parts[0]
            for s in status))
    )

    # grab the history until the refcommit
    stdout, stderr = ds.repo._git_custom_command(
        None,
        # name, email, timestamp, shasum
        ['git', 'log', '--pretty=format:%aN%x00%aE%x00%aI%x00%H', refcommit],
        expect_fail=True)
    commits = [line.split('\0') for line in stdout.splitlines()]
    # version, always anchored on the first commit (tags could move and
    # make the integer commit count ambigous, and subtantially complicate
    # version comparisons
    version = '0-{}-g{}'.format(
        len(commits),
        # first seven chars of the shasum (like git-describe)
        commits[0][3][:7],
    )
    meta = {
        'version': version,
        # the true ID of this version of this dataset
        'refcommit': refcommit,
    }
    if ds.config.obtain(
            'datalad.metadata.datalad-core.report-authors',
            True, valtype=EnsureBool()):
        meta.update(
            authors=sorted(set('{} <{}>'.format(c[0], c[1]) for c in commits)))
    if ds.config.obtain(
            'datalad.metadata.datalad-core.report-modification-dates',
            True, valtype=EnsureBool()):
        meta.update(
            dateCreated=commits[-1][2],
            dateModified=commits[0][2],
        )
    return meta
