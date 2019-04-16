# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for Git-annex metadata

This extractor only deals with the metadata that can be assigned to annexed
files via git-annex's `metadata` command. It does not deal with other implicit
git-annex metadata, such as file availability information. This is already
handles by the `datalad_core` extractor.

There is no standard way to define a vocabulary that is used for this kind of
metadata.
"""


from .base import MetadataExtractor

from six import text_type
import logging
lgr = logging.getLogger('datalad.metadata.extractors.annexmeta')
from datalad.utils import (
    Path,
    PurePosixPath,
)
from datalad.support.annexrepo import AnnexRepo


class AnnexMetadataExtractor(MetadataExtractor):
    def __call__(self, dataset, process_type, status):
        # shortcut
        ds = dataset

        repo = ds.repo   # OPT: .repo could be relatively expensive
        if not isinstance(repo, AnnexRepo):
            # nothing to be done
            return

        if process_type in ('all', 'content'):
            # no progress bar, we are only making a one-shot call to
            # annex, the rest is pretty much instantaneous

            # limit query to paths that are annexed
            query_paths = [
                # go relative to minimize cmdline footprint of annex call
                text_type(Path(s['path']).relative_to(ds.pathobj))
                for s in status
                # anything that looks like an annexed file
                if s.get('type', None) == 'file'
                and s.get('key', None) is not None
            ]

            for fpath, meta in repo.get_metadata(
                    query_paths,
                    # no timestamps, we are describing the status quo
                    timestamps=False):
                meta = {
                    k:
                    v[0] if isinstance(v, list) and len(v) == 1 else v
                    for k, v in meta.items()}
                if not meta:
                    # only talk about files that actually carry metadata
                    continue
                yield dict(
                    # git annex reports the path in POSIX conventions
                    path=PurePosixPath(fpath),
                    metadata=meta,
                    type='file',
                    status='ok',
                )

    def get_state(self, dataset):
        #from datalad.support.external_versions import external_versions
        return dict(
            # report on the annex version used to report metadata
            #gitannex_version=external_versions['cmd:annex']
            # do not report on the git-annex version itself, as
            # any git-annex update would trigger a re-extraction of metadata
            # most likely without any change.
            # Instead, increment/change this version whenever the extractor
            # output would change in the further (maybe due to changes in
            # git-annex, or for other reasons).
            version=1,
        )
