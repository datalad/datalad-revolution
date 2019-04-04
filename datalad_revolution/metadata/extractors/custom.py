# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for custom (JSON-LD) metadata contained in a dataset

One or more source files with metadata can be specified via the
'datalad.metadata.custom-dataset-source' configuration variable.
The content of these files must e a JSON object, and a metadata
dictionary is built by updating it with the content of the JSON
objects in the order in which they are given.

By default a single file is read: '.datalad/custom_metadata.json'
"""

from .base import MetadataExtractor

import logging
lgr = logging.getLogger('datalad.metadata.extractors.custom')
from datalad.utils import (
    assure_list,
)

from datalad.support.json_py import load as jsonload
# TODO test the faith of this one
from datalad.metadata.definitions import version as vocabulary_version

from ... import utils as ut


class CustomMetadataExtractor(MetadataExtractor):
    def __call__(self, dataset, process_type, status):
        if process_type not in ('all', 'dataset'):
            # ATM we only deal with dataset metadata
            return

        # shortcut
        ds = dataset

        # which files to look at
        cfg_srcfiles = ds.config.obtain(
            'datalad.metadata.custom-dataset-source',
            [])
        cfg_srcfiles = assure_list(cfg_srcfiles)
        # OK to be always POSIX
        srcfiles = ['.datalad/custom_metadata.json'] \
            if not cfg_srcfiles else cfg_srcfiles
        dsmeta = {}
        for srcfile in srcfiles:
            abssrcfile = ds.pathobj / ut.PurePosixPath(srcfile)
            # TODO get annexed files, or do in a central place?
            if not abssrcfile.exists():
                # nothing to load
                # warn if this was configured
                if srcfile in cfg_srcfiles:
                    yield dict(
                        type='dataset',
                        status='impossible',
                        message=(
                            'configured custom metadata source is not '
                            'available in %s: %s',
                            ds, srcfile),
                    )
                    # no further operation on half-broken metadata
                    return
                continue
            lgr.debug('Load custom metadata from %s', abssrcfile)
            meta = jsonload(str(abssrcfile))
            dsmeta.update(meta)
        yield dict(
            metadata=dsmeta,
            type='dataset',
            status='ok',
        )
