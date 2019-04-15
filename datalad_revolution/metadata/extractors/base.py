# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor base class"""


class MetadataExtractor(object):
    # ATM this doesn't do anything, but inheritance from this class enables
    # detection of new-style extractor API
    pass

    def __call__(self, dataset, process_type, status):
        """Run metadata extraction

        Any implementation gets a comprehensive description of a dataset
        via the `status` argument. In many scenarios this can prevent
        needless interaction with the dataset on disk, or specific
        further queries via dataset or repository methods.

        Parameters
        ----------
        dataset : Dataset
          Dataset instance to extract metadata from.
        process_type : {'all', 'dataset', 'content'}
          Type of metadata to extract.
        status : list
          Status records produced by the `status` command for the given
          dataset. Records are filtered to not contain any untracked
          content, or any files that are to be ignored for the purpose
          of metadata extraction (e.g. content under .dataset/metadata).
          There are only records on content within the given dataset, not
          about content of any existing subdatasets.
        """
        raise NotImplementedError  # pragma: no cover

    def get_required_content(self, dataset, process_type, status):
        """Report records for dataset content that must be available locally

        Any implementation can yield records in the given `status` that
        correspond to dataset content that must be available locally for an
        extractor to perform its work. It is acceptable to not yield such a
        record, or no records at all. In such case, the extractor is expected
        to handle the case of non-available content in some sensible way
        internally.

        The parameters are identical to those of
        `MetadataExtractor.__call__()`.

        Any content corresponding to a yielded record will be obtained
        automatically before metadata extraction is initiated. Hence any
        extractor reporting accurately can expect all relevant content
        to be present locally.

        Instead of a status record, it is also possible to return custom
        dictionaries that must contain a 'path' key, containing the absolute
        path to the required file within the given dataset.

        Example implementation

        ```
        for s in status:
            if s['path'].endswith('.pdf'):
                yield s
        ```
        """
        # be default an individual extractor is expected to manage
        # availability on its own
        return []


# XXX this is the legacy interface, keep around for a bit more and then
# remove
class BaseMetadataExtractor(object):  # pragma: no cover

    NEEDS_CONTENT = True   # majority of the extractors need data content

    def __init__(self, ds, paths):
        """
        Parameters
        ----------
        ds : dataset instance
          Dataset to extract metadata from.
        paths : list
          Paths to investigate when extracting content metadata
        """

        self.ds = ds
        self.paths = paths

    def get_metadata(self, dataset=True, content=True):
        """
        Returns
        -------
        dict or None, dict or None
          Dataset metadata dict, dictionary of filepath regexes with metadata,
          dicts, each return value could be None if there is no such metadata
        """
        # default implementation
        return \
            self._get_dataset_metadata() if dataset else None, \
            ((k, v) for k, v in self._get_content_metadata()) if content else None

    def _get_dataset_metadata(self):
        """
        Returns
        -------
        dict
          keys and values are arbitrary
        """
        raise NotImplementedError

    def _get_content_metadata(self):
        """Get ALL metadata for all dataset content.

        Possibly limited to the paths given to the extractor.

        Returns
        -------
        generator((location, metadata_dict))
        """
        raise NotImplementedError
