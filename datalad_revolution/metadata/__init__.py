
from collections import (
    Mapping,
)

from six import iteritems
from datalad.utils import (
    PurePosixPath,
)

aggregate_layout_version = 1

# relative paths which to exclude from any metadata processing
# including anything underneath them
# POSIX conventions (if needed)
exclude_from_metadata = ('.datalad', '.git', '.gitmodules', '.gitattributes')

# TODO filepath_info is obsolete
location_keys = ('dataset_info', 'content_info', 'filepath_info')


# this is the default context, but any node document can define
# something more suitable
default_context = {
    # schema.org definitions by default
    "@vocab": "http://schema.org/",
    # resolve non-compact/absolute identifiers to the DataLad
    # resolver
    "@base": "http://dx.datalad.org/",
}


def get_metadata_type(ds):
    """Return the metadata type(s)/scheme(s) of a dataset

    Parameters
    ----------
    ds : Dataset
      Dataset instance to be inspected

    Returns
    -------
    list(str)
      Metadata type labels or an empty list if no type setting is found and
      optional auto-detection yielded no results
    """
    cfg_key = 'datalad.metadata.nativetype'
    old_cfg_key = 'metadata.nativetype'
    if cfg_key in ds.config:
        return ds.config[cfg_key]
    # FIXME this next conditional should be removed once datasets at
    # datasets.datalad.org have received the metadata config update
    elif old_cfg_key in ds.config:
        return ds.config[old_cfg_key]
    return []


def get_refcommit(ds):
    """Get most recent commit that changes any metadata-relevant content.

    This function should be executed in a clean dataset, with no uncommitted
    changes (untracked is OK).

    Returns
    -------
    str or None
      None if there is no matching commit, a hexsha otherwise.
    """
    exclude_paths = [
        ds.repo.pathobj / PurePosixPath(e)
        for e in exclude_from_metadata
    ]
    count = 0
    diff_cache = {}
    while True:
        cur = 'HEAD~{:d}'.format(count)
        try:
            # get the diff between the next pair of previous commits
            diff = {
                p.relative_to(ds.repo.pathobj): props
                for p, props in iteritems(ds.repo.diffstatus(
                    'HEAD~{:d}'.format(count + 1),
                    cur,
                    # superfluous, but here to state the obvious
                    untracked='no',
                    # this should be OK, unit test covers the cases
                    # of subdataset addition, modification and removal
                    # refcommit evaluation only makes sense in a clean
                    # dataset, and if that is true, any change in the
                    # submodule record will be visible in the parent
                    # already
                    eval_submodule_state='no',
                    # boost performance, we don't care about file types
                    # here
                    eval_file_type=False,
                    _cache=diff_cache))
                if props.get('state', None) != 'clean' \
                and p not in exclude_paths \
                and not any(e in p.parents for e in exclude_paths)
            }
        except ValueError as e:
            # likely ran out of commits to check
            return None
        if diff:
            return ds.repo.get_hexsha(cur)
        # next pair
        count += 1


def get_refcommit_from_metadata(md):
    """Given a metadata record will report 'refcommit' of the dataset.

    Expected is a full metadata records (all extractors). Returns None if
    there is none.
    """
    dcmd = md.get('datalad_core', {})
    docs = dcmd['@graph'] if '@graph' in dcmd else [dcmd]
    for doc in docs:
        if doc.get('@type', None) == 'Dataset' and '@id' in doc:
            return doc['@id']


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
