

from six import iteritems
from datalad.utils import (
    PurePosixPath,
)
from datalad.consts import PRE_INIT_COMMIT_SHA

aggregate_layout_version = 1

# relative paths which to exclude from any metadata processing
# including anything underneath them
# POSIX conventions (if needed)
exclude_from_metadata = ('.datalad', '.git', '.gitmodules', '.gitattributes')

# TODO filepath_info is obsolete
location_keys = ('dataset_info', 'content_info', 'filepath_info')


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
    precommit = False
    while True:
        cur = 'HEAD~{:d}'.format(count)
        try:
            # get the diff between the next pair of previous commits
            diff = {
                p.relative_to(ds.repo.pathobj): props
                for p, props in iteritems(ds.repo.diffstatus(
                    PRE_INIT_COMMIT_SHA
                    if precommit
                    else 'HEAD~{:d}'.format(count + 1),
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
            if precommit:
                # end of things
                return None
            else:
                # one last round, taking in the entire history
                precommit = True
                continue
        if diff:
            return ds.repo.get_hexsha(cur)
        # next pair
        count += 1
