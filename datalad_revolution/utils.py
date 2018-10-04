
# handle this dance once, and import pathlib from here
# in all other places
try:
    from pathlib import (
        Path,
        PurePosixPath,
    )
except ImportError:
    from pathlib2 import (
        Path,
        PurePosixPath,
    )


def nothere(*args, **kwargs):
    raise NotImplementedError


def saveds(ds, paths=None, _status=None):
    """Save content in a single dataset.

    Parameters
    ----------
    ds : Dataset
      An instance of the dataset the content should be saved in.
    paths : list or None
      Any content with path matching any of the paths given in this
      list will be saved. Matching will be performed against the
      dataset status (GitRepo.status()), or a custom status provided
      via `_status`. If no paths are provided, ALL non-clean paths
      present in the repo status or `_status` will be saved.
    _status : dict or None
      If None, Repo.status() will be queried for the given `ds`. If
      a dict is given, its content will be used as a constrain.
      For example, to save only modified content, but no untracked
      content, set `paths` to None and provide a `_status` that has
      no entries for untracked content.
    """
    pass
