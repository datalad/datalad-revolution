__docformat__ = 'restructuredtext'

from six import string_types
import logging
from pathlib import Path

from datalad.distribution.dataset import Dataset
from datalad.support.constraints import Constraint
from datalad.dochelpers import exc_str
from datalad.support.gitrepo import (
    InvalidGitRepositoryError,
    NoSuchPathError,
)

from datalad_revolution.gitrepo import RevolutionGitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo
from datalad_revolution.utils import nothere

lgr = logging.getLogger('datalad.revolution.dataset')


class RevolutionDataset(Dataset):
    @property
    def pathobj(self):
        """pathobj for the dataset"""
        # XXX this relies on the assumption that self._path as managed
        # by the base class is always a native path
        return Path(self._path)

    @property
    def repo(self):
        """Get an instance of the version control system/repo for this dataset,
        or None if there is none yet.

        If creating an instance of GitRepo is guaranteed to be really cheap
        this could also serve as a test whether a repo is present.

        Returns
        -------
        GitRepo
        """

        # Note: lazy loading was disabled, since this is provided by the
        # flyweight pattern already and a possible invalidation of an existing
        # instance has to be done therein.
        # TODO: Still this is somewhat problematic. We can't invalidate strong
        # references

        for cls, ckw, kw in (
                # TODO: Do we really want to allow_noninitialized=True here?
                # And if so, leave a proper comment!
                (RevolutionAnnexRepo, {'allow_noninitialized': True}, {'init': False}),
                (RevolutionGitRepo, {}, {})
        ):
            if cls.is_valid_repo(self._path, **ckw):
                try:
                    lgr.log(5, "Detected %s at %s", cls, self._path)
                    self._repo = cls(self._path, create=False, **kw)
                    break
                except (InvalidGitRepositoryError, NoSuchPathError) as exc:
                    lgr.log(5,
                            "Oops -- guess on repo type was wrong?: %s",
                            exc_str(exc))
                    pass
                # version problems come as RuntimeError: DO NOT CATCH!
        if self._repo is None:
            # Often .repo is requested to 'sense' if anything is installed
            # under, and if so -- to proceed forward. Thus log here only
            # at DEBUG level and if necessary "complaint upstairs"
            lgr.log(5, "Failed to detect a valid repo at %s", self.path)

        return self._repo


# remove deprecated method from API
setattr(RevolutionDataset, 'get_subdatasets', nothere)


# Note: Cannot be defined within constraints.py, since then dataset.py needs to
# be imported from constraints.py, which needs to be imported from dataset.py
# for another constraint
class EnsureDataset(Constraint):

    def __call__(self, value):
        if isinstance(value, Dataset):
            return value
        elif isinstance(value, string_types):
            return RevolutionDataset(path=value)
        else:
            raise ValueError("Can't create Dataset from %s." % type(value))

    def short_description(self):
        return "Dataset"

    def long_description(self):
        return """Value must be a Dataset or a valid identifier of a Dataset
        (e.g. a path)"""


