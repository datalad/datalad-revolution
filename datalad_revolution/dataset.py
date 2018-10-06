__docformat__ = 'restructuredtext'

from six import string_types
from six import PY2
import wrapt
import logging
import datalad_revolution.utils as ut

from datalad.distribution.dataset import Dataset as _Dataset
from datalad.support.constraints import Constraint
from datalad.dochelpers import exc_str
from datalad.support.gitrepo import (
    InvalidGitRepositoryError,
    NoSuchPathError,
)

from datalad.utils import optional_args

from datalad_revolution.gitrepo import RevolutionGitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo

lgr = logging.getLogger('datalad.revolution.dataset')


class RevolutionDataset(_Dataset):
    @property
    def pathobj(self):
        """pathobj for the dataset"""
        # XXX this relies on the assumption that self._path as managed
        # by the base class is always a native path
        return ut.Path(self._path)

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
setattr(RevolutionDataset, 'get_subdatasets', ut.nothere)


@optional_args
def datasetmethod(f, name=None, dataset_argname='dataset'):
    """Decorator to bind functions to Dataset class.

    The decorated function is still directly callable and additionally serves
    as method `name` of class Dataset.  To achieve this, the first positional
    argument is redirected to original keyword argument 'dataset_argname'. All
    other arguments stay in order (and keep their names, of course). That
    means, that the signature of the bound function is name(self, a, b) if the
    original signature is name(a, dataset, b) for example.

    The decorator has no effect on the actual function decorated with it.
    """
    if not name:
        name = f.func_name if PY2 else f.__name__

    @wrapt.decorator
    def apply_func(wrapped, instance, args, kwargs):
        # Wrapper function to assign arguments of the bound function to
        # original function.
        #
        # Note
        # ----
        # This wrapper is NOT returned by the decorator, but only used to bind
        # the function `f` to the Dataset class.

        kwargs = kwargs.copy()
        from inspect import getargspec
        orig_pos = getargspec(f).args

        # If bound function is used with wrong signature (especially by
        # explicitly passing a dataset, let's raise a proper exception instead
        # of a 'list index out of range', that is not very telling to the user.
        if len(args) >= len(orig_pos):
            raise TypeError("{0}() takes at most {1} arguments ({2} given):"
                            " {3}".format(name, len(orig_pos), len(args),
                                          ['self'] + [a for a in orig_pos
                                                      if a != dataset_argname]))
        if dataset_argname in kwargs:
            raise TypeError("{}() got an unexpected keyword argument {}"
                            "".format(name, dataset_argname))
        kwargs[dataset_argname] = instance
        ds_index = orig_pos.index(dataset_argname)
        for i in range(0, len(args)):
            if i < ds_index:
                kwargs[orig_pos[i]] = args[i]
            elif i >= ds_index:
                kwargs[orig_pos[i+1]] = args[i]
        return f(**kwargs)

    setattr(RevolutionDataset, name, apply_func(f))
    return f


# Note: Cannot be defined within constraints.py, since then dataset.py needs to
# be imported from constraints.py, which needs to be imported from dataset.py
# for another constraint
class EnsureDataset(Constraint):

    def __call__(self, value):
        if isinstance(value, _Dataset):
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


