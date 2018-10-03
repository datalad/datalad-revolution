from six import iteritems

from datalad_revolution.gitrepo import RevolutionGitRepo as GitRepo
from datalad_revolution.annexrepo import RevolutionAnnexRepo as AnnexRepo

from datalad.tests.utils import (
    eq_,
    assert_is,
)


def assert_repo_status(path, annex=None, untracked_mode='normal', **kwargs):
    """Compare a repo status against (optional) exceptions.

    Anything file/directory that is not explicitly indicated must have
    state 'clean', i.e. no modifications and recorded in Git.

    This is an alternative to the traditional `ok_clean_git` helper.

    Parameters
    ----------
    path: str or Repo
      in case of a str: path to the repository's base dir;
      Note, that passing a Repo instance prevents detecting annex. This might
      be useful in case of a non-initialized annex, a GitRepo is pointing to.
    annex: bool or None
      explicitly set to True or False to indicate, that an annex is (not)
      expected; set to None to autodetect, whether there is an annex.
      Default: None.
    untracked_mode: {'no', 'normal', 'all'}
      If and how untracked content is reported. The specification of untracked
      files that are OK to be found must match this mode. See `Repo.status()`
    **kwargs
      Files/directories that are OK to not be in 'clean' state. Each argument
      must be one of 'added', 'untracked', 'deleted', 'modified' and each
      value must be a list of filenames (relative to the root of the
      repository.
    """
    r = None
    if isinstance(path, AnnexRepo):
        if annex is None:
            annex = True
        # if `annex` was set to False, but we find an annex => fail
        assert_is(annex, True)
        r = path
    elif isinstance(path, GitRepo):
        if annex is None:
            annex = False
        # explicitly given GitRepo instance doesn't make sense with
        # 'annex' True
        assert_is(annex, False)
        r = path
    else:
        # 'path' is an actual path
        try:
            r = AnnexRepo(path, init=False, create=False)
            if annex is None:
                annex = True
            # if `annex` was set to False, but we find an annex => fail
            assert_is(annex, True)
        except Exception:
            # Instantiation failed => no annex
            try:
                r = GitRepo(path, init=False, create=False)
            except Exception:
                raise AssertionError("Couldn't find an annex or a git "
                                     "repository at {}.".format(path))
            if annex is None:
                annex = False
            # explicitly given GitRepo instance doesn't make sense with
            # 'annex' True
            assert_is(annex, False)

    status = r.status(untracked=untracked_mode)
    # for any file state that indicates some kind of change (all but 'clean)
    for state in ('added', 'untracked', 'deleted', 'modified'):
        oktobefound = kwargs.get(state, [])
        state_files = [k for k, v in iteritems(status)
                       if v.get('state', None) == state]
        eq_(sorted(state_files), sorted(oktobefound))
