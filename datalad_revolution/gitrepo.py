__docformat__ = 'restructuredtext'


from collections import OrderedDict
import logging
import re
from six import iteritems

import datalad_revolution.utils as ut
from datalad.support.gitrepo import GitRepo

lgr = logging.getLogger('datalad.revolution.gitrepo')

obsolete_methods = (
    'dirty',
    'is_dirty',
)


class RevolutionGitRepo(GitRepo):
    def __init__(self, *args, **kwargs):
        super(RevolutionGitRepo, self).__init__(*args, **kwargs)
        # the sole purpose of this init is to add a pathlib
        # native path object to the instance
        # XXX this relies on the assumption that self.path as managed
        # by the base class is always a native path
        self.pathobj = ut.Path(self.path)

    def get_content_info(self, paths=None, ref=None, untracked='all'):
        """Get identifier and type information from repository content.

        This is simplified front-end for `git ls-files/tree`.

        Parameters
        ----------
        paths : list
          Specific paths to query info for. In none are given, info is
          reported for all content.
        ref : gitref or None
          If given, content information is retrieved for this Git reference
          (via ls-tree), otherwise content information is produced for the
          present work tree (via ls-files).
        untracked : {'no', 'normal', 'all'}
          If and how untracked content is reported when no `ref` was given:
          'no': no untracked files are reported; 'normal': untracked files
          and entire untracked directories are reported as such; 'all': report
          individual files even in fully untracked directories.

        Returns
        -------
        dict
          Each content item has an entry under its relative path within
          the repository. Each value is a dictionary with properties:

          `type`
            Can be 'file', 'symlink', 'dataset', 'directory'
          `gitshasum`
            SHASUM of the item as tracked by Git, or None, if not
            tracked. This could be different from the SHASUM of the file
            in the worktree, if it was modified.
        """
        # TODO limit by file type to replace code in subdatasets command
        info = OrderedDict()

        mode_type_map = {
            '100644': 'file',
            '100755': 'file',
            '120000': 'symlink',
            '160000': 'dataset',
        }

        # this will not work in direct mode, but everything else should be
        # just fine
        if not ref:
            cmd = ['git', 'ls-files', '--stage', '-z', '-d', '-m']
            # untracked report mode, using labels from `git diff` option style
            if untracked == 'all':
                cmd.append('-o')
            elif untracked == 'normal':
                cmd += ['-o', '--directory']
            elif untracked == 'no':
                pass
            else:
                raise ValueError(
                    'unknown value for `untracked`: %s', untracked)
        else:
            cmd = ['git', 'ls-tree', ref, '-z', '-r', '--full-tree']
        # works for both modes
        props_re = re.compile(r'([0-9]+) (.*) (.*)\t(.*)$')

        stdout, stderr = self._git_custom_command(
            paths if paths else [],
            cmd,
            log_stderr=True,
            log_stdout=True,
            # not sure why exactly, but log_online has to be false!
            log_online=False,
            expect_stderr=False,
            shell=False,
            # we don't want it to scream on stdout
            expect_fail=True)

        for line in stdout.split('\0'):
            if not line:
                continue
            inf = {}
            props = props_re.match(line)
            if not props:
                # not known to Git, but Git always reports POSIX
                path = ut.PurePosixPath(line)
                inf['gitshasum'] = None
            else:
                # again Git reports always in POSIX
                path = ut.PurePosixPath(props.group(4))
                inf['gitshasum'] = props.group(2 if not ref else 3)
                inf['type'] = mode_type_map.get(
                    props.group(1), props.group(1))

            # join item path with repo path to get a universally useful
            # path representation with auto-conversion and tons of other
            # stuff
            path = self.pathobj.joinpath(path)
            if 'type' not in inf:
                # be nice and assign types for untracked content
                inf['type'] = 'directory' if path.is_dir() \
                    else 'symlink' if path.is_symlink() else 'file'
            info[path] = inf
        return info

    def status(self, paths=None, untracked='all', ignore_submodules='no'):
        """Simplified `git status` equivalent.

        Performs a comparison of a get_content_info(stat_wt=True) with a
        get_content_info(ref='HEAD').

        Importantly, this function will not detect modified subdatasets.
        This would require recursion into present subdatasets and query
        their status. This is left to higher-level commands.

        Parameters
        ----------
        untracked : {'no', 'normal', 'all'}
          If and how untracked content is reported when no `ref` was given:
          'no': no untracked files are reported; 'normal': untracked files
          and entire untracked directories are reported as such; 'all': report
          individual files even in fully untracked directories.
        ignore_submodules : {'no', 'other', 'all'}

        Returns
        -------
        dict
          Each content item has an entry under its relative path within
          the repository. Each value is a dictionary with properties:

          `type`
            Can be 'file', 'symlink', 'dataset', 'directory'
          `state`
            Can be 'added', 'untracked', 'clean', 'deleted', 'modified'.
        """
        # TODO report more info from get_content_info() calls in return
        # value, those are cheap and possibly useful to a consumer
        status = OrderedDict()
        # we need three calls to git
        # 1. everything we know about the worktree, including os.stat
        # for each file
        wt = self.get_content_info(
            paths=paths, ref=None, untracked=untracked)
        # 2. the last committed state
        head = self.get_content_info(paths=paths, ref='HEAD')
        # 3. we want Git to tell us what it considers modified and avoid
        # reimplementing logic ourselves
        modified = set(
            self.pathobj.joinpath(ut.PurePosixPath(p))
            for p in self._git_custom_command(
                paths, ['git', 'ls-files', '-z', '-m'])[0].split('\0')
            if p)

        for f, wt_r in iteritems(wt):
            props = None
            if f not in head:
                # this is new, or rather not known to the previous state
                props = dict(
                    state='added' if wt_r['gitshasum'] else 'untracked',
                    type=wt_r['type'],
                )
            elif wt_r['gitshasum'] == head[f]['gitshasum'] and \
                    f not in modified:
                if ignore_submodules != 'all' or wt_r['type'] != 'dataset':
                    # no change in git record, and no change on disk
                    props = dict(
                        state='clean' if f.exists() or
                              f.is_symlink() else 'deleted',
                        type=wt_r['type'],
                    )
            else:
                # change in git record, or on disk
                props = dict(
                    # TODO is 'modified' enough, should be report typechange?
                    # often this will be a pointless detail, though...
                    # TODO we could have a new file that is already staged
                    # but had subsequent modifications done to it that are
                    # unstaged. Such file would presently show up as 'added'
                    # ATM I think this is OK, but worth stating...
                    state='modified' if f.exists() or
                    f.is_symlink() else 'deleted',
                    # TODO record before and after state for diff-like use
                    # cases
                    type=wt_r['type'],
                )
            if props['state'] in ('clean', 'added'):
                props['gitshasum'] = wt_r['gitshasum']
            status[f] = props

        if ignore_submodules == 'all':
            return status

        # loop over all subdatasets and look for additional modifications
        for f, st in iteritems(status):
            if not (st['type'] == 'dataset' and st['state'] == 'clean' and
                    GitRepo.is_valid_repo(str(f))):
                # no business here
                continue
            # we have to recurse into the dataset and get its status
            subrepo = RevolutionGitRepo(str(f))
            # subdataset records must be labeled clean up to this point
            if st['gitshasum'] != subrepo.get_hexsha():
                # current commit in subdataset deviates from what is
                # recorded in the dataset, cheap test
                st['state'] = 'modified'
            else:
                # the recorded commit did not change, so we need to make
                # a more expensive traversal
                rstatus = subrepo.status(
                    paths=None,
                    untracked=untracked,
                    # TODO could be RF'ed to stop after the first find
                    # of a modified subdataset
                    # ATM implementation performs an exhaustive search
                    ignore_submodules='other')
                if any(v['state'] != 'clean'
                       for k, v in iteritems(rstatus)):
                    st['state'] = 'modified'
            if ignore_submodules == 'other' and st['state'] == 'modified':
                # we know for sure that at least one subdataset is modified
                # go home quick
                break
        return status


# remove deprecated methods from API
for m in obsolete_methods:
    if hasattr(RevolutionGitRepo, m):
        setattr(RevolutionGitRepo, m, ut.nothere)
