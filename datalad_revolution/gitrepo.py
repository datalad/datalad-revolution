__docformat__ = 'restructuredtext'

from collections import OrderedDict
import logging
import os
import os.path as op
import re
from six import iteritems
import stat

from datalad.support.gitrepo import GitRepo

from datalad_revolution.utils import nothere

lgr = logging.getLogger('datalad_revolution.gitrepo')

obsolete_methods = (
    'dirty',
    'is_dirty',
)


class RevolutionGitRepo(GitRepo):
    def get_content_info(self, paths=None, ref=None, stat_wt=False,
                         untracked='all'):
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
        stat_wt : bool
          If given, reports the result of `os.lstat()` as `stat_wt` property
          for the work tree content.
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
                # not known to Git
                path = line.strip(op.sep)
                inf['gitshasum'] = None
            else:
                path = props.group(4).strip(op.sep)
                inf['gitshasum'] = props.group(2 if not ref else 3)
                inf['type'] = mode_type_map.get(
                    props.group(1), props.group(1))
            abspath_ = op.join(self.path, path)
            if stat_wt:
                if not op.lexists(abspath_):
                    inf['stat_wt'] = None
                else:
                    s = os.lstat(abspath_)
                    inf['stat_wt'] = s
                    if 'type' not in inf:
                        s = s.st_mode
                        if stat.S_ISDIR(s):
                            inf['type'] = 'directory'
                        elif stat.S_ISREG(s):
                            inf['type'] = 'file'
                        elif stat.S_ISLNK(s):
                            inf['type'] = 'symlink'

            info[path] = inf
        return info

    def status(self, paths=None, untracked='all'):
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
            paths=paths, ref=None, stat_wt=True, untracked=untracked)
        # 2. the last committed state
        head = self.get_content_info(paths=paths, ref='HEAD', stat_wt=False)
        # 3. we want Git to tell us what it considers modified and avoid
        # reimplementing logic ourselves
        modified = set(
            p for p in self._git_custom_command(
                paths, ['git', 'ls-files', '-z', '-m'])[0].split('\0')
            if p)

        for f, wt_r in iteritems(wt):
            if f not in head:
                # this is new, or rather not known to the previous state
                status[f] = dict(
                    state='added' if wt_r['gitshasum'] else 'untracked',
                    type=wt_r['type'],
                )
            elif wt_r['gitshasum'] == head[f]['gitshasum'] and f not in modified:
                # no change in git record, and no change on disk
                status[f] = dict(
                    state='clean' if wt_r['stat_wt'] else 'deleted',
                    type=wt_r['type'],
                )
            else:
                # change in git record, or on disk
                status[f] = dict(
                    # TODO is 'modified' enough, should be report typechange?
                    # often this will be a pointless detail, though...
                    # TODO we could have a new file that is already staged
                    # but had subsequent modifications done to it that are
                    # unstaged. Such file would presently show up as 'added'
                    # ATM I think this is OK, but worth stating...
                    state='modified' if wt_r['stat_wt'] else 'deleted',
                    # TODO record before and after state for diff-like use cases
                    type=wt_r['type'],
                )

        return status


# remove deprecated methods from API
for m in obsolete_methods:
    if hasattr(RevolutionGitRepo, m):
        setattr(RevolutionGitRepo, m, nothere)
