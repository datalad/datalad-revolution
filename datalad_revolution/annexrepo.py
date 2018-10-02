__docformat__ = 'restructuredtext'

from collections import OrderedDict
import logging
import os.path as op
from six import iteritems

from datalad.support.annexrepo import AnnexRepo

from datalad_revolution.gitrepo import (
    RevolutionGitRepo,
    obsolete_methods as gitrepo_obsolete_methods,
)
from datalad_revolution.utils import nothere

lgr = logging.getLogger('datalad_revolution.annexrepo')

obsolete_methods = (
    # next two are only needed by 'ok_clean_git'
    # 'untracked_files',
    # 'get_status',
    'is_dirty',
)


class RevolutionAnnexRepo(AnnexRepo, RevolutionGitRepo):
    def _mark_content_availability(self, info):
        objectstore = op.join(
            self.path, RevolutionGitRepo.get_git_dir(self), 'annex', 'objects')
        for f, r in iteritems(info):
            if 'key' not in r:
                # not annexed
                continue
            # test hashdirmixed first, as it is used in non-bare repos
            # which be a more frequent target
            # TODO optimize order based on some check that reveals
            # what scheme is used in a given annex
            r['has_content'] = False
            for testpath in (
                    op.join(objectstore, r['hashdirmixed'], r['key']),
                    op.join(objectstore, r['hashdirlower'], r['key'])):
                if op.exists(testpath):
                    r.pop('hashdirlower', None)
                    r.pop('hashdirmixed', None)
                    r['objloc'] = testpath
                    r['has_content'] = True
                    break

    def get_content_annexinfo(
            self, paths=None, init='git', ref=None, eval_availability=False, **kwargs):
        """
        Calling without any options given will always give the fastest
        performance.

        Parameters
        ----------
        paths : list
          Specific paths to query info for. In none are given, info is
          reported for all content.
        init : 'git' or dict-like or None
          If set to 'git' annex content info will ammend the output of
          GitRepo.get_content_info(), otherwise the dict-like object
          supplied will receive this information and the present keys will
          limit the report of annex properties. Alternatively, if `None`
          is given, no initialization is done, and no limit is in effect.
        ref : gitref or None
          If not None, annex content info for this Git reference will be
          produced, otherwise for the content of the present worktree.
        eval_availability : bool
          If this flag is given, evaluate whether the content of any annex'ed
          file is present in the local annex.
        **kwargs :
          Additional arguments for GitRepo.get_content_info(), if `init` is
          set to 'git'.

        Returns
        -------
        dict
          Each content item has an entry under its relative path within
          the repository. Each value is a dictionary with properties:

          `type`
            Can be 'file', 'symlink', 'dataset', 'directory'
          `revision`
            SHASUM is last commit affecting the item, or None, if not
            tracked.
          `key`
            Annex key of a file (if an annex'ed file)
          `bytesize`
            Size of an annexed file in bytes.
          `has_content`
            Bool whether a content object for this key exists in the local annex (with
            `eval_availability`)
          `objloc`
            Absolute path of the content object in the local annex, if one is available
            (with `eval_availability`)
        """
        if init is None:
            info = OrderedDict()
        elif init == 'git':
            info = super(AnnexRepo, self).get_content_info(
                paths=paths, **kwargs)
        else:
            info = init
        if ref:
            cmd = 'findref'
            opts = [ref]
        else:
            cmd = 'find'
            # TODO maybe inform by `path`?
            opts = ['--include', '*']
        for j in self._run_annex_command_json(cmd, opts=opts):
            path = j['file']
            if init is not None and path not in info:
                # ignore anything that Git hasn't reported on
                # TODO figure out when it is more efficient to query
                # a particular set of paths, instead of all of them
                # and just throwing away the results
                continue
            rec = info.get(path, {})
            rec.update({k: j[k] for k in j if k != 'file'})
            info[path] = rec
            # TODO make annex availability checks optional and move in here
            if not eval_availability:
                # not desired, or not annexed
                continue
            self._mark_content_availability(info)
        return info

    def annexstatus(self, paths=None, untracked='all'):
        info = self.get_content_annexinfo(
            eval_availability=False,
            init=self.get_content_annexinfo(
                paths=paths,
                ref='HEAD',
                eval_availability=False))
        self._mark_content_availability(info)
        for f, r in iteritems(self.status(paths=paths)):
            info[f].update(r)

        return info



# remove deprecated methods from API
for m in obsolete_methods + gitrepo_obsolete_methods:
    if hasattr(RevolutionAnnexRepo, m):
        setattr(RevolutionAnnexRepo, m, nothere)
