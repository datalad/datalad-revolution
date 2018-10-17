# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test create action

"""

from datalad.tests.utils import known_failure_windows


import os
import os.path as op
from os.path import join as opj
from os.path import lexists

from datalad_revolution.dataset import (
    RevolutionDataset as Dataset
)
from datalad.api import rev_create as create
from datalad.utils import chpwd
from datalad.utils import _path_
from datalad.cmd import Runner

from datalad.tests.utils import with_tempfile
from datalad.tests.utils import create_tree
from datalad.tests.utils import eq_
from datalad.tests.utils import ok_
from datalad.tests.utils import assert_not_in
from datalad.tests.utils import assert_in
from datalad.tests.utils import assert_raises
from datalad.tests.utils import assert_equal
from datalad.tests.utils import assert_status
from datalad.tests.utils import assert_in_results
from datalad.tests.utils import ok_clean_git
from datalad.tests.utils import ok_exists
from datalad.tests.utils import with_tree
from datalad.tests.utils import ok_file_has_content
from datalad.tests.utils import ok_file_under_git


_dataset_hierarchy_template = {
    'origin': {
        'file1': '',
        'sub': {
            'file2': 'file2',
            'subsub': {
                'file3': 'file3'}}}}

raw = dict(return_type='list', result_filter=None, result_xfm=None, on_failure='ignore')


@with_tempfile(mkdir=True)
@with_tempfile
def test_create_raises(path, outside_path):
    ds = Dataset(path)
    # incompatible arguments (annex only):
    assert_raises(ValueError, ds.create, no_annex=True, description='some')
    assert_raises(ValueError, ds.create, no_annex=True, annex_opts=['some'])
    assert_raises(ValueError, ds.create, no_annex=True, annex_init_opts=['some'])

    with open(opj(path, "somefile.tst"), 'w') as f:
        f.write("some")
    # non-empty without `force`:
    assert_in_results(
        ds.rev_create(force=False, **raw),
        status='error',
        message='will not create a dataset in a non-empty directory, use `force` option to ignore')
    # non-empty with `force`:
    ds.rev_create(force=True)
    # create sub outside of super:
    assert_in_results(
        ds.rev_create(outside_path, **raw),
        status='error',
        message='path not associated with any dataset')

    # create a sub:
    ds.rev_create('sub')
    # fail when doing it again
    assert_in_results(
        ds.rev_create('sub', **raw),
        status='error',
        message=('collision with %s (dataset) in dataset %s',
                 str(ds.pathobj / 'sub'),
                 ds.path)
    )

    # now deinstall the sub and fail trying to create a new one at the
    # same location
    ds.uninstall('sub', check=False)
    assert_in('sub', ds.subdatasets(fulfilled=False, result_xfm='relpaths'))
    # and now should fail to also create inplace or under
    for s in 'sub', _path_('sub/subsub'):
        assert_in_results(
            ds.rev_create(s, **raw),
            status='error',
            message=('collision with %s (dataset) in dataset %s',
                     str(ds.pathobj / 'sub'),
                     ds.path)
        )


@with_tempfile
@with_tempfile
def test_create_curdir(path, path2):
    with chpwd(path, mkdir=True):
        create()
    ds = Dataset(path)
    ok_(ds.is_installed())
    ok_clean_git(ds.path, annex=True)

    with chpwd(path2, mkdir=True):
        create(no_annex=True)
    ds = Dataset(path2)
    ok_(ds.is_installed())
    ok_clean_git(ds.path, annex=False)


@with_tempfile
def test_create(path):
    ds = Dataset(path)
    ds.rev_create(description="funny", native_metadata_type=['bim', 'bam', 'bum'],
              shared_access='world')
    ok_(ds.is_installed())
    ok_clean_git(ds.path, annex=True)

    # check default backend
    eq_(ds.config.get("annex.backends"), 'MD5E')
    eq_(ds.config.get("core.sharedrepository"), '2')
    runner = Runner()
    # check description in `info`
    cmd = ['git', 'annex', 'info']
    cmlout = runner.run(cmd, cwd=path)
    assert_in('funny [here]', cmlout[0])
    # check datset ID
    eq_(ds.config.get_value('datalad.dataset', 'id'),
        ds.id)
    assert_equal(
        ds.config.get_value('datalad.metadata', 'nativetype'),
        ('bim', 'bam', 'bum'))


@with_tempfile
def test_create_sub(path):

    ds = Dataset(path)
    ds.rev_create()

    # 1. create sub and add to super:
    subds = ds.rev_create("some/what/deeper")
    ok_(isinstance(subds, Dataset))
    ok_(subds.is_installed())
    ok_clean_git(subds.path, annex=True)

    # subdataset is known to superdataset:
    assert_in(op.join("some", "what", "deeper"),
              ds.subdatasets(result_xfm='relpaths'))
    # and was committed:
    ok_clean_git(ds.path)

    # subds finds superdataset
    ok_(subds.get_superdataset() == ds)

    # 2. create sub without adding to super:
    subds2 = Dataset(opj(path, "someother")).rev_create()
    ok_(isinstance(subds2, Dataset))
    ok_(subds2.is_installed())
    ok_clean_git(subds2.path, annex=True)

    # unknown to superdataset:
    assert_not_in("someother", ds.subdatasets(result_xfm='relpaths'))

    # 3. create sub via super:
    subds3 = ds.rev_create("third", no_annex=True)
    ok_(isinstance(subds3, Dataset))
    ok_(subds3.is_installed())
    ok_clean_git(subds3.path, annex=False)
    assert_in("third", ds.subdatasets(result_xfm='relpaths'))


# windows failure triggered by
# File "C:\Miniconda35\envs\test-environment\lib\site-packages\datalad\tests\utils.py", line 421, in newfunc
#    rmtemp(d)
# PermissionError: [WinError 32] The process cannot access the file because it is being used by another process: 'C:\\Users\\appveyor\\AppData\\Local\\Temp\\1\\datalad_temp_tree_h43urkyc\\origin'
@known_failure_windows
@with_tree(tree=_dataset_hierarchy_template)
def test_create_subdataset_hierarchy_from_top(path):
    # how it would look like to overlay a subdataset hierarchy onto
    # an existing directory tree
    ds = Dataset(opj(path, 'origin')).rev_create(force=True)
    # we got a dataset ....
    ok_(ds.is_installed())
    # ... but it has untracked content
    ok_(ds.repo.dirty)
    subds = ds.rev_create('sub', force=True)
    ok_(subds.is_installed())
    ok_(subds.repo.dirty)
    subsubds = subds.rev_create('subsub', force=True)
    ok_(subsubds.is_installed())
    ok_(subsubds.repo.dirty)
    ok_(ds.id != subds.id != subsubds.id)
    ds.save(recursive=True)
    # 'file*' in each repo was untracked before and should remain as such
    # (we don't want a #1419 resurrection
    ok_(ds.repo.dirty)
    ok_(subds.repo.dirty)
    ok_(subsubds.repo.dirty)
    # if we add these three, we should get clean
    ds.add(['file1', opj(subds.path, 'file2'), opj(subsubds.path, 'file3')])
    ok_clean_git(ds.path)
    ok_(ds.id != subds.id != subsubds.id)


# CommandError: command '['git', '-c', 'receive.autogc=0', '-c', 'gc.auto=0', 'annex', 'init', '--version', '6']' failed with exitcode 1
# Failed to run ['git', '-c', 'receive.autogc=0', '-c', 'gc.auto=0', 'annex', 'init', '--version', '6'] under 'C:\\Users\\appveyor\\AppData\\Local\\Temp\\1\\datalad_temp_okvmx7gq\\lvl1\\subds'. Exit code=1.
@known_failure_windows
@with_tempfile
def test_nested_create(path):
    # to document some more organic usage pattern
    ds = Dataset(path).rev_create()
    ok_clean_git(ds.path)
    lvl2relpath = opj('lvl1', 'lvl2')
    lvl2path = opj(ds.path, lvl2relpath)
    os.makedirs(lvl2path)
    os.makedirs(opj(ds.path, 'lvl1', 'empty'))
    with open(opj(lvl2path, 'file'), 'w') as f:
        f.write('some')
    ok_(ds.add('.'))
    # later create subdataset in a fresh dir
    # WINDOWS FAILURE IS NEXT LINE
    subds1 = ds.rev_create(opj('lvl1', 'subds'))
    ok_clean_git(ds.path)
    eq_(ds.subdatasets(result_xfm='relpaths'), [opj('lvl1', 'subds')])
    # later create subdataset in an existing empty dir
    subds2 = ds.rev_create(opj('lvl1', 'empty'))
    ok_clean_git(ds.path)
    # later try to wrap existing content into a new subdataset
    # but that won't work
    assert_in_results(
        ds.rev_create(lvl2relpath, **raw),
        status='error',
        message='will not create a dataset in a non-empty directory, use `force` option to ignore')
    # even with force, as to do this properly complicated surgery would need to
    # take place
    # MIH disable shaky test till proper dedicated upfront check is in-place in `create`
    # gh-1725
    #assert_in_results(
    #    ds.rev_create(lvl2relpath, force=True,
    #              on_failure='ignore', result_xfm=None, result_filter=None),
    #    status='error', action='add')
    # only way to make it work is to unannex the content upfront
    ds.repo._run_annex_command('unannex', annex_options=[opj(lvl2relpath, 'file')])
    # nothing to save, git-annex commits the unannex itself
    assert_status(
        'ok' if ds.repo.config.getint("annex", "version") == 6 else 'notneeded',
        ds.save())
    # still nothing without force
    # "err='lvl1/lvl2' already exists in the index"
    assert_in_results(
        ds.rev_create(lvl2relpath, **raw),
        status='error',
        message='will not create a dataset in a non-empty directory, use `force` option to ignore')
    # XXX even force doesn't help, because (I assume) GitPython doesn't update
    # its representation of the Git index properly
    ds.rev_create(lvl2relpath, force=True)
    assert_in(lvl2relpath, ds.subdatasets(result_xfm='relpaths'))


# Imported from #1016
@with_tree({'ds2': {'file1.txt': 'some'}})
def test_saving_prior(topdir):
    # the problem is that we might be saving what is actually needed to be
    # "created"

    # we would like to place this structure into a hierarchy of two datasets
    # so we create first top one
    ds1 = create(topdir, force=True)
    # and everything is ok, stuff is not added BUT ds1 will be considered dirty
    ok_(ds1.repo.dirty)
    # And then we would like to initiate a sub1 subdataset
    ds2 = create('ds2', dataset=ds1, force=True)
    # But what will happen is file1.txt under ds2 would get committed first into
    # ds1, and then the whole procedure actually crashes since because ds2/file1.txt
    # is committed -- ds2 is already known to git and it just pukes with a bit
    # confusing    'ds2' already exists in the index
    assert_in('ds2', ds1.subdatasets(result_xfm='relpaths'))


@known_failure_windows  # https://github.com/datalad/datalad/issues/2606
@with_tempfile(mkdir=True)
def test_create_withprocedure(path):
    # first without
    ds = create(path)
    assert(not lexists(opj(ds.path, 'README.rst')))
    ds.remove()
    assert(not lexists(ds.path))
    # now for reals...
    ds = create(
        # needs to identify the dataset, otherwise post-proc
        # procedure doesn't know what to run on
        dataset=path,
        proc_post=[['cfg_metadatatypes', 'xmp', 'datacite']])
    ok_clean_git(path)
    ds.config.reload()
    eq_(ds.config['datalad.metadata.nativetype'], ('xmp', 'datacite'))


# Skipping on Windows due to lack of MagicMime support:
# https://github.com/datalad/datalad/pull/2770#issuecomment-415842284
@known_failure_windows
@with_tempfile(mkdir=True)
def test_create_text_no_annex(path):
    ds = create(path, text_no_annex=True)
    ok_clean_git(path)
    import re
    ok_file_has_content(
        _path_(path, '.gitattributes'),
        content='\* annex\.largefiles=\(not\(mimetype=text/\*\)\)',
        re_=True,
        match=False,
        flags=re.MULTILINE
    )
    # and check that it is really committing text files to git and binaries
    # to annex
    create_tree(path,
        {
            't': 'some text',
            'b': ''  # empty file is not considered to be a text file
                     # should we adjust the rule to consider only non empty files?
        }
    )
    ds.add(['t', 'b'])
    ok_file_under_git(path, 't', annexed=False)
    ok_file_under_git(path, 'b', annexed=True)


@with_tempfile(mkdir=True)
def test_create_fake_dates(path):
    ds = create(path, fake_dates=True)

    ok_(ds.config.getbool("datalad", "fake-dates"))
    ok_(ds.repo.fake_dates_enabled)

    # Another instance detects the fake date configuration.
    ok_(Dataset(path).repo.fake_dates_enabled)

    first_commit = ds.repo.repo.commit(
        ds.repo.repo.git.rev_list("--reverse", "--all").split()[0])

    eq_(ds.config.obtain("datalad.fake-dates-start") + 1,
        first_commit.committed_date)