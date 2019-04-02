# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata aggregation"""


import os.path as op

from datalad.api import (
    query_metadata,
    install,
    rev_create,
    rev_aggregate_metadata,
)
from ...dataset import RevolutionDataset as Dataset

from datalad.utils import (
    chpwd,
)

from datalad.tests.utils import (
    skip_ssh,
    with_tree,
    with_tempfile,
    assert_result_count,
    assert_raises,
    assert_status,
    assert_repo_status,
    assert_dict_equal,
    assert_not_in,
    eq_,
    skip_if_on_windows,
)


def _assert_metadata_empty(meta):
    ignore = set(['@id', '@context'])
    assert (not len(meta) or set(meta.keys()) == ignore), \
        'metadata record is not empty: {}'.format(
            {k: meta[k] for k in meta if k not in ignore})


_dataset_hierarchy_template = {
    'origin': {
        'dataset_description.json': """
{
    "Name": "mother_äöü東"
}""",
        'sub': {
            'dataset_description.json': """
{
    "Name": "child_äöü東"
}""",
            'subsub': {
                'dataset_description.json': """
            {
    "Name": "grandchild_äöü東"
}"""}}}}


@with_tree(tree=_dataset_hierarchy_template)
def test_basic_aggregate(path):
    # TODO give datasets some more metadata to actually aggregate stuff
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    sub = base.rev_create('sub', force=True)
    #base.query_metadata(sub.path, init=dict(homepage='this'), apply2global=True)
    subsub = base.rev_create(op.join('sub', 'subsub'), force=True)
    base.add('.', recursive=True)
    assert_repo_status(base.path)
    # we will first aggregate the middle dataset on its own, this will
    # serve as a smoke test for the reuse of metadata objects later on
    sub.rev_aggregate_metadata()
    base.save()
    assert_repo_status(base.path)
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)
    direct_meta = base.query_metadata(recursive=True, return_type='list')
    # loose the deepest dataset
    sub.uninstall('subsub', check=False)
    # no we should be able to reaggregate metadata, and loose nothing
    # because we can aggregate aggregated metadata of subsub from sub
    base.rev_aggregate_metadata(recursive=True, into='all')
    # same result for aggregate query than for (saved) direct query
    agg_meta = base.query_metadata(recursive=True, return_type='list')
    for d, a in zip(direct_meta, agg_meta):
        assert_dict_equal(d, a)
    # no we can throw away the subdataset tree, and loose no metadata
    base.uninstall('sub', recursive=True, check=False)
    assert(not sub.is_installed())
    assert_repo_status(base.path)
    # same result for aggregate query than for (saved) direct query
    agg_meta = base.query_metadata(recursive=True, return_type='list')
    for d, a in zip(direct_meta, agg_meta):
        assert_dict_equal(d, a)


# tree puts aggregate metadata structures on two levels inside a dataset
@with_tree(tree={
    '.datalad': {
        'metadata': {
            'objects': {
                'someshasum': '{"homepage": "http://top.example.com"}'},
            'aggregate_v1.json': """\
{
    "sub/deep/some": {
        "dataset_info": "objects/someshasum"
    }
}
"""}},
    'sub': {
        '.datalad': {
            'metadata': {
                'objects': {
                    'someotherhash': '{"homepage": "http://sub.example.com"}'},
                'aggregate_v1.json': """\
{
    "deep/some": {
        "dataset_info": "objects/someotherhash"
    }
}
"""}}},
})
@with_tempfile(mkdir=True)
def test_aggregate_query(path, randompath):
    ds = Dataset(path).rev_create(force=True)
    # no magic change to actual dataset metadata due to presence of
    # aggregated metadata
    res = ds.query_metadata(reporton='datasets', on_failure='ignore')
    assert_result_count(res, 0)
    # but we can now ask for metadata of stuff that is unknown on disk
    res = ds.query_metadata(op.join('sub', 'deep', 'some'), reporton='datasets')
    assert_result_count(res, 1)
    eq_({'homepage': 'http://top.example.com'}, res[0]['metadata'])
    sub = ds.rev_create('sub', force=True)
    # when no reference dataset there is NO magic discovery of the relevant
    # dataset
    with chpwd(randompath):
        assert_raises(ValueError, query_metadata,
            op.join(path, 'sub', 'deep', 'some'), reporton='datasets')
    # but inside a dataset things work
    with chpwd(ds.path):
        res = query_metadata(
            op.join(path, 'sub', 'deep', 'some'),
            reporton='datasets')
        assert_result_count(res, 1)
        # the metadata in the discovered top dataset is return, not the
        # metadata in the subdataset
        eq_({'homepage': 'http://top.example.com'}, res[0]['metadata'])
    # when a reference dataset is given, it will be used as the metadata
    # provider
    res = sub.query_metadata(op.join('deep', 'some'), reporton='datasets')
    assert_result_count(res, 1)
    eq_({'homepage': 'http://sub.example.com'}, res[0]['metadata'])


# this is for gh-1971
@with_tree(tree=_dataset_hierarchy_template)
def test_reaggregate_with_unavailable_objects(path):
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.rev_create('sub', force=True)
    subsub = base.rev_create(op.join('sub', 'subsub'), force=True)
    base.add('.', recursive=True)
    assert_repo_status(base.path)
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)
    objpath = op.join('.datalad', 'metadata', 'objects')
    objs = list(sorted(base.repo.find(objpath)))
    # we have 3x2 metadata sets (dataset/files) under annex
    eq_(len(objs), 6)
    eq_(all(base.repo.file_has_content(objs)), True)
    # drop all object content
    base.drop(objs, check=False)
    eq_(all(base.repo.file_has_content(objs)), False)
    assert_repo_status(base.path)
    # now re-aggregate, the state hasn't changed, so the file names will
    # be the same
    base.rev_aggregate_metadata(recursive=True, into='all', force='fromscratch')
    eq_(all(base.repo.file_has_content(objs)), True)
    # and there are no new objects
    eq_(
        objs,
        list(sorted(base.repo.find(objpath)))
    )


@with_tree(tree=_dataset_hierarchy_template)
@with_tempfile(mkdir=True)
def test_aggregate_with_unavailable_objects_from_subds(path, target):
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.rev_create('sub', force=True)
    subsub = base.rev_create(op.join('sub', 'subsub'), force=True)
    base.add('.', recursive=True)
    assert_repo_status(base.path)
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)

    # now make that a subdataset of a new one, so aggregation needs to get the
    # metadata objects first:
    super = Dataset(target).rev_create()
    super.install("base", source=base.path)
    assert_repo_status(super.path)
    clone = Dataset(op.join(super.path, "base"))
    assert_repo_status(clone.path)
    objpath = op.join('.datalad', 'metadata', 'objects')
    objs = [o for o in sorted(clone.repo.get_annexed_files(with_content_only=False)) if o.startswith(objpath)]
    eq_(len(objs), 6)
    eq_(all(clone.repo.file_has_content(objs)), False)

    # now aggregate should get those metadata objects
    super.rev_aggregate_metadata(recursive=True, into='all')
    eq_(all(clone.repo.file_has_content(objs)), True)


# this is for gh-1987
@skip_if_on_windows  # create_sibling incompatible with win servers
@skip_ssh
@with_tree(tree=_dataset_hierarchy_template)
def test_publish_aggregated(path):
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    base.rev_create('sub', force=True)
    base.add('.', recursive=True)
    assert_repo_status(base.path)
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)

    # create sibling and publish to it
    spath = op.join(path, 'remote')
    base.create_sibling(
        name="local_target",
        sshurl="ssh://localhost",
        target_dir=spath)
    base.publish('.', to='local_target', transfer_data='all')
    remote = Dataset(spath)
    objpath = op.join('.datalad', 'metadata', 'objects')
    objs = list(sorted(base.repo.find(objpath)))
    # all object files a present in both datasets
    eq_(all(base.repo.file_has_content(objs)), True)
    eq_(all(remote.repo.file_has_content(objs)), True)
    # and we can squeeze the same metadata out
    eq_(
        [{k: v for k, v in i.items() if k not in ('path', 'refds', 'parentds')}
         for i in base.query_metadata('sub')],
        [{k: v for k, v in i.items() if k not in ('path', 'refds', 'parentds')}
         for i in remote.query_metadata('sub')],
    )


def _get_contained_objs(ds):
    return set(f for f in ds.repo.get_indexed_files()
               if f.startswith(op.join('.datalad', 'metadata', 'objects', '')))


def _get_referenced_objs(ds):
    return set([op.relpath(r[f], start=ds.path)
                for r in ds.query_metadata(reporton='aggregates', recursive=True)
                for f in ('content_info', 'dataset_info')])


@with_tree(tree=_dataset_hierarchy_template)
def test_aggregate_removal(path):
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.rev_create('sub', force=True)
    subsub = sub.rev_create(op.join('subsub'), force=True)
    base.add('.', recursive=True)
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)
    res = base.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    assert_result_count(res, 1, path=subsub.path)
    # check that we only have object files that are listed in agginfo
    eq_(_get_contained_objs(base), _get_referenced_objs(base))
    # now delete the deepest subdataset to test cleanup of aggregated objects
    # in the top-level ds
    base.remove(op.join('sub', 'subsub'), check=False)
    # now aggregation has to detect that subsub is not simply missing, but gone
    # for good
    base.rev_aggregate_metadata(recursive=True, into='all')
    assert_repo_status(base.path)
    # internally consistent state
    eq_(_get_contained_objs(base), _get_referenced_objs(base))
    # info on subsub was removed at all levels
    res = base.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 0, path=subsub.path)
    assert_result_count(res, 2)
    res = sub.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 0, path=subsub.path)
    assert_result_count(res, 1)


@with_tree(tree=_dataset_hierarchy_template)
def test_update_strategy(path):
    base = Dataset(op.join(path, 'origin')).rev_create(force=True)
    # force all metadata objects into the annex
    with open(op.join(base.path, '.datalad', '.gitattributes'), 'w') as f:
        f.write(
            '** annex.largefiles=nothing\nmetadata/objects/** annex.largefiles=anything\n')
    sub = base.rev_create('sub', force=True)
    subsub = sub.rev_create(op.join('subsub'), force=True)
    base.add('.', recursive=True)
    assert_repo_status(base.path)
    # we start clean
    for ds in base, sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    # aggregate the base dataset only, nothing below changes
    base.rev_aggregate_metadata()
    eq_(len(_get_contained_objs(base)), 2)
    for ds in sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    # aggregate the entire tree, but by default only updates
    # the top-level dataset with all objects, none of the leaf
    # or intermediate datasets get's touched
    base.rev_aggregate_metadata(recursive=True)
    eq_(len(_get_contained_objs(base)), 6)
    eq_(len(_get_referenced_objs(base)), 6)
    for ds in sub, subsub:
        eq_(len(_get_contained_objs(ds)), 0)
    res = base.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    # it is impossible to query an intermediate or leaf dataset
    # for metadata
    for ds in sub, subsub:
        assert_status(
            'impossible',
            ds.query_metadata(reporton='aggregates', on_failure='ignore'))
    # get the full metadata report
    target_meta = base.query_metadata(return_type='list')

    # now redo full aggregation, this time updating all
    # (intermediate) datasets
    base.rev_aggregate_metadata(recursive=True, into='all')
    eq_(len(_get_contained_objs(base)), 6)
    eq_(len(_get_contained_objs(sub)), 4)
    eq_(len(_get_contained_objs(subsub)), 2)
    # it is now OK to query an intermediate or leaf dataset
    # for metadata
    for ds in sub, subsub:
        assert_status(
            'ok',
            ds.query_metadata(reporton='aggregates', on_failure='ignore'))

    # TODO end here until https://github.com/datalad/datalad-revolution/pull/84
    # has metadata() adjusted to give a uniform input
    return
    # all of that has no impact on the reported metadata
    eq_(target_meta, base.query_metadata(return_type='list'))


@with_tree({
    'this': 'that',
    'sub1': {'here': 'there'},
    'sub2': {'down': 'under'}})
def test_partial_aggregation(path):
    ds = Dataset(path).rev_create(force=True)
    sub1 = ds.rev_create('sub1', force=True)
    sub2 = ds.rev_create('sub2', force=True)
    ds.add('.', recursive=True)

    # if we aggregate a path(s) and say to recurse, we must not recurse into
    # the dataset itself and aggregate others
    ds.rev_aggregate_metadata(path='sub1', recursive=True)
    res = ds.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 1, path=ds.path)
    assert_result_count(res, 1, path=sub1.path)
    # so no metadata aggregates for sub2 yet
    assert_result_count(res, 0, path=sub2.path)

    ds.rev_aggregate_metadata(recursive=True)
    # baseline, recursive aggregation gets us something for all three datasets
    res = ds.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    # now let's do partial aggregation from just one subdataset
    # we should not loose information on the other datasets
    # as this would be a problem any time anything in a dataset
    # subtree is missing: not installed, too expensive to reaggregate, ...
    ds.rev_aggregate_metadata(path='sub1')
    res = ds.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 3)
    assert_result_count(res, 1, path=sub2.path)
    # from-scratch aggregation kills datasets that where not listed
    # note the trailing separator that indicated that path refers
    # to the content of the subdataset, not the subdataset record
    # in the superdataset
    ds.rev_aggregate_metadata(path='sub1' + op.sep, force='fromscratch')
    res = ds.query_metadata(reporton='aggregates', recursive=True)
    assert_result_count(res, 1)
    assert_result_count(res, 1, path=sub1.path)
    # now reaggregated in full
    ds.rev_aggregate_metadata(recursive=True)
    # make change in sub1
    sub1.unlock('here')
    with open(op.join(sub1.path, 'here'), 'w') as f:
        f.write('fresh')
    ds.save(recursive=True)
    assert_repo_status(path)
    # TODO for later
    # test --since with non-incremental
    #ds.aggregate_metadata(recursive=True, since='HEAD~1', incremental=False)
    #res = ds.rev_metadata(reporton='aggregates')
    #assert_result_count(res, 3)
    #assert_result_count(res, 1, path=sub2.path)
