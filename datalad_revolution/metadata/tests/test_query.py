import os.path as op

from datalad.support.gitrepo import GitRepo
from datalad.support.annexrepo import AnnexRepo
from datalad.distribution.dataset import Dataset

from datalad.api import (
    install,
    query_metadata,
)
from datalad.utils import (
    chpwd,
)
from datalad.tests.utils import (
    with_tree,
    with_tempfile,
    assert_result_count,
    assert_true,
    assert_raises,
    eq_,
)


@with_tempfile(mkdir=True)
def test_ignore_nondatasets(path):
    # we want to ignore the version/commits for this test
    def _kill_time(meta):
        for m in meta:
            for k in ('version', 'shasum'):
                if k in m:
                    del m[k]
        return meta

    ds = Dataset(path).create()
    meta = _kill_time(ds.query_metadata(reporton='datasets', on_failure='ignore'))
    n_subm = 0
    # placing another repo in the dataset has no effect on metadata
    for cls, subpath in ((GitRepo, 'subm'), (AnnexRepo, 'annex_subm')):
        subm_path = op.join(ds.path, subpath)
        r = cls(subm_path, create=True)
        with open(op.join(subm_path, 'test'), 'w') as f:
            f.write('test')
        r.add('test')
        r.commit('some')
        assert_true(Dataset(subm_path).is_installed())
        eq_(meta, _kill_time(ds.query_metadata(reporton='datasets', on_failure='ignore')))
        # making it a submodule has no effect either
        ds.rev_save(subpath)
        eq_(len(ds.subdatasets()), n_subm + 1)
        eq_(meta, _kill_time(ds.query_metadata(reporton='datasets', on_failure='ignore')))
        n_subm += 1


@with_tree({'dummy': 'content'})
@with_tempfile(mkdir=True)
def test_bf2458(src, dst):
    ds = Dataset(src).create(force=True)
    ds.rev_save(to_git=False)

    # no clone (empty) into new dst
    clone = install(source=ds.path, path=dst)
    # XXX whereis says nothing in direct mode
    # content is not here
    eq_(clone.repo.whereis('dummy'), [ds.config.get('annex.uuid')])
    # check that plain metadata access does not `get` stuff
    clone.query_metadata('.', on_failure='ignore')
    # XXX whereis says nothing in direct mode
    eq_(clone.repo.whereis('dummy'), [ds.config.get('annex.uuid')])


@with_tempfile(mkdir=True)
def test_get_aggregates_fails(path):
    with chpwd(path), assert_raises(ValueError):
        query_metadata(reporton='aggregates')
    ds = Dataset(path).create()
    res = ds.query_metadata(reporton='aggregates', on_failure='ignore')
    assert_result_count(res, 1, path=ds.path, status='impossible')
