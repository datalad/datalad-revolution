# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test all extractors at a basic level"""

from six import (
    text_type,
)
from pkg_resources import iter_entry_points
from datalad.api import (
    Dataset,
    rev_extract_metadata as extract_metadata,
    rev_save as save,
    install,
)
from datalad.support.gitrepo import GitRepo

from nose import SkipTest
from datalad.tests.utils import (
    assert_repo_status,
    assert_true,
    assert_not_in,
    assert_in,
    assert_raises,
    assert_result_count,
    eq_,
    with_tree,
    with_tempfile,
)
from ...tests import (
    make_ds_hierarchy_with_metadata,
)


@with_tree(tree={'file.dat': ''})
def check_api(no_annex, path):
    ds = Dataset(path).create(force=True, no_annex=no_annex)
    ds.rev_save()
    assert_repo_status(ds.path)

    processed_extractors, skipped_extractors = [], []
    for extractor_ep in iter_entry_points('datalad.metadata.extractors'):
        # we need to be able to query for metadata, even if there is none
        # from any extractor
        try:
            res = extract_metadata(
                dataset=ds,
                sources=[extractor_ep.name],
            )
        except Exception as exc:
            exc_ = str(exc)
            skipped_extractors += [exc_]
            continue
        # datalad_core does provide some information about our precious file
        if extractor_ep.name == 'datalad_core':
            assert_result_count(
                res,
                1,
                path=ds.path,
                type='dataset',
                status='ok',
            )
            assert_true(
                all('datalad_core' in r.get('metadata', {}) for r in res))
            # every single report comes with an identifier
            assert_true(all(
                r['metadata']['datalad_core'].get(
                    '@id', None) is not None
                for r in res))
        processed_extractors.append(extractor_ep.name)
    assert "datalad_core" in processed_extractors, \
        "Should have managed to find at least the core extractor extractor"
    if skipped_extractors:
        raise SkipTest(
            "Not fully tested/succeded since some extractors failed"
            " to load:\n%s" % ("\n".join(skipped_extractors)))


def test_api_git():
    # should tollerate both pure git and annex repos
    yield check_api, True


def test_api_annex():
    yield check_api, False


@with_tempfile(mkdir=True)
def test_plainest(path):
    # blow on nothing
    assert_raises(
        ValueError,
        extract_metadata, dataset=path, sources=['datalad_core'])
    r = GitRepo(path, create=True)
    # proper error, no crash, when there is the thinnest of all traces
    # of a dataset: but nothing to describe
    assert_result_count(
        extract_metadata(
            dataset=r.path,
            on_failure='ignore',
        ),
        1,
        status='error',
        # message contains exception
        type='dataset',
        path=r.path,
    )
    # not we add some dummy content that does not count as metadata-relevant
    # and we still fail
    (r.pathobj / '.datalad').mkdir()
    (r.pathobj / '.datalad' / 'dummy').write_text(text_type('stamp'))
    ds = Dataset(r.path)
    ds.rev_save()
    assert_result_count(
        extract_metadata(
            dataset=ds.path,
            sources=['datalad_core'],
            on_failure='ignore',
        ),
        1,
        status='error',
        # message contains exception
        type='dataset',
        path=ds.path,
    )


@with_tempfile
@with_tempfile
def test_report(path, orig):
    origds, subds = make_ds_hierarchy_with_metadata(orig)
    # now clone to a new place to ensure no content is present
    ds = install(source=origds.path, path=path)
    # only dataset-global metadata
    res = extract_metadata(dataset=ds, process_type='dataset')
    assert_result_count(res, 1)
    assert_in(
        {'@type': 'Dataset', '@id': subds.id, 'name': 'sub'},
        res[0]['metadata']['datalad_core']['hasPart']
    )
    # has not seen the content
    assert_not_in(
        'contentbytesize',
        res[0]['metadata']['datalad_core']
    )
    res = extract_metadata(dataset=ds, process_type='content')
    assert(any(
        dict(tag=['one', 'two']) == r['metadata'].get('annex', None)
        for r in res
    ))
    # we have a report on file(s)
    assert(len(res) > 0)
    # but no subdataset reports
    assert_result_count(res, 0, type='dataset')
    content_size = sum(
        r['metadata']['datalad_core']['contentbytesize'] for r in res)
    # and now all together
    res = extract_metadata(dataset=ds, process_type='all')
    # got a content size report that sums up all individual sizes
    eq_(
        res[0]['metadata']['datalad_core']['contentbytesize'],
        content_size
    )
