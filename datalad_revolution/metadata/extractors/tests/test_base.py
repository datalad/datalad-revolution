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
from datalad.api import Dataset
from datalad.api import rev_extract_metadata as extract_metadata
from datalad.api import rev_save as save
from datalad.support.gitrepo import GitRepo

from nose import SkipTest
from datalad.tests.utils import (
    assert_repo_status,
    assert_true,
    assert_raises,
    assert_result_count,
    with_tree,
    with_tempfile,
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
                    'identifier', None) is not None
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
            sources=['datalad_core'],
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
