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

from nose import SkipTest
from datalad.tests.utils import (
    assert_repo_status,
    assert_true,
    assert_result_count,
    with_tree,
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
        # datalad_core does provide some (not really) information about our
        # precious file
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
