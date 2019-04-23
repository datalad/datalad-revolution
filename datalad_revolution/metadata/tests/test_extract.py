# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata extraction"""

import os.path as op

from simplejson import dumps as jsondumps

from datalad.distribution.dataset import Dataset
from datalad.api import (
    rev_extract_metadata as extract_metadata,
)
from datalad.utils import chpwd

from datalad.tests.utils import (
    with_tempfile,
    with_tree,
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    eq_,
)

sample_dsmeta = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "name": "NCDC Storm Events Database",
    "description": "Storm Data is provided by the NWS",
}
sample_fmeta = {
    "something": "stupid",
    "complextype": {
        "entity": {
            "some": "many",
            "properties": "here",
        },
        "age": "young",
        "numbers": [3, 2, 1, 0],
    }
}
meta_tree = {
    '.datalad': {
        'custom_metadata.json': jsondumps(sample_dsmeta),
        'custom_metadata': {
            'sub': {
                'one.json': jsondumps(sample_fmeta),
                'nothing.json': '{}',
            },
        },
    },
    'sub': {
        'one': '1',
        'nothing': '2',
    },
}


@with_tempfile(mkdir=True)
def test_error(path):
    # go into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            ValueError,
            extract_metadata, sources=['bogus__'], path=[path])
    # fails also on unavailable metadata extractor
    ds = Dataset(path).rev_create()
    assert_raises(
        ValueError,
        extract_metadata, dataset=ds, sources=['bogus__'])


@with_tree(meta_tree)
def test_ds_extraction(path):
    ds = Dataset(path).rev_create(force=True)
    ds.config.add('datalad.metadata.exclude-path', '.metadata',
                  where='dataset')
    ds.rev_save()
    assert_repo_status(ds.path)

    # by default we get core and annex reports
    res = extract_metadata(dataset=ds)
    # dataset, plus two files (payload)
    assert_result_count(res, 3)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 2, type='file')
    # core has stuff on everythin
    assert(all('datalad_core' in r['metadata'] for r in res))

    # now for specific extractor request
    res = extract_metadata(
        sources=['custom'],
        dataset=ds,
        # artificially disable extraction from any file in the dataset
        path=[])
    assert_result_count(
        res, 1,
        type='dataset', status='ok', action='extract_metadata', path=path,
        refds=ds.path)
    assert_in('custom', res[0]['metadata'])

    # now the more useful case: getting everthing for 'custom' from a dataset
    res = extract_metadata(
        sources=['custom'],
        dataset=ds)
    assert_result_count(res, 2)
    assert_result_count(
        res, 1,
        type='dataset', status='ok', action='extract_metadata', path=path,
        refds=ds.path)
    assert_result_count(
        res, 1,
        type='file', status='ok', action='extract_metadata',
        path=op.join(path, 'sub', 'one'),
        parentds=ds.path)
    for r in res:
        assert_in('custom', r['metadata'])
    # we have a unique value report
    eq_(
        res[0]['metadata']["datalad_unique_content_properties"]['custom']["something"],
        ["stupid"]
    )
    # and lastly, if we disable extraction via config, we get nothing
    ds.config.add('datalad.metadata.extract-from-custom', 'dataset',
                  where='dataset')
    assert_result_count(extract_metadata(sources=['custom'], dataset=ds), 1)


@with_tree(meta_tree)
def test_file_extraction(path):
    # go into virgin dir to avoid detection of any dataset
    testpath = op.join(path, 'sub', 'one')
    with chpwd(path):
        res = extract_metadata(
            sources=['custom'],
            path=[testpath])
        assert_result_count(
            res, 1, type='file', status='ok', action='extract_metadata',
            path=testpath)
        assert_in('custom', res[0]['metadata'])
        eq_(res[0]['metadata']['custom'], sample_fmeta)
