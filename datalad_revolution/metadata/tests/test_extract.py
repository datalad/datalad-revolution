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

from shutil import copy

from datalad.distribution.dataset import Dataset
from datalad.api import (
    rev_extract_metadata as extract_metadata,
)
from datalad.utils import chpwd

from datalad.tests.utils import (
    with_tempfile,
    assert_repo_status,
    assert_raises,
    assert_result_count,
    assert_in,
    eq_,
)


testpath = op.join(op.dirname(op.dirname(op.dirname(__file__))),
                   'metadata', 'tests', 'data', 'xmp.pdf')


@with_tempfile(mkdir=True)
def test_error(path):
    # go into virgin dir to avoid detection of any dataset
    with chpwd(path):
        assert_raises(
            ValueError,
            extract_metadata, sources=['bogus__'], path=[testpath])
    # fails also on unavailable metadata extractor
    ds = Dataset(path).rev_create()
    assert_raises(
        ValueError,
        extract_metadata, dataset=ds, sources=['bogus__'])


@with_tempfile(mkdir=True)
def test_ds_extraction(path):
    from datalad.tests.utils import SkipTest
    try:
        import libxmp
    except ImportError:
        raise SkipTest

    ds = Dataset(path).rev_create()
    copy(testpath, path)
    ds.add('.')
    assert_repo_status(ds.path)

    # by default we get core and annex reports
    res = extract_metadata(dataset=ds)
    # dataset, plus two file (xmp.pdf, .gitattributes)
    assert_result_count(res, 3)
    assert_result_count(res, 1, type='dataset')
    assert_result_count(res, 2, type='file')
    # core has stuff on everythin
    assert(all('datalad_core' in r['metadata'] for r in res))
    # annex just on the annex'ed file
    assert(all('annex' in r['metadata'] or not r['path'].endswith('.pdf')
               for r in res))

    # now for specific extractor request
    res = extract_metadata(
        sources=['xmp'],
        dataset=ds,
        # artificially disable extraction from any file in the dataset
        path=[])
    assert_result_count(
        res, 1,
        type='dataset', status='ok', action='extract_metadata', path=path,
        refds=ds.path)
    assert_in('xmp', res[0]['metadata'])

    # now the more useful case: getting everthing for xmp from a dataset
    res = extract_metadata(
        sources=['xmp'],
        dataset=ds)
    assert_result_count(res, 2)
    assert_result_count(
        res, 1,
        type='dataset', status='ok', action='extract_metadata', path=path,
        refds=ds.path)
    assert_result_count(
        res, 1,
        type='file', status='ok', action='extract_metadata',
        path=op.join(path, 'xmp.pdf'),
        parentds=ds.path)
    for r in res:
        assert_in('xmp', r['metadata'])
    # we have a unique value report
    eq_(
        res[0]['metadata']["datalad_unique_content_properties"]['xmp']["dc:description"],
        ["dlsubject"]
    )
    # and lastly, if we disable extraction via config, we get nothing
    ds.config.add('datalad.metadata.extract-from-xmp', 'dataset',
                  where='dataset')
    assert_result_count(extract_metadata(sources=['xmp'], dataset=ds), 1)


@with_tempfile(mkdir=True)
def test_file_extraction(path):
    from datalad.tests.utils import SkipTest
    try:
        import libxmp
    except ImportError:
        raise SkipTest

    # go into virgin dir to avoid detection of any dataset
    with chpwd(path):
        res = extract_metadata(
            sources=['xmp'],
            path=[testpath])
        assert_result_count(
            res, 1, type='file', status='ok', action='extract_metadata',
            path=testpath)
        assert_in('xmp', res[0]['metadata'])
