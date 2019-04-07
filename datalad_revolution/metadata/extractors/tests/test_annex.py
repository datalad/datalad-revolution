# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil; coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test annex metadata extractor"""

from six import text_type

from datalad.distribution.dataset import Dataset
# API commands needed
from datalad.api import (
    rev_create,
    rev_save,
    rev_extract_metadata,
)
from datalad.tests.utils import (
    with_tempfile,
    assert_result_count,
)


@with_tempfile
def test_annex_contentmeta(path):
    ds = Dataset(path).rev_create()
    mfile_path = ds.pathobj / 'sudir' / 'dummy.txt'
    mfile_path.parent.mkdir()
    mfile_path.write_text('nothing')
    (ds.pathobj / 'ignored').write_text('nometa')
    ds.rev_save()
    # TODO strip this list() wrapper when
    # https://github.com/datalad/datalad/pull/3298 is merged
    list(ds.repo.set_metadata(
        text_type(mfile_path.relative_to(ds.pathobj)),
        init={'tag': 'mytag', 'fancy': 'this?'}
    ))
    res = ds.rev_extract_metadata(sources=['annex'], process_type='content')
    # there are only results on files with annex metadata, nothing else
    #  dataset record, no records on files without annex metadata
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=text_type(mfile_path),
        type='file',
        status='ok',
        metadata={'annex': {'tag': 'mytag', 'fancy': 'this?'}},
        action='extract_metadata'
    )
