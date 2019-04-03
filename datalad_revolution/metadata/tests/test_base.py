# emacs: -*- mode: python-mode; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# -*- coding: utf-8 -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test metadata """

import os.path as op

from datalad.distribution.dataset import Dataset
from datalad.api import (
    rev_create,
)
from .. import get_metadata_type
from datalad.tests.utils import (
    with_tempfile,
    eq_,
)


@with_tempfile(mkdir=True)
def test_get_metadata_type(path):
    Dataset(path).rev_create()
    # nothing set, nothing found
    eq_(get_metadata_type(Dataset(path)), [])
    # got section, but no setting
    open(op.join(path, '.datalad', 'config'), 'w').write('[datalad "metadata"]\n')
    eq_(get_metadata_type(Dataset(path)), [])
    # minimal setting
    open(op.join(path, '.datalad', 'config'), 'w+').write('[datalad "metadata"]\nnativetype = mamboschwambo\n')
    eq_(get_metadata_type(Dataset(path)), 'mamboschwambo')


# FIXME remove when support for the old config var is removed
@with_tempfile(mkdir=True)
def test_get_metadata_type_oldcfg(path):
    Dataset(path).rev_create()
    # minimal setting
    open(op.join(path, '.datalad', 'config'), 'w+').write('[metadata]\nnativetype = mamboschwambo\n')
    eq_(get_metadata_type(Dataset(path)), 'mamboschwambo')
