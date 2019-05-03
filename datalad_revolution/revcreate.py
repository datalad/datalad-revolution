import logging

import traceback
lgr = logging.getLogger('datalad.revolution.create')

_tb = [t[2] for t in traceback.extract_stack()]
if '_generate_extension_api' not in _tb:  # pragma: no cover
    lgr.warn(
        "The module 'datalad_revolution.revcreate' is deprecated. "
        'The `RevCreate` class can be imported with: '
        '`from datalad.core.local.create import Create as RevCreate`')

from datalad.interface.base import (
    build_doc,
)
from datalad.interface.utils import eval_results
from .dataset import (
    rev_datasetmethod,
)

from datalad.core.local.create import Create


@build_doc
class RevCreate(Create):

    @staticmethod
    @rev_datasetmethod(name='rev_create')
    @eval_results
    def __call__(path=None,
                 initopts=None,
                 force=False,
                 description=None,
                 dataset=None,
                 no_annex=False,
                 fake_dates=False,
                 cfg_proc=None):
        for r in Create.__call__(path=path,
                                 initopts=initopts,
                                 force=force,
                                 description=description,
                                 dataset=dataset,
                                 no_annex=no_annex,
                                 fake_dates=fake_dates,
                                 cfg_proc=cfg_proc,
                                 result_renderer=None,
                                 result_xfm=None,
                                 on_failure="ignore",
                                 return_type='generator'):
            yield r
