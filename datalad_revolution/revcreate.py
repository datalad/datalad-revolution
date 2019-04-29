import logging

import traceback
lgr = logging.getLogger('datalad.revolution.create')

_tb = [t[2] for t in traceback.extract_stack()]
if '_generate_extension_api' not in _tb:  # pragma: no cover
    lgr.warn(
        "The module 'datalad_revolution.revcreate' is deprecated. "
        'The `RevCreate` class can be imported with: '
        '`from datalad.core.local.create import Create as RevCreate`')

from datalad.core.local.create import Create as RevCreate
