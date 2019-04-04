import logging

lgr = logging.getLogger('datalad.revolution.save')

import traceback
_tb = [t[2] for t in traceback.extract_stack()]
if '_generate_extension_api' not in _tb:
    lgr.warn(
        "The module 'datalad_revolution.revsave' is deprecated. "
        'The `RevSave` class can be imported with: '
        '`from datalad.core.local.save import Save as RevSave`')

from datalad.core.local.save import Save as RevSave
