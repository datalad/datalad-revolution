import logging

lgr = logging.getLogger('datalad.revolution.save')
lgr.warn(
    "The module 'datalad_revolution.revsave' is deprecated. "
    'The `RevSave` class can be imported with: '
    '`from datalad.core.local.save import Save as RevSave`')

from datalad.core.local.save import Save as RevSave
