import logging

lgr = logging.getLogger('datalad.revolution.create')
lgr.warn(
    "The module 'datalad_revolution.revcreate' is deprecated. "
    'The `RevCreate` class can be imported with: '
    '`from datalad.core.local.create import Create as RevCreate`')

from datalad.core.local.create import Create as RevCreate
