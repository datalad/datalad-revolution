"""DataLad demo extension"""

__docformat__ = 'restructuredtext'

from .version import __version__

# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad revolutionary command suite",
    [
        (
            'datalad_revolution.revstatus',
            'RevStatus',
            'rev-status',
            'rev_status'
        ),
        (
            'datalad_revolution.revdiff',
            'RevDiff',
            'rev-diff',
            'rev_diff'
        ),
        (
            'datalad_revolution.metadata.revextract',
            'RevExtractMetadata',
            'rev-extract-metadata',
            'rev_extract_metadata'
        ),
        (
            'datalad_revolution.metadata.revaggregate',
            'RevAggregateMetadata',
            'rev-aggregate-metadata',
            'rev_aggregate_metadata'
        ),
    ]
)

from datalad import setup_package
from datalad import teardown_package
