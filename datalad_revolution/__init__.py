"""DataLad demo extension"""

__docformat__ = 'restructuredtext'


# defines a datalad command suite
# this symbold must be indentified as a setuptools entrypoint
# to be found by datalad
command_suite = (
    # description of the command suite, displayed in cmdline help
    "DataLad revolutionary command suite",
    [
        # specification of a command, any number of commands can be defined
        (
            # importable module that contains the command implementation
            'datalad_revolution.cmd',
            # name of the command class implementation in above module
            'RevolutionCommand',
            # optional name of the command in the cmdline API
            'rev-cmd',
            # optional name of the command in the Python API
            'rev_cmd'
        ),
    ]
)
