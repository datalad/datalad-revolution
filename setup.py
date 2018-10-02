#!/usr/bin/env python

import os.path as op
from setuptools import setup
from setuptools import find_packages


def get_version():
    """Load version of datalad from version.py without entailing any imports
    """
    # This might entail lots of imports which might not yet be available
    # so let's do ad-hoc parsing of the version.py
    with open(op.join(op.dirname(__file__),
                      'datalad_revolution',
                      'version.py')) as f:
        version_lines = list(filter(lambda x: x.startswith('__version__'), f))
    assert (len(version_lines) == 1)
    return version_lines[0].split('=')[1].strip(" '\"\t\n")


setup(
    # basic project properties can be set arbitrarily
    name="datalad_revolution",
    author="The DataLad Team and Contributors",
    author_email="team@datalad.org",
    version=get_version(),
    description="Revolutionary DataLad extension package",
    packages=[pkg for pkg in find_packages('.') if pkg.startswith('datalad')],
    # datalad command suite specs from here
    install_requires=[
        # in general datalad will be a requirement, unless the datalad extension
        # aspect is an optional component of a larger project
        # disable for now as we currently need a Git snapshot (requirements.txt)
        'datalad',
    ],
    entry_points = {
        # 'datalad.extensions' is THE entrypoint inspected by the datalad API builders
        'datalad.extensions': [
            # the label in front of '=' is the command suite label
            # the entrypoint can point to any symbol of any name, as long it is
            # valid datalad interface specification (see demo in this extensions
            'revolution=datalad_revolution:command_suite',
        ]
    },
)
