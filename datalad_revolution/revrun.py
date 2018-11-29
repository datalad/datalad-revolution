# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Thin wrapper around `run` from DataLad core"""

__docformat__ = 'restructuredtext'


import logging

# take everything from run, all we want to be is a thin variant
from datalad.interface.run import (
    Run,
    run_command,
    build_doc,
    eval_results,
)
from datalad_revolution.dataset import (
    RevolutionDataset as Dataset,
    datasetmethod,
)
from datalad_revolution.revsave import RevSave

lgr = logging.getLogger('datalad.revolution.run')


def _save_outputs(ds, to_save, msg):
    """Helper to save results after command execution is completed"""
    return RevSave.__call__(
        to_save,
        message=msg,
        # need to convert any incoming dataset into a revolutionary one
        dataset=Dataset(ds.path),
        recursive=True,
        return_type='generator')


@build_doc
class RevRun(Run):
    __doc__ = Run.__doc__

    @staticmethod
    @datasetmethod(name='rev_run')
    @eval_results
    def __call__(
            cmd=None,
            dataset=None,
            inputs=None,
            outputs=None,
            expand=None,
            explicit=False,
            message=None,
            sidecar=None):
        if cmd:
            for r in run_command(cmd, dataset=dataset,
                                 inputs=inputs, outputs=outputs,
                                 expand=expand,
                                 explicit=explicit,
                                 message=message,
                                 sidecar=sidecar,
                                 saver=_save_outputs):
                yield r
        else:
            lgr.warning("No command given")
