"""Amendment of the DataLad `Dataset` base class"""
__docformat__ = 'restructuredtext'

from . import utils as ut

from datalad.distribution.dataset import (
    Dataset as RevolutionDataset,
    EnsureDataset as EnsureRevDataset,
    datasetmethod as rev_datasetmethod,
    path_under_rev_dataset,
    require_dataset as require_rev_dataset,
    rev_get_dataset_root,
    rev_resolve_path,
)

# remove deprecated method from API
setattr(RevolutionDataset, 'get_subdatasets', ut.nothere)

# this is here to make it easier for extensions that use this already
# TODO remove when merged into datalad-core, but keep in extension code
datasetmethod = rev_datasetmethod
require_dataset = require_rev_dataset
path_under_dataset = path_under_rev_dataset
resolve_path = rev_resolve_path
get_dataset_root = rev_get_dataset_root
EnsureDataset = EnsureRevDataset


