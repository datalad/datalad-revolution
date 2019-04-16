from six import text_type
from datalad.api import (
    Dataset,
    rev_save as save,
)
from datalad.tests.utils import (
    create_tree,
)


def make_ds_hierarchy_with_metadata(path):
    """Test helper that returns the two datasets in the hierarchy

    The top-level dataset contains an annex'ed file with annex
    metadata.
    """
    ds = Dataset(path).rev_create(force=True)
    create_tree(ds.path, {'file.dat': 'content'})
    ds.rev_save()
    ds.repo.set_metadata('file.dat', reset={'tag': ['one', 'two']})
    subds = ds.rev_create('sub')
    # we need one real piece of content for metadata extraction
    (subds.pathobj / 'real').write_text(text_type('real'))
    ds.rev_save(recursive=True)
    return ds, subds
