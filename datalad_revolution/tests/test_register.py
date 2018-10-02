from datalad.tests.utils import (
    assert_result_count,
    with_tempfile,
)
from datalad.api import create


@with_tempfile
def test_register(path):
    import datalad.api as da
    assert hasattr(da, 'rev_cmd')
    create(path)
    assert_result_count(
        da.rev_cmd(path),
        1,
        action='demo')
