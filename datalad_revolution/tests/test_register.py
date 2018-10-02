from datalad.tests.utils import assert_result_count


def test_register():
    import datalad.api as da
    assert hasattr(da, 'rev_cmd')
    assert_result_count(
        da.rev_cmd(),
        1,
        action='demo')

