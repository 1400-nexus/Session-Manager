"""The only coverage of `quarantine_name` that does not route through a fake
or the file-store contract harness.

`FakeFileStore` and `test_file_store_contract.py` both used to call
`quarantine_name` to form their expectations, so a regression *in the helper
itself* would have changed the expectation and the code together and passed.
Those call sites are gone; this module pins the helper with literal strings
instead. Do not fold these assertions into `test_local_file_store.py` or the
contract suite, and do not have the fake call `quarantine_name` again -- that
would delete the only independent check.
"""

from session_manager.adapters.quarantine_paths import quarantine_name
from session_manager.domain.ids import SessionId


def test_session_id_goes_before_the_extension() -> None:
    assert quarantine_name("report.bin", SessionId("a3f9c1d2")) == "report.a3f9c1d2.bin"


def test_a_name_with_no_extension() -> None:
    assert quarantine_name("CHANGELOG", SessionId("deadbeef")) == "CHANGELOG.deadbeef"


def test_only_the_last_extension_is_split() -> None:
    assert quarantine_name("archive.tar.gz", SessionId("c0ffee")) == "archive.tar.c0ffee.gz"


def test_subdirectories_are_preserved_with_forward_slashes() -> None:
    assert quarantine_name("a/b/c/output.bin", SessionId("1234abcd")) == "a/b/c/output.1234abcd.bin"


def test_a_token_hex_8_shaped_id() -> None:
    # secrets.token_hex(8): 16 lowercase hex chars, the production shape.
    assert (
        quarantine_name("sub/data.bin", SessionId("a3f9c1d2e4b50678"))
        == "sub/data.a3f9c1d2e4b50678.bin"
    )
