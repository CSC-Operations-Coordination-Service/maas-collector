"""SimpleHttp "links" mode: the files a page links to, from the last collect on"""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from maas_collector.rawdata.collector.simplehttp.links_impl import (
    SimpleHttpLinksImplementation,
)

PAGE = """
<a href="/documents/d/sentinel/s1a_mp_user_20261101t100000_20261120t100000">old</a>
<a href="/documents/d/sentinel/s1a_mp_user_20261112t100000_20261201t100000">in</a>
<a href="https://site.test/documents/d/sentinel/s1c_mp_user_20261110t100000_20261130t100000">in</a>
<a href="/documents/d/sentinel/s1c_mp_user_20261110t100000_20261130t100000">same file</a>
<a href="/documents/d/sentinel/s1a_mp_user_20261130t100000_20261220t100000">future</a>
<a href="/documents/d/sentinel/other_file_20261112">not a plan</a>
"""


def test_yields_the_linked_files_from_the_last_collect():
    session = MagicMock()
    session.get.side_effect = lambda url, **_: SimpleNamespace(
        text=PAGE, content=url.encode(), raise_for_status=lambda: None
    )
    config = SimpleNamespace(
        interface_name="PLANS",
        product_url="https://site.test/plans",
        get_config_product_url=lambda: "https://site.test/plans",
        http_rest_uri="",
        link_pattern=r"/documents/d/sentinel/(?P<name>s1[a-d]_mp_user_(?P<date>\d{8})t\d{6}_\d{8}t\d{6})",
        end_date_time_offset=0,
        refresh_interval=1440,
        auth_method="",
        token_field_header="",
        token="",
    )
    journal = SimpleNamespace(
        last_date=datetime.datetime(2026, 11, 17, tzinfo=datetime.UTC),
        is_a_replay_journal=False,
    )
    end = datetime.datetime(2026, 11, 23, tzinfo=datetime.UTC)
    collector = SimpleNamespace(http_config=SimpleNamespace(timeout=10))

    query = SimpleHttpLinksImplementation(collector, config, session, None, end, journal)

    files = list(query)

    # oldest first, one per name; before last collect - 7 days or after the end: skipped
    assert files == [
        b"https://site.test/documents/d/sentinel/s1c_mp_user_20261110t100000_20261130t100000",
        b"https://site.test/documents/d/sentinel/s1a_mp_user_20261112t100000_20261201t100000",
    ]
    assert query.get_filename(0) == "s1c_mp_user_20261110t100000_20261130t100000"
