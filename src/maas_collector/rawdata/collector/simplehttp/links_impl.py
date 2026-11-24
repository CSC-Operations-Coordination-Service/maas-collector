"""Simple Http "links" implementation: collect the files a web page links to"""

import datetime
import re
from urllib.parse import urljoin

from maas_collector.rawdata.collector.simplehttp.query_strategy import (
    AbstractSimpleHttpQueryStrategy,
)

# ponytail: every run reads again the files of the last week (unchanged documents are
# skipped); make it a setting if a source publishes files dated further back
LOOKBACK = datetime.timedelta(days=7)


class SimpleHttpLinksImplementation(AbstractSimpleHttpQueryStrategy):
    """Yield the content of each file product_url links to.

    link_pattern is a regular expression run on the page: each match is a link, its
    "name" group the file name and its "date" group (YYYYMMDD) the file date. Files
    dated before the last collect minus LOOKBACK, or after the end date, are skipped.
    """

    def __iter__(self):
        page = self._get(self.product_url).text

        start = (self.start_date - LOOKBACK).strftime("%Y%m%d")
        end = self.end_date.strftime("%Y%m%d")

        # a page may link a file twice: keep one link per name, oldest first
        links = {
            match["name"]: urljoin(self.product_url, match[0])
            for match in sorted(
                re.finditer(self.config.link_pattern, page), key=lambda m: m["date"]
            )
            if start <= match["date"] <= end
        }

        self.names = list(links)

        self.logger.info(
            "[%s] %d files to collect since %s", self.interface_name, len(links), start
        )

        for url in links.values():
            yield self._get(url).content

    def get_filename(self, page: int) -> str:
        return self.names[page]

    def _get(self, url):
        self.logger.debug("Requesting url %s", url)

        response = self.http_session.get(
            url,
            headers=self.authentication.get_headers(),
            timeout=self.collector.http_config.timeout,
        )
        response.raise_for_status()

        return response
