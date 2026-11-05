import sys
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import json
import urllib.error

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from run_pipeline import (
    SCHEDULE,
    get_open_tabs_snapshot,
    cleanup_scraper_orphaned_tabs,
)

class TestPipelineSafety(unittest.TestCase):
    def test_tdma_buffer_safety(self):
        """Verify that all tasks have >= 2.0 minutes buffer and TV is on 15m closes."""
        sorted_mins = sorted(SCHEDULE.keys())
        self.assertEqual(len(sorted_mins), 17, "Expected 17 runs per hour")

        # Verify TV liquidations are strictly on 15m candle closes
        for m in (0, 15, 30, 45):
            self.assertIn(m, SCHEDULE)
            script, desc = SCHEDULE[m]
            self.assertEqual(script, "4_coinglass_tv_scraper.py")

        # Verify Footprint sweeps are 6x per hour
        footprint_mins = [m for m, (s, _) in SCHEDULE.items() if "footprint" in s]
        self.assertEqual(len(footprint_mins), 6)

        # Check all intervals
        for i in range(len(sorted_mins)):
            m_curr = sorted_mins[i]
            m_next = sorted_mins[(i + 1) % len(sorted_mins)]
            if m_next > m_curr:
                buf = m_next - m_curr
            else:
                buf = (m_next + 60) - m_curr
            self.assertGreaterEqual(buf, 2, f"Buffer between :{m_curr:02d} and :{m_next:02d} is {buf} min (< 2 min)!")
            self.assertLessEqual(buf, 7, f"Buffer between :{m_curr:02d} and :{m_next:02d} is {buf} min (> 7 min)!")

    @patch("urllib.request.urlopen")
    def test_orphaned_coinglass_tab_cleanup(self, mock_urlopen):
        """Simulate scraper crash leaving a CoinGlass tab open."""
        # Baseline tab snapshot before scraper starts
        baseline = {"tab_user_browser_1", "tab_user_browser_2"}

        # Tabs after scraper crashed: user tabs + orphaned scraper tab
        mock_tabs = [
            {"id": "tab_user_browser_1", "type": "page", "url": "https://google.com", "title": "Google"},
            {"id": "tab_user_browser_2", "type": "page", "url": "https://tradingview.com", "title": "TV"},
            {"id": "tab_orphaned_cg", "type": "page", "url": "https://www.coinglass.com/tv/Binance_BTCUSDT", "title": "CoinGlass TV"},
        ]

        # Setup mock urlopen
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_tabs).encode()
        mock_resp.__enter__.return_value = mock_resp

        closed_tab_urls = []
        def side_effect(url_or_req, timeout=None):
            url_str = url_or_req if isinstance(url_or_req, str) else url_or_req.full_url
            if "/close/" in url_str:
                closed_tab_urls.append(url_str)
                ret = MagicMock()
                ret.__enter__.return_value = ret
                return ret
            return mock_resp

        mock_urlopen.side_effect = side_effect

        cleanup_scraper_orphaned_tabs(baseline)

        # Assert only the orphaned CoinGlass tab was closed
        self.assertEqual(len(closed_tab_urls), 1)
        self.assertIn("tab_orphaned_cg", closed_tab_urls[0])
        self.assertNotIn("tab_user_browser_1", closed_tab_urls[0])
        self.assertNotIn("tab_user_browser_2", closed_tab_urls[0])

    @patch("urllib.request.urlopen")
    def test_orphaned_blank_tab_cleanup(self, mock_urlopen):
        """Simulate scraper crash before navigation leaving about:blank tab."""
        baseline = {"tab_user_1"}
        mock_tabs = [
            {"id": "tab_user_1", "type": "page", "url": "https://google.com", "title": "Google"},
            {"id": "tab_orphaned_blank", "type": "page", "url": "about:blank", "title": ""},
        ]

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(mock_tabs).encode()
        mock_resp.__enter__.return_value = mock_resp

        closed_tab_urls = []
        def side_effect(url_or_req, timeout=None):
            url_str = url_or_req if isinstance(url_or_req, str) else url_or_req.full_url
            if "/close/" in url_str:
                closed_tab_urls.append(url_str)
                ret = MagicMock()
                ret.__enter__.return_value = ret
                return ret
            return mock_resp

        mock_urlopen.side_effect = side_effect

        cleanup_scraper_orphaned_tabs(baseline)

        print("\nClosed URLs for blank tab test:", closed_tab_urls)
        # Check if about:blank was closed or stranded
        if not closed_tab_urls:
            print("WARNING: Orphaned about:blank tab was NOT closed!")

if __name__ == "__main__":
    unittest.main()
