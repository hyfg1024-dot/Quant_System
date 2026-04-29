import unittest
from unittest.mock import patch

import pandas as pd

import shared.authoritative_market as authoritative_market


class _FakeAk:
    @staticmethod
    def stock_fhps_detail_em(symbol: str):
        return pd.DataFrame(
            [
                {"报告期": "2025-12-31", "现金分红-现金分红比例": 10.3, "现金分红-股息率": 0.021391},
                {"报告期": "2025-06-30", "现金分红-现金分红比例": 9.8, "现金分红-股息率": 0.022410},
                {"报告期": "2024-12-31", "现金分红-现金分红比例": 22.6, "现金分红-股息率": 0.055055},
            ]
        )


class AuthoritativeMarketTests(unittest.TestCase):
    def test_china_shenhua_dividend_uses_latest_fiscal_year_ttm_cash(self):
        with patch.object(authoritative_market, "ak", _FakeAk), patch.object(
            authoritative_market, "fetch_eastmoney_price", return_value=48.15
        ):
            value = authoritative_market.fetch_a_dividend_yield_ttm("601088")

        self.assertAlmostEqual(value, 4.174455, places=6)

    def test_failed_authoritative_fetch_returns_unavailable_without_values(self):
        with patch.object(authoritative_market, "_request_eastmoney", side_effect=RuntimeError("blocked")), patch.object(
            authoritative_market, "fetch_eastmoney_a_spot_valuation", return_value={}
        ):
            result = authoritative_market.fetch_authoritative_valuation("601088", include_dividend=False)

        self.assertEqual(result["source"], "eastmoney")
        self.assertEqual(result["source_status"], "unavailable")
        self.assertIsNone(result["pe_dynamic"])
        self.assertIsNone(result["pb"])

    def test_authoritative_valuation_prefers_eastmoney_spot_dividend(self):
        push2_metrics = {
            "code": "601088",
            "current_price": 48.15,
            "pe_dynamic": 24.48,
            "pe_static": 19.76,
            "pe_rolling": 20.25,
            "pe_ttm": 20.25,
            "pb": 2.13,
            "dividend_yield": None,
        }
        spot_metrics = {"pe_dynamic": 24.50, "pb": 2.14, "dividend_yield": 4.380173}
        with patch.object(authoritative_market, "fetch_eastmoney_valuation", return_value=push2_metrics), patch.object(
            authoritative_market, "fetch_eastmoney_a_spot_valuation", return_value=spot_metrics
        ), patch.object(authoritative_market, "fetch_a_dividend_yield_ttm", return_value=4.174455):
            result = authoritative_market.fetch_authoritative_valuation("601088", use_spot_fallback=True)

        self.assertEqual(result["source_status"], "ok")
        self.assertEqual(result["dividend_yield"], 4.380173)
        self.assertEqual(result["field_sources"]["dividend_yield"], "eastmoney_spot")
        self.assertEqual(result["pe_dynamic"], 24.48)
        self.assertEqual(result["field_sources"]["pe_dynamic"], "eastmoney_push2")

    def test_authoritative_valuation_prefers_push2_dividend_field(self):
        push2_metrics = {
            "code": "601088",
            "current_price": 48.10,
            "pe_dynamic": 24.45,
            "pe_static": 19.74,
            "pe_rolling": 20.23,
            "pe_ttm": 20.23,
            "pb": 2.12,
            "dividend_yield": 4.01,
        }
        with patch.object(authoritative_market, "fetch_eastmoney_valuation", return_value=push2_metrics), patch.object(
            authoritative_market, "fetch_eastmoney_a_spot_valuation", return_value={"dividend_yield": 4.38}
        ), patch.object(authoritative_market, "fetch_a_dividend_yield_ttm", return_value=4.17):
            result = authoritative_market.fetch_authoritative_valuation("601088", use_spot_fallback=True)

        self.assertEqual(result["dividend_yield"], 4.01)
        self.assertEqual(result["field_sources"]["dividend_yield"], "eastmoney_push2")


if __name__ == "__main__":
    unittest.main()
