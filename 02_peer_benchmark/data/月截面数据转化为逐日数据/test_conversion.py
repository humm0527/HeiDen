"""转换日期边界及数据质量检查的回归测试（不修改生产输入）。"""

import csv
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import convert_monthly_to_daily as converter


class ConversionTests(unittest.TestCase):
    def test_first_period_and_month_end_inclusive(self):
        boundaries = [date(2025, 1, 15), date(2025, 2, 28), date(2025, 3, 31)]
        for day, expected in [(date(2025, 1, 15), 0), (date(2025, 2, 28), 0),
                              (date(2025, 3, 3), 1), (date(2025, 3, 31), 1),
                              (date(2025, 4, 1), 2)]:
            self.assertEqual(converter.source_index(day, boundaries), expected)
        with self.assertRaises(ValueError):
            converter.source_index(date(2025, 1, 14), boundaries)

    def test_weekend_and_holiday_rollback(self):
        calendar = [date(2025, 1, 27), date(2025, 2, 5), date(2025, 8, 29), date(2025, 9, 1)]
        self.assertEqual(converter.previous_or_same_day(date(2025, 2, 2), calendar), date(2025, 1, 27))
        self.assertEqual(converter.previous_or_same_day(date(2025, 8, 31), calendar), date(2025, 8, 29))
        with self.assertRaises(ValueError):
            converter.previous_or_same_day(date(2025, 9, 2), calendar)

    def test_weekend_boundary_and_missing_months(self):
        boundaries = [date(2026, 4, 30), date(2026, 5, 29), date(2026, 8, 3), date(2026, 8, 31)]
        self.assertEqual(converter.source_index(date(2026, 5, 29), boundaries), 0)
        self.assertEqual(converter.source_index(date(2026, 6, 1), boundaries), 1)
        self.assertEqual(converter.source_index(date(2026, 8, 3), boundaries), 1)
        self.assertEqual(converter.source_index(date(2026, 8, 4), boundaries), 2)
        self.assertEqual(converter.source_index(date(2026, 8, 31), boundaries), 2)

    def test_synthetic_input_blank_policies_preserve_codes(self):
        from openpyxl import Workbook
        with TemporaryDirectory() as temp:
            path = Path(temp) / "股票打分结果20250115.xlsx"
            book = Workbook(); sheet = book.active; sheet.title = "Sheet1"
            sheet.append(["证券代码", "证券名称", "集中度分类"])
            sheet.append(["000001.SZ", "示例甲", "A"])
            sheet.append(["000002.SZ", "示例乙", None])
            sheet.append(["600001.SH", "示例丙", "G"])
            book.save(path)
            kept, blanks = converter.read_snapshot(path, "keep")
            filled, filled_blanks = converter.read_snapshot(path, "g")
            dropped, dropped_blanks = converter.read_snapshot(path, "drop")
            self.assertEqual((len(kept), blanks), (3, 1))
            self.assertEqual((blanks, filled_blanks, dropped_blanks), (1, 1, 1))
            self.assertEqual(len(dropped), 2)
            self.assertEqual(filled, [(code, name, category or "G") for code, name, category in kept])
            self.assertEqual(kept[0][0], "000001.SZ")
            with self.assertRaises(ValueError):
                converter.read_snapshot(path, "error")

    def test_quarter_split_and_csv_contract(self):
        with TemporaryDirectory() as temp:
            base = Path(temp)
            inputs = base / "input"
            inputs.mkdir()
            for label in ["20250115", "20250228", "20250331"]:
                (inputs / f"股票打分结果{label}.xlsx").touch()
            calendar = base / "calendar.csv"
            calendar.write_text("market_code,calendar_date,is_trading_day\n"
                                "XSHG,2025-01-15,True\nXSHG,2025-02-28,True\n"
                                "XSHG,2025-03-03,True\nXSHG,2025-03-31,True\n"
                                "XSHG,2025-04-01,True\n", encoding="utf-8")

            def fake_snapshot(path, blank_policy, sheet_name):
                category = {"20250115": "A", "20250228": "B", "20250331": "C"}[path.stem[-8:]]
                return [("000001.SZ", "股票,名称", category)], 0

            with patch.object(converter, "read_snapshot", side_effect=fake_snapshot):
                result = converter.convert(inputs, base / "output", calendar, "keep", end=date(2025, 4, 1))
            self.assertEqual((result["trading_days"], result["rows"]), (5, 5))
            self.assertEqual([o["rows"] for o in result["outputs"]], [4, 1])
            first = base / "output/集中度分类_2025Q1.csv"
            self.assertTrue(first.read_bytes().startswith(b"\xef\xbb\xbf"))
            with first.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(reader.fieldnames, converter.OUTPUT_COLUMNS)
                rows = list(reader)
            self.assertEqual([r["券商03"] for r in rows], ["A", "A", "B", "B"])
            self.assertTrue(all(r["证券名称"] == "股票,名称" for r in rows))


if __name__ == "__main__":
    unittest.main()
