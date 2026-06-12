import os
import sys
import unittest
import gzip
import zipfile
import tempfile
import pandas as pd
import numpy as np
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent / "backend"
sys.path.insert(0, str(backend_dir))

from utils.csv_reader import (
    get_file_size,
    open_compressed_file,
    detect_csv_delimiter,
    count_csv_data_rows,
    estimate_csv_rows_from_head,
    read_last_nonempty_line,
    summarize_csv_file,
    seek_first_timestamp_offset,
    read_selected_range_cpu,
)
from utils.csv_stream import stream_ticks

class TestCompressedFiles(unittest.TestCase):
    def setUp(self):
        # Create a temp directory for our files
        self.test_dir = tempfile.TemporaryDirectory()
        self.dir_path = Path(self.test_dir.name)

        # Generate some mock tick data (100 rows)
        self.timestamps = pd.date_range("2026-01-01 10:00:00", periods=100, freq="1s")
        self.bids = [1.2000 + i * 0.0001 for i in range(100)]
        self.asks = [1.2001 + i * 0.0001 for i in range(100)]
        
        # Create DataFrame
        self.df = pd.DataFrame({
            "Timestamp": self.timestamps.strftime("%Y-%m-%d %H:%M:%S.%f").str[:-3],
            "Bid": self.bids,
            "Ask": self.asks
        })

        # Save to raw CSV
        self.csv_path = self.dir_path / "EURUSD_2026.csv"
        self.df.to_csv(self.csv_path, index=False)

        # Save to Gz
        self.gz_path = self.dir_path / "EURUSD_2026.csv.gz"
        with open(self.csv_path, "rb") as f_in:
            with gzip.open(self.gz_path, "wb") as f_out:
                f_out.writelines(f_in)

        # Save to Zip
        self.zip_path = self.dir_path / "EURUSD_2026.csv.zip"
        with zipfile.ZipFile(self.zip_path, "w") as z:
            z.write(self.csv_path, arcname=self.csv_path.name)

    def tearDown(self):
        self.test_dir.cleanup()

    def test_file_sizes(self):
        raw_size = self.csv_path.stat().st_size
        self.assertEqual(get_file_size(self.csv_path), raw_size)
        self.assertEqual(get_file_size(self.gz_path), raw_size)
        self.assertEqual(get_file_size(self.zip_path), raw_size)

    def test_detect_csv_delimiter(self):
        self.assertEqual(detect_csv_delimiter(self.csv_path), ",")
        self.assertEqual(detect_csv_delimiter(self.gz_path), ",")
        self.assertEqual(detect_csv_delimiter(self.zip_path), ",")

    def test_count_csv_data_rows(self):
        self.assertEqual(count_csv_data_rows(self.csv_path), 100)
        self.assertEqual(count_csv_data_rows(self.gz_path), 100)
        self.assertEqual(count_csv_data_rows(self.zip_path), 100)

    def test_estimate_csv_rows_from_head(self):
        # Ensure our row estimate matches or is very close
        self.assertAlmostEqual(estimate_csv_rows_from_head(self.csv_path), 100, delta=5)
        self.assertAlmostEqual(estimate_csv_rows_from_head(self.gz_path), 100, delta=5)
        self.assertAlmostEqual(estimate_csv_rows_from_head(self.zip_path), 100, delta=5)

    def test_read_last_nonempty_line(self):
        last_line_expected = self.df.iloc[-1].tolist()
        
        last_line_csv = read_last_nonempty_line(self.csv_path)
        last_line_gz = read_last_nonempty_line(self.gz_path)
        last_line_zip = read_last_nonempty_line(self.zip_path)

        self.assertIn(str(last_line_expected[0]), last_line_csv)
        self.assertIn(str(last_line_expected[0]), last_line_gz)
        self.assertIn(str(last_line_expected[0]), last_line_zip)

    def test_summarize_csv_file(self):
        sum_csv = summarize_csv_file(self.csv_path)
        sum_gz = summarize_csv_file(self.gz_path)
        sum_zip = summarize_csv_file(self.zip_path)

        for summary in [sum_csv, sum_gz, sum_zip]:
            self.assertEqual(summary["rows"], 100)
            self.assertEqual(summary["time_col"], "Timestamp")
            self.assertEqual(summary["price_col"], "Bid")
            self.assertEqual(summary["ask_col"], "Ask")
            self.assertEqual(summary["status"], "ok")

    def test_seek_first_timestamp_offset(self):
        target_t = pd.Timestamp("2026-01-01 10:00:10", tz="UTC")
        columns = ["Timestamp", "Bid", "Ask"]
        time_index = 0
        delimiter = ","

        with open(self.csv_path, "rb") as f:
            f.readline()
            data_start = f.tell()

        offset_csv = seek_first_timestamp_offset(self.csv_path, target_t, data_start, time_index, delimiter)
        offset_gz = seek_first_timestamp_offset(self.gz_path, target_t, data_start, time_index, delimiter)
        offset_zip = seek_first_timestamp_offset(self.zip_path, target_t, data_start, time_index, delimiter)

        self.assertEqual(offset_csv, offset_gz)
        self.assertEqual(offset_csv, offset_zip)

        # Read line at offset to check if it's the expected timestamp
        with open_compressed_file(self.csv_path, "r") as f:
            f.seek(offset_csv)
            line = f.readline().strip().split(",")
            self.assertEqual(line[0], "2026-01-01 10:00:10.000")

    def test_read_selected_range_cpu(self):
        start_t = pd.Timestamp("2026-01-01 10:00:10", tz="UTC")
        end_t = pd.Timestamp("2026-01-01 10:00:20", tz="UTC")

        res_csv = read_selected_range_cpu(
            self.csv_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, max_rows=None
        )
        res_gz = read_selected_range_cpu(
            self.gz_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, max_rows=None
        )
        res_zip = read_selected_range_cpu(
            self.zip_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, max_rows=None
        )

        # prices, times, rows_scanned, rows_loaded, engine_name, bids, asks
        np.testing.assert_array_equal(res_csv[0], res_gz[0])
        np.testing.assert_array_equal(res_csv[0], res_zip[0])
        np.testing.assert_array_equal(res_csv[1], res_gz[1])
        np.testing.assert_array_equal(res_csv[1], res_zip[1])
        self.assertEqual(res_csv[3], 11)  # 10 to 20 inclusive is 11 rows
        self.assertEqual(res_gz[3], 11)
        self.assertEqual(res_zip[3], 11)

    def test_stream_ticks(self):
        start_t = pd.Timestamp("2026-01-01 10:00:10", tz="UTC")
        end_t = pd.Timestamp("2026-01-01 10:00:20", tz="UTC")

        # Stream all chunk ticks
        ticks_csv = list(stream_ticks(
            self.csv_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, chunk_rows=5
        ))
        ticks_gz = list(stream_ticks(
            self.gz_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, chunk_rows=5
        ))
        ticks_zip = list(stream_ticks(
            self.zip_path, ",", "Timestamp", "Bid", "Bid", "Ask", start_t, end_t, chunk_rows=5
        ))

        self.assertEqual(len(ticks_csv), len(ticks_gz))
        self.assertEqual(len(ticks_csv), len(ticks_zip))

        # Check total rows streamed
        total_rows_csv = sum(t[4] for t in ticks_csv)
        total_rows_gz = sum(t[4] for t in ticks_gz)
        total_rows_zip = sum(t[4] for t in ticks_zip)

        self.assertEqual(total_rows_csv, 11)
        self.assertEqual(total_rows_gz, 11)
        self.assertEqual(total_rows_zip, 11)

if __name__ == "__main__":
    unittest.main()
