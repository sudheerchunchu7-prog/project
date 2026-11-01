# Local 100,000-row benchmark

- Rows generated: 100,000
- Input CSV size: 7.71 MiB
- Pipeline wall-clock time: 2.21 seconds
- Throughput: 45,265 rows/second
- Valid rows: 100,000
- Rejected rows: 0
- Output rows: 100,000
- Run status: SUCCESS

This is a single local run and is illustrative, not a production SLA. Results depend on the machine, Python version, and storage speed. The next optimization for much larger files is chunked CSV reading with incremental profile counters and batched SQLite inserts.
