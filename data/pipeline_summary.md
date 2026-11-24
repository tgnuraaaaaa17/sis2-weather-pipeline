# Pipeline verification

| Stage | Records |
|---|---:|
| Records collected from the API | 768 |
| Deliberate invalid test records added by producer | 5 |
| Messages published to Kafka | 773 |
| Messages consumed from Kafka | 773 |
| Valid records (Pandera) | 768 |
| Invalid records (Pandera) | 5 |
| Records after cleaning | 768 |
| Records stored in SQLite3 | 768 |

## Checks

- [x] API records >= 200
- [x] Published messages >= 200
- [x] Consumed messages >= 200
- [x] published == API records + test records
- [x] consumed == published
- [x] valid + invalid == consumed
- [x] all deliberate test records detected by Pandera
- [x] at least 3 invalid test records demonstrated
- [x] cleaned == valid - test - duplicates
- [x] stored in SQLite == cleaned
- [x] idempotent loading (2nd run adds 0 rows, same checksum)
- [x] SQLite integrity_check == ok
- [x] no duplicate record_id in SQLite

## Explanation of count differences

- API records (768) + test records (5) = published messages (773).
- Consumed (773) equals published (773): no message was lost or duplicated (skipped messages of older runs: 0).
- Invalid records (5) = test records detected (5) + real invalid API records (0); they are quarantined, not loaded.
- Cleaning removed 0 leftover test records and 0 duplicates; 0 wind values were imputed (rows kept).
- Stored rows (768) equal cleaned rows (768); a second load inserted 0 rows.
