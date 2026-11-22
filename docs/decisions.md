# Decisions log

Single place for architecture decisions, schema notes and open questions. Append-only: add a new entry
at the bottom, do not rewrite old ones. If an old decision changes, add a new entry that supersedes it.

One entry = one decision, 3–5 lines: what we decided, why, what we rejected. This is what the brief
asks for when it requires students to justify major architectural decisions.

## Template

```
## D-NNN — <short title> (<YYYY-MM-DD>)
Decision: ...
Why: ...
Rejected: ...
```

---

## D-001 — One repository, documentation kept in three files (2026-11-13)

Decision: A single Git repository for code, config and manifests; only `README.md`, `AGENTS.md` and this
file hold written documentation, plus `docs/report.md` at the end.
Why: the team works on three machines and AI assistants kept generating stray markdown files at the
root, which made the repo unreadable and duplicated content that the final report must contain anyway.
Rejected: a full `docs/` tree with ADR-per-file, per-experiment write-ups and a weekly plan file —
too much prose to maintain for a 12-week course project.


## D-002 — Schema bronze (2026-11-15)
Decision: Đề xuất schema trong [contract v1](../src/gh_ecosystem/event_contract_v1.json): trường chung có kiểu rõ ràng, chỉ `org_id` nullable; `payload` là JSON string, timestamp UTC, giữ nguyên gzip ở raw.
Why: Trích trường chung để truy vấn, giữ payload và raw để xử lý khác biệt giữa các event type; dữ liệu sai trường bắt buộc phải báo lỗi.
Rejected: Parse payload thành struct ở bronze, ép kiểu âm thầm hoặc bỏ bản raw.

## D-003 — Grain của silver và gold (2026-11-15)
Decision: Bronze giữ bản trùng; `silver.events` dedup toàn cục theo `event_id`, các bảng theo event giữ một hàng/event_id; gold tổng hợp theo dimension/key. Tên bảng và metric nằm trong contract v1.
Why: Dedup chọn `ingested_at` sớm nhất, rồi `source_file`, rồi hash nội dung; gold ghi lại ngày bị ảnh hưởng để chạy lại không cộng trùng. Loại event lạ vẫn được giữ ở common silver; typed schema đầy đủ để S2.
Rejected: Dedup riêng từng giờ, bung array làm đổi grain hoặc append aggregate khi rerun; định nghĩa metric của B/C cần họ review.

## D-004 — Lưu trữ và partition (2026-11-15)
Decision: Dùng Iceberg/Parquet; bronze/silver partition `days(created_at)`, gold partition `event_date` UTC; mỗi bảng nằm trong bucket của zone lấy từ config.
Why: Phân vùng theo thời gian sự kiện giúp lọc ngày, xử lý dữ liệu đến muộn và hạn chế số partition ở v1.
Rejected: Partition theo repo/loại event/giờ khi chưa đo small-file và skew; v1 chưa phải kết luận hiệu năng.

## D-005 — Config, catalog và entrypoint (2026-11-15)
Decision: Hai config mẫu local/cluster dùng cùng key và cú pháp JSON hợp lệ trong YAML; catalog JDBC `gh_ecosystem`, Postgres database `iceberg_catalog`, warehouse `s3://bronze/warehouse`; credential lấy qua biến môi trường.
Why: Giữ cùng code giữa môi trường và không thêm dependency. `make ingest` và `make inspect` dùng chung hàm tải mẫu trong `src/`; config key/type nằm trong contract, runtime do C review.
Rejected: Hardcode đường dẫn/bucket trong code, nhúng credential, catalog riêng từng engine hoặc downloader riêng cho khảo sát.

## D-006 — Bằng chứng và trạng thái hợp đồng (2026-11-15)
Decision: Issue #1 dùng mẫu UTC `2024-01-01-0`; [bằng chứng](../results/issue_1_sample.json) liên kết URL, checksum, manifest và run_id. Số đo nằm trong [experiments.csv](../results/experiments.csv), header chờ C review; toàn bộ hợp đồng chờ B/C duyệt trong issue.
Why: Nghiệm thu bằng `make inspect HOUR=2024-01-01-0 CONFIG=config/local.yml`, `make contract-check CONFIG=config/local.yml`, `make test-contracts`, `make check`; config/local.yml sao chép từ mẫu local và được gitignore.
Rejected: Coi khảo sát một giờ là benchmark quy mô, ghi thời gian tải mới cho cache hit, triển khai backfill/silver/gold/cluster trong issue này hoặc tự xác nhận review đã hoàn tất.

## D-007 — Triển khai không chờ review (2026-11-15)
Decision: Theo yêu cầu của Hiếu, áp dụng hợp đồng v1 để triển khai; review không còn là điều kiện chặn bàn giao, thay cho phần chờ duyệt trong D-006.
Why: Hợp đồng hỗ trợ phối hợp; kiểm tra thực thi và bằng chứng là cơ sở xác nhận kết quả, trao đổi với owner khi có sự cố hoặc thay đổi giao diện chung.
Rejected: Dừng công việc chỉ vì chưa có comment duyệt hoặc tự nhận rằng người khác đã review.

## D-008 — Khoảng dữ liệu mục tiêu sau khảo sát nguồn (2026-11-15)
Decision: Chọn khoảng mục tiêu `2024-01-01-0` đến `2024-01-07-23` theo ngân sách raw trong config; [bằng chứng issue #3](../results/issue_3_capacity.json) liên kết manifest và run_id. Chỉ tải ngày mẫu, backfill khoảng mục tiêu là việc tiếp theo.
Why: `make measure-source FROM=2024-01-01-0 TO=2024-01-01-23 CONFIG=config/local.yml` đo đủ ngày; `make capacity-check CONFIG=config/local.yml` kiểm file và CSV. Dòng `measured.*` là số đo, `planning.*` là dự trù tuyến tính ngày/tuần/tháng, chưa đo storage curated hay xử lý phân tán.
Rejected: Coi ngày mẫu đại diện mọi ngày, coi budget là dung lượng cluster đã cấp hoặc dùng dự trù làm benchmark; xác minh khả năng lưu trữ và xử lý trước khi backfill.

## D-009 — Trạng thái manifest và chạy tiếp ingestion (2026-11-19)
Decision: #5 giữ một dòng mỗi giờ UTC với các cột `hour,source_url,local_path,sha256,compressed_bytes,downloaded_at,status,ingested_at,error`. Chỉ công bố trạng thái `ok` sau khi kiểm gzip và lưu file. `pending` đánh dấu lần tải chưa xong, `failed` giữ lý do lỗi; checksum, dung lượng và thời điểm nạp để trống ở hai trạng thái này.
Why: Giữ bằng chứng cũ của #1/#3 bằng cách đọc header cũ và giữ nguyên `downloaded_at` khi nâng cấp. `make ingest RESUME=1 CONFIG=config/local.yml` kiểm các giờ đã ghi; file `ok` khớp SHA-256 được dùng lại. Số lần thử và thời gian chờ lấy từ config, một tiến trình ghi manifest tại mỗi thời điểm.
Rejected: Thêm dòng trùng khi chạy lại, coi file tải dở là đã xong, thử lại lỗi HTTP vĩnh viễn hoặc tự tải các giờ chưa có trong manifest.

## D-010 — Mục tiêu 30 ngày thay cho 7 ngày (2026-11-19)

Decision: Chọn target `2024-01-01-0` đến `2024-01-30-23` (30 ngày, ~116.4M events, ~54.35 GB compressed) và budget planning 64 GiB; mẫu đo vẫn là 24 giờ, còn 7 ngày chỉ là một horizon planning nhỏ, không phải target. Thay D-008.
Why: Brief yêu cầu scale 10M → 50M → 100M+ và band 30–80 GB; 7 ngày chỉ có 27.16M events / 12.68 GB nên không đủ 50M/100M. 30 ngày là mốc nhỏ nhất đủ 100M và nằm trong band; 6–24 tháng vẫn là khuyến nghị dài hạn, mở rộng khi storage cho phép.
Rejected: Giữ 7 ngày vì budget 16 GiB; coi 7 ngày là target; coi mẫu 24 giờ là benchmark quy mô.

## D-011 — Merge vào main qua PR, review và CI (2026-11-19)

Decision: Bảo vệ `main` bằng branch protection: bắt buộc PR, 1 approval, check `ci`, dismiss stale approval, resolve conversation, chặn force-push và xóa branch. Owner admin được bypass để unblock; collaborator write phải qua review. Không dùng CODEOWNERS, signed commits hay linear history ở giai đoạn này. Quy trình merge này thay D-007.
Why: Ba PR gần đây merge không có review, issue vẫn open; cần một mức review tối thiểu nhưng không làm nhóm 3 người bị kẹt khi chỉ một người online.
Rejected: Để `main` unprotected; bắt buộc enforce admin tuyệt đối hoặc thêm CODEOWNERS/signed commits quá nặng cho bài tập nhóm.

## D-012 — Nền tảng local k3d và phạm vi smoke test (2026-11-20)

Decision: Dùng k3d một server + hai agent; ảnh ghim `gh-spark:3.5.5`, `trinodb/trino:484`, `apache/superset:4.1.2` và MinIO `bitnamilegacy/minio:2025.7.23-debian-12-r5`. `make up` tạo bucket raw/bronze/silver/gold, đẩy một giờ mẫu lên MinIO, chạy Spark đọc S3A, kiểm Trino `tpch.tiny.nation` và Superset `/health`, rồi ghi `results/issue_2_smoke.json`.
Why: Cần một môi trường Kubernetes tái lập được trên máy 8 GiB; Trino `tpch` và Superset SQLite đủ để chứng minh serving trước khi #4 thêm catalog Iceberg/Postgres. Ảnh chính thức của MinIO không tải được nên dùng tag legacy đã ghim.
Rejected: Docker Compose; chạy MinIO/Trino/Superset bằng binary trên host; dựng Postgres/Iceberg catalog trong #2; dùng `hostNetwork` hoặc port-forward thay cho manifest.

## D-013 — Walking skeleton lát cắt dữ liệu cục bộ cho S1 (2026-11-20)

Decision: Triển khai lát cắt walking skeleton cục bộ định dạng JSONL qua `jobs/build_bronze.py`, `jobs/build_silver.py`, `jobs/build_gold.py` và logic tại `src/gh_ecosystem/`. Bronze trích trường chung và giữ payload string (`bronze.events`); Silver dedup toàn cục theo `event_id` với tie-break canonical hash và trích typed fields cho `PushEvent` (`silver.push_events`); Gold tính số lượng sự kiện theo ngày (`gold.event_volume_daily`). Tất cả partition theo `event_date` UTC từ `created_at`. Đường dẫn và bucket lấy động từ cấu hình `storage.raw_dir` và `storage.buckets`. Silver và Gold luôn đọc toàn bộ đầu vào và ghi lại toàn bộ partition (snapshot), xóa partition cũ không còn dữ liệu; không có chế độ chạy theo giờ cho silver/gold. `make bronze SCOPE_HOUR=YYYY-MM-DD-H` chỉ giới hạn một giờ khi kiểm thử; conflict evidence ghi tại `silver/conflicts.jsonl`. Tạm thời thay thế D-004 cho lát cắt cục bộ S1; định dạng Parquet/Iceberg và serving trên k3d (Trino/Superset) là bước bàn giao tiếp theo cho C.
Why: Hoàn thành kiểm chứng luồng dữ liệu 1 giờ (153.619 bronze events, 110.370 push events, 15 dòng metric gold) độc lập với cụm phân tán và không phụ thuộc Java/Hadoop cục bộ.
Rejected: Bỏ qua deduplication; hardcode đường dẫn dữ liệu; tự động ép kiểu dữ liệu lỗi; tự nhận hoàn thành toàn bộ serving/cluster của #4 trong lát cắt này.
