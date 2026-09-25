# API Reference — CTX-A-0109 telegram_pipeline (수집·정규화·추출·리포트 CLI)

- 기준 commit: `89be01a` (repo HEAD, 2026-09-25 확인)
- 정본 경로: `C:\vsurf_capital\projects\telegram_pipe\telegram_pipeline`
- 자산 성격: SELF_DEVELOPED

> 2026-09-25 이전에는 `ingest` 명령만 확인된 상태로 이 자산이 "수집 CLI"로만 취급됐고, 아래 나머지 9개 명령·5개 모듈(2,033줄)은 문서화되지 않은 채 TGXS 프로젝트에서 일부 기능(개념추출)이 중복 구현됐다. 이 문서는 그 재발을 막기 위해 `cli.py`와 `app/` 전체를 직접 읽고 작성했다.

## CLI 명령 전체 (`typer`, 진입점 `cli.py`)

| 명령 | 구현 위치 | 하는 일 | 주요 입력 | 출력 |
|---|---|---|---|---|
| `init` | `cli.py:28` → `app/db.py:66 init_db()` | `schema.sql`을 `CREATE TABLE IF NOT EXISTS`로 실행(비파괴) | 없음 | DB 스키마 초기화 |
| `ingest --config --since --until --day` | `cli.py:34` → `app/ingest.py:265 run_ingest()` | Telegram에서 신규 메시지 수집, checkpoint 기반 증분 또는 명시적 날짜범위, `raw_messages`에 저장(dedup) | config 경로, 날짜범위 또는 없음(checkpoint 모드) | `Fetched: N, Inserted: M` |
| `process --since --until --day` | `cli.py:53` → `app/process.py:30 process_messages()` | `raw_messages`를 정제(URL 마스킹, forward 헤더 제거, 공백 정규화)해 `processed_messages`에 적재 | 날짜범위(선택) | 처리 건수 print |
| `extract --since --until --day` | `cli.py:63` → `app/extract.py:169 run_extract()` | `processed_messages`에서 규칙기반(정규식+시드 JSON) 엔티티·키워드 추출, `extracted_entities`/`extracted_keywords`에 적재(멱등, 재실행 시 해당 범위 DELETE 후 재삽입) | 날짜범위(선택) | "Extraction complete." |
| `report --day --week` | `cli.py:73` → `app/report.py:531 generate_report()` | KST 일간 리포트 생성 — 상위 엔티티/키워드, 모호어, 제목·URL 추출, TG 딥링크, 한국종목 라벨 해석 | day(YYYY-MM-DD), week(미구현, v0.1은 day만) | `outputs/reports/report_<day>.md` 등 |
| `analyze --day` | `cli.py:77` → `app/analyze.py:254 analyze_report()` | 생성된 리포트를 Claude API(Anthropic)로 요약해 투자 코멘터리 작성 | day, env `ANTHROPIC_API_KEY` 필요 | `commentary_<day>.md` |
| `refine-import <json_file>` | `cli.py:85` → `app/refine.py:158 import_refined_json()` | LLM 정제 결과 JSON(배열)을 검증 후 `llm_refined_news`에 upsert | JSON 파일 경로 | `{total, inserted, updated, errors}` |
| `reprocess --since --until --day` | `cli.py:106` | `process_messages` + `run_extract` 재실행(원문 `raw_messages`는 불변) | 날짜범위 | 위 두 명령 출력 합 |
| `run-day <day> --config` | `cli.py:123` | **일간 원스톱**: `ingest`→`process`→`run_extract`→`generate_report` 순차 실행 | day(YYYY-MM-DD), config 경로 | 각 단계 로그 + 리포트 파일 |
| `synthesize --day` | `cli.py:136` → `app/analyze.py:206 synthesize_frames()` | `data/frame_scores_<day>.json`을 읽어 IC gate 결정(Howard Marks persona) 산출 | day, 사전 존재하는 frame_scores JSON | `data/ic_gate_<day>.json` |

## 재사용 가능한 주요 함수

| 함수 | 위치 | 하는 일 | 주요 입력 | 출력 |
|---|---|---|---|---|
| `get_checkpoint(conn, channel_id)` / `set_checkpoint(conn, channel_id, message_id, message_date)` | `app/db.py:42,50` | 채널별 영속 수집 커서 조회/전진(역행 금지 upsert) | sqlite 커넥션, channel_id | 마지막 message_id 또는 None |
| `check_write_permission()` | `app/db.py:8` | env `ALLOW_WRITE=1` 킬스위치 강제 | 없음 | 미설정 시 `RuntimeError` |
| `get_telegram_credentials()` | `app/config.py:12` | `TELEGRAM_API_ID/HASH`(우선) 또는 `TG_API_ID/HASH`(구버전 호환) 읽기 | 없음(env) | `(api_id:int, api_hash:str)` |
| `clean_text(text)` | `app/process.py:4` | URL→`<URL>`, zero-width 문자 제거, forward 헤더 제거, 공백 정규화 | 원문 str | 정제된 str |
| `extract_entities_from_text(text)` | `app/extract.py:26` | 정규식 기반 티커 패턴($AAPL, NASDAQ:TSLA, 6자리 한국코드) + 시드 JSON(`rules/entities_seed.json`) 별칭 매핑으로 엔티티 추출 | 정제된 텍스트 | `[{entity_name, entity_type, confidence, match_text, is_ambiguous}]` |
| `extract_keywords_from_text(text, taxonomy=None)` | `app/extract.py:115` | 시드 JSON(`rules/keywords_seed.json`) 기반 키워드 매칭 | 정제된 텍스트 | `[{keyword, category, match_text}]` — **TGXS의 `extract_concept()`(6개 하드코딩 키워드)과 기능 중복, ADAPT 재검토 대상** |
| `import_refined_json(json_file)` | `app/refine.py:158` | LLM 정제 JSON 스키마 검증(`_validate_item`) 후 upsert | JSON 파일 경로 | `{total, inserted, updated, errors}` |

## 참고

- 전체 5개 앱 모듈: `ingest.py`(297줄) / `process.py`(81줄) / `extract.py`(225줄) / `report.py`(794줄) / `analyze.py`(293줄) / `refine.py`(227줄) / `db.py`(81줄) / `config.py`(35줄).
- 영속 checkpoint(`ingest_checkpoints` 테이블)는 2026-09-25 TGXS P1에서 신규 추가됨 — `--day`/`--since`/`--until` 미지정 시에만 적용.
- `extract.py`의 규칙기반 추출과 TGXS의 신규 `extract_concept()`(엔캐리/관세/금리/HBM/데이터센터/실적 6개 키워드)는 **역할이 겹친다** — 향후 TGXS Reuse Audit 재실행 시 ADAPT 여부를 판정해야 한다(미완료, 이 문서는 사실 기록만).
