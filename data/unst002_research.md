# Unst-002: Telegram Backward 모듈 v1 — 잠정실적 발표일 감지 리서치

> 작성: 2026-04-19 | 담당: Unst·地
> 목적: 순수 리서치 (코드 수정 없음)

---

## Task 1. AWAKE 채널 접근 검증

### 결과: ✅ 접근 가능

| 항목 | 값 |
|------|-----|
| 채널명 | AWAKE - 실시간 주식 공시 정리채널 |
| username | @darthacking |
| channel_id | 1066938528 |
| 총 메시지 수 | 136,207 |
| 최초 메시지 | 2021-02-17 |
| 채널 유형 | Public (공개) |
| Telethon 세션 | `data/telethon.session` 정상 작동 |
| Python 경로 | `C:/Python314/python.exe` (telethon 1.43.0) |

### 비고
- 기존 `configs/config.yaml`에 awake 채널 **미등록** 상태
- Telethon `iter_messages`로 메시지 읽기 확인 완료
- 인코딩 주의: `PYTHONIOENCODING=utf-8` 필요 (cp949 환경에서 이모지/한글 혼합 시 UnicodeEncodeError 발생)

---

## Task 2. 기존 ingest.py 스크래핑 메커니즘 분석

### 핵심 구조

```
CLI (cli.py)
  └─ run_ingest(config_path, since, until)
       └─ _ingest_telethon(channels, session_path, since, until)
            └─ client.iter_messages(entity, offset_date=until_utc, reverse=False)
                 → newest → oldest 순회
                 → since 이전 도달 시 break
                 → ingest_message() → raw_messages 테이블 INSERT OR IGNORE
```

### `--day` 파라미터 동작

```python
# cli.py: _resolve_day_range()
KST day → UTC since/until 변환 (24시간 윈도우)
예) --day 2026-04-18 → since=2026-04-17T15:00:00Z, until=2026-04-18T15:00:00Z
```

- **단일 날짜만 지원** — 다중 일자 범위 미지원
- `--since` / `--until` 직접 지정도 가능하나 CLI에서 datetime 파싱 필요

### 3년치 백필 시 고려사항

| 방식 | 장단점 |
|------|--------|
| `--since 2023-01-01 --until 2026-04-19` | 한 번에 13만+ 메시지 순회. Telethon rate limit 주의. 가장 단순 |
| 날짜별 루프 (`--day` 반복) | 안전하지만 ~1,100일 × CLI 호출. 비효율 |
| **권장: since/until 직접 지정** | `run_ingest(config, since, until)` 직접 호출. 날짜 범위 자유 |

### Dedup 안전성
- `raw_messages` 테이블: `UNIQUE(channel_id, message_id)` + `INSERT OR IGNORE`
- 중복 실행 시 안전 (idempotent)
- `content_hash` 기반 cross-channel 중복 탐지도 존재

---

## Task 3. 잠정실적 메시지 텍스트 패턴 분석

### 패턴 A: 사업보고서/분기보고서 내 `잠정실적 : Y/N` 필드

```
[사업보고서] or [분기보고서]
종목코드: XXXXXX (종목명)
...
잠정실적 : Y    ← 탐지 대상
```

**특징:**
- 구조화된 Key-Value 형식
- `잠정실적 : Y` → 잠정실적 포함 공시
- `잠정실적 : N` → 비포함
- 보고서 제출일 = 메시지 날짜 (message_date)

**Regex 예시:**
```python
import re
# 잠정실적 Y/N 탐지
pattern_a = re.compile(r'잠정실적\s*:\s*(Y|N)', re.IGNORECASE)
# 종목코드 추출
pattern_code = re.compile(r'종목코드\s*:\s*(\d{6})')
```

### 패턴 B: 영업(잠정)실적(공정공시)

```
[영업(잠정)실적(공정공시)]
종목코드: XXXXXX (종목명)
매출액: XXX억원
영업이익: XXX억원
당기순이익: XXX억원
...
```

**특징:**
- 잠정실적 **발표 자체**를 알리는 공시
- 제목에 `영업(잠정)실적` 포함 → 해당 종목의 잠정실적 발표일
- 재무 수치 포함 (매출액, 영업이익, 당기순이익)

**Regex 예시:**
```python
pattern_b = re.compile(r'영업\(잠정\)실적')
```

### 오탐(False Positive) 리스크

| 키워드 | 오탐 사례 | 대응 |
|--------|-----------|------|
| `발표일` | 주식병합 발표일, 감자 발표일, 유상증자 발표일 등 비실적 공시에도 출현 | `발표일` 단독 사용 불가 — 반드시 `잠정실적` 또는 `영업(잠정)실적` 컨텍스트와 AND 조건 |
| `잠정실적 : N` | 보고서에 잠정실적 미포함 | `Y`만 필터 |
| `잠정` 단독 | `잠정 인가`, `잠정 조치` 등 | `잠정실적` 또는 `영업(잠정)실적` 전체 매칭 필요 |

### 패턴 신뢰도 평가

| 패턴 | Precision 예상 | Recall 예상 | 비고 |
|------|---------------|-------------|------|
| A (`잠정실적 : Y`) | 높음 (>95%) | 중간 | 보고서 내 잠정실적 포함 여부만 탐지. 발표일 ≠ 실적 자체 |
| B (`영업(잠정)실적(공정공시)`) | 매우 높음 (>98%) | 높음 | 잠정실적 공시 그 자체. 발표일 = message_date |
| A ∪ B | 높음 | 높음 | 두 패턴 합집합 권장 |

### 권장 탐지 로직 (의사코드)

```python
def is_preliminary_earnings(text: str) -> dict:
    """잠정실적 관련 메시지 판별"""
    result = {"detected": False, "type": None, "code": None}

    # Pattern B: 영업(잠정)실적 공시 (우선)
    if re.search(r'영업\(잠정\)실적', text):
        result["detected"] = True
        result["type"] = "earnings_disclosure"

    # Pattern A: 보고서 내 잠정실적 Y
    elif re.search(r'잠정실적\s*:\s*Y', text, re.IGNORECASE):
        result["detected"] = True
        result["type"] = "report_with_preliminary"

    # 종목코드 추출
    m = re.search(r'종목코드\s*:\s*(\d{6})', text)
    if m:
        result["code"] = m.group(1)

    return result
```

---

## Task 4. 기존 DB 현황 및 재사용성

### risk_commander.sqlite 현황

| 항목 | 값 |
|------|-----|
| DB 경로 | `C:\autoai\telegram_pipe\data\risk_commander.sqlite` |
| raw_messages 총 건수 | 676 |
| 기간 | 2026-04-09 ~ 2026-04-10 (2일) |
| awake 채널 데이터 | **0건** (미등록) |
| 잠정실적 관련 extracted_keywords | 4건 (`실적` 키워드, 타 채널) |

### 스키마 재사용성 평가

| 테이블 | awake 용도 적합성 | 비고 |
|--------|------------------|------|
| raw_messages | ✅ 그대로 사용 가능 | channel_id + message_id unique, immutable triggers |
| processed_messages | ✅ 사용 가능 | cleaned_text 저장 |
| extracted_entities | ✅ 사용 가능 | 종목코드 → entity_name 매핑 |
| extracted_keywords | ✅ 사용 가능 | `잠정실적`, `영업(잠정)실적` 키워드 등록 |
| entity_rules | ✅ 확장 필요 | 한국 종목코드 6자리 규칙 추가 |
| keyword_rules | ✅ 확장 필요 | 잠정실적 관련 키워드/regex 등록 |

### 권장사항

1. **별도 DB 불필요** — 기존 `risk_commander.sqlite` 스키마로 충분
2. **config.yaml에 awake 채널 추가:**
   ```yaml
   - name: "@darthacking"
     label: "AWAKE 공시"
     tags: ["disclosure", "earnings"]
     sector: "market-wide"
   ```
3. **keyword_rules 추가:**
   ```sql
   INSERT INTO keyword_rules (keyword, category, match_type) VALUES
     ('잠정실적 : Y', 'PreliminaryEarnings', 'EXACT'),
     ('영업\(잠정\)실적', 'PreliminaryEarnings', 'REGEX');
   ```
4. **백필 전략:**
   - `--since 2023-01-01 --until 2026-04-19` 로 3년치 일괄 수집
   - 136K 메시지 중 잠정실적 관련은 수천 건 예상
   - rate limit 대비: Telethon 기본 flood wait 자동 처리

---

## 종합 판단

### 구현 난이도: 낮음

| 항목 | 평가 |
|------|------|
| 채널 접근 | ✅ Public, Telethon 정상 |
| 메시지 패턴 | ✅ 고도로 구조화, regex 충분 |
| 기존 인프라 재사용 | ✅ ingest.py + 스키마 그대로 |
| 오탐 리스크 | ⚠️ `발표일` 단독 사용 금지, 패턴 A+B 합집합 사용 시 낮음 |
| 백필 규모 | ⚠️ 136K 메시지, 1회성 작업 |

### v1 구현 시 최소 작업 목록 (참고용)

1. `config.yaml`에 `@darthacking` 추가
2. `keyword_rules`에 잠정실적 패턴 2종 등록
3. 백필 실행 (`--since 2021-02-17`)
4. 잠정실적 탐지 필터 함수 작성 (위 의사코드 기반)
5. 탐지 결과 → VVP 종목코드 매핑 (6자리 코드 → code_stk 조인)

---

*End of Research Report*
