from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from openai import AsyncOpenAI

REPORT_DIR = Path("C:/DCOS/10_Pillars/20_AutoAI/telepipe")
MODEL = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# synthesize_frames — Howard Marks IC gate
# ---------------------------------------------------------------------------

SYNTH_MODEL = "gpt-4o-mini"
SYNTH_TEMPERATURE = 0.3
SYNTH_MAX_TOKENS = 800
SYNTH_BATCH_SIZE = 10
SYNTH_CONCURRENCY = 5

SYNTH_SYSTEM = (
    "You are Howard Marks, the legendary investor.\n"
    "You think in second-level thinking — always asking\n"
    "what others know and what they are missing.\n"
    "Respond ONLY in valid JSON. Korean for text fields."
)

SYNTH_USER_TEMPLATE = """다음은 하나의 뉴스 기사에 대한 3개 프레임 분석 결과다.
이를 종합해서 단일 투자 의견을 생성하라.

[프레임 분석]
{frame_scores}

[원문 요약]
{raw_text}

[출력 스키마]
{{
  "message_id": "{message_id}",
  "date": "{date}",
  "group": "{group}",

  "thesis": "투자 thesis 2~3줄. 핵심 논거 중심.",

  "conviction": 0~10,

  "market_phase": "공포|탐욕|중립|혼조",

  "second_thought": "하워드 막스 2차사고 1~2줄. 모두가 아는 것 너머의 관점. 없으면 null.",

  "action_bias": "strong_buy|buy|hold|sell|strong_sell|watch",

  "risk_flags": [
    "리스크 요인 1줄씩, 최대 3개. 없으면 []"
  ],

  "key_tickers": [
    "관련 ticker 또는 종목코드. 없으면 []"
  ],

  "frame_summary": {{
    "momentum_score": 숫자,
    "theme_score": 숫자,
    "bookie_score": 숫자 또는 null,
    "dominant_frame": "momentum|theme|bookie"
  }}
}}

판단 기준:
- conviction 8 이상: 강한 확신, 복수 프레임 일치
- conviction 5~7: 중간, 한 프레임만 강함
- conviction 4 이하: 불확실, 신호 혼재
- second_thought: 컨센서스와 반대 관점이 있을 때만
- action_bias watch: 방향은 있지만 진입 시점 불명확"""

VALID_ACTION_BIAS = {"strong_buy", "buy", "hold", "sell", "strong_sell", "watch"}
VALID_MARKET_PHASE = {"공포", "탐욕", "중립", "혼조"}


def _validate_synth(data: dict) -> bool:
    if not isinstance(data, dict):
        return False
    conv = data.get("conviction")
    if conv is None or not isinstance(conv, (int, float)) or not (0 <= conv <= 10):
        return False
    if data.get("action_bias") not in VALID_ACTION_BIAS:
        return False
    if data.get("market_phase") not in VALID_MARKET_PHASE:
        return False
    fs = data.get("frame_summary")
    if not isinstance(fs, dict):
        return False
    return True


async def _call_synth(client: AsyncOpenAI, sem: asyncio.Semaphore, item: dict) -> dict | None:
    mid = item["message_id"]
    frames_json = json.dumps(item["frames"], ensure_ascii=False, indent=2)
    raw_text = item.get("raw_text", "")

    user_msg = (
        SYNTH_USER_TEMPLATE
        .replace("{frame_scores}", frames_json)
        .replace("{raw_text}", raw_text)
        .replace("{message_id}", str(mid))
        .replace("{date}", item.get("date", "")[:10])
        .replace("{group}", item.get("group", ""))
    )

    async with sem:
        try:
            resp = await client.chat.completions.create(
                model=SYNTH_MODEL,
                temperature=SYNTH_TEMPERATURE,
                max_tokens=SYNTH_MAX_TOKENS,
                messages=[
                    {"role": "system", "content": SYNTH_SYSTEM},
                    {"role": "user", "content": user_msg},
                ],
            )
            raw = resp.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
                if raw.endswith("```"):
                    raw = raw[:-3]
                raw = raw.strip()
            data = json.loads(raw)
            if not _validate_synth(data):
                print(f"  SKIP {mid}: validation failed")
                return None
            return data
        except Exception as e:
            print(f"  SKIP {mid}: {e}")
            return None


async def _run_synth(items: list[dict]) -> list[dict]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY 환경변수를 설정해주세요.")

    client = AsyncOpenAI(api_key=api_key)
    sem = asyncio.Semaphore(SYNTH_CONCURRENCY)
    results: list[dict] = []

    for i in range(0, len(items), SYNTH_BATCH_SIZE):
        batch = items[i : i + SYNTH_BATCH_SIZE]
        batch_num = i // SYNTH_BATCH_SIZE + 1
        print(f"  Batch {batch_num}: items {i + 1}-{i + len(batch)}")
        batch_results = await asyncio.gather(
            *[_call_synth(client, sem, item) for item in batch]
        )
        results.extend([r for r in batch_results if r is not None])

    return results


def synthesize_frames(frame_scores: list[dict], day: str) -> list[dict]:
    """Synthesize 3-frame scores into IC gate decisions via Howard Marks persona."""
    if not frame_scores:
        raise SystemExit(f"No frame scores to synthesize for day={day}")

    print(f"Synthesizing {len(frame_scores)} articles (day={day})")
    print(f"Model: {SYNTH_MODEL} | temperature={SYNTH_TEMPERATURE} | concurrency={SYNTH_CONCURRENCY}")

    results = asyncio.run(_run_synth(frame_scores))
    print(f"\nDone: {len(results)}/{len(frame_scores)} succeeded")
    return results

SYSTEM_PROMPT = """당신은 전문 주식/매크로 투자 애널리스트입니다.
텔레그램 채널 모니터링 리포트(Key Companies, Top Keywords, Unknown Candidates)를 읽고
투자 관점의 간결한 코멘터리를 한국어로 작성합니다.

다음 구조로 작성하세요:

## 오늘의 핵심 테마
가장 중요한 2-3가지 투자 테마를 간결하게 서술.

## 주목 기업 / 섹터
언급 빈도와 맥락을 기반으로 주요 기업 및 섹터 코멘트.

## 매크로 환경
지정학, 유가, 금리 등 거시 변수 핵심 사항.

## 리스크 요인
단기적으로 주의해야 할 사항.

## 확장 아이디어
Unknown Candidates 또는 관련 기업 중 추가 검토 가치 있는 방향.

규칙:
- 추천 종목이 아닌 정보 요약 관점으로 서술
- 각 섹션은 3-5문장 이내로 간결하게
- 수치나 구체적 증거가 있으면 포함
"""


def analyze_report(day: str):
    try:
        from anthropic import Anthropic
    except ImportError:
        raise SystemExit("anthropic 패키지가 필요합니다: pip install anthropic")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY 환경변수를 설정해주세요.")

    report_path = REPORT_DIR / f"report_{day}.md"
    if not report_path.exists():
        raise SystemExit(f"리포트 파일 없음: {report_path}")

    report_content = report_path.read_text(encoding="utf-8-sig")

    client = Anthropic(api_key=api_key)
    message = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"다음은 {day} 마켓 인텔리전스 리포트입니다. "
                    f"투자 코멘터리를 작성해주세요:\n\n{report_content}"
                ),
            }
        ],
    )

    commentary = message.content[0].text

    output_path = REPORT_DIR / f"commentary_{day}.md"
    with open(output_path, "w", encoding="utf-8-sig") as f:
        f.write(f"# 투자 코멘터리 - {day}\n\n")
        f.write(commentary)

    print(f"Commentary generated: {output_path}")
