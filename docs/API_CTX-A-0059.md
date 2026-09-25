# API Reference — CTX-A-0059 telegram_pipe (루트)

- 기준 commit: `89be01a` (repo HEAD, 2026-09-25 확인)
- 정본 경로: `C:\vsurf_capital\projects\telegram_pipe`
- 자산 성격: SELF_DEVELOPED

루트에는 발행(Bot API) 전용 스크립트 1개만 있다. 수집·정규화·추출·리포트 코드는 `telegram_pipeline/`(별도 CTX ID `CTX-A-0109`)에 있다.

## CLI 명령 / 진입점

| 명령 | 위치 | 하는 일 | 주요 입력 | 출력 |
|---|---|---|---|---|
| `python send_telegram.py "메시지"` | `send_telegram.py:149 main()` | Bot API로 텍스트 발행 | 위치인자 메시지, `--channel`/`--report`/`--ver`/`--gm`/`--plain` | Telegram `sendMessage` 응답(JSON), stdout에 성공/실패 로그 |
| `python send_telegram.py --file note.md` | 동일 | 파일 내용을 텍스트로 발행 | 파일 경로 | 동일 |
| `python send_telegram.py --attach report.xlsx --caption "요약"` | `send_telegram.py:98 send_file()` | 파일을 문서로 첨부 발행(`sendDocument`, multipart) | 파일 경로, caption, report/ver/gm | 동일 |
| `python send_telegram.py --selftest` | `send_telegram.py:138 selftest()` | 외부 발행 없이 `getMe`로 토큰 유효성만 확인 | 없음 | 봇 username 출력 |

## 재사용 가능한 주요 함수

| 함수 | 위치 | 하는 일 | 주요 입력 | 출력 |
|---|---|---|---|---|
| `get_bot_token()` | `send_telegram.py:38` | `BOT_TOKEN`→`telegram_bot_token`→winreg(HKCU) 순으로 토큰 탐색, 전부 실패 시 `sys.exit(2)` | 없음(환경변수/레지스트리 읽음) | 토큰 문자열 |
| `send_message(text, channel=None, parse_mode="Markdown", report=None, ver=1, gm=0)` | `send_telegram.py:81` | `sendMessage` 호출, report 헤더(`[REPORT #BU-NNN vN \| Bill -> GM:X]`) 자동 삽입 | text, 선택적 channel/report 메타 | Telegram API 응답 dict |
| `send_file(path, caption="", channel=None, report=None, ver=1, gm=0)` | `send_telegram.py:98` | `sendDocument`(multipart) 호출 | 파일 경로, caption | Telegram API 응답 dict |

## 참고

- 시크릿: 토큰은 코드에 하드코딩돼 있지 않고 환경변수/레지스트리에서만 읽는다(`TOKEN_VARS`).
- 기본 채널: `DEFAULT_CHANNEL = "-1003952708285"`(env `TG_CHANNEL_ID`로 오버라이드 가능).
- 읽기(read) 기능은 이 파일에 없다 — Bill 표준상 읽기는 `telegram-mcp`/`telegram-research`(`CTX-A-0053`) 경로 전용.
