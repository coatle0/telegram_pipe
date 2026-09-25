#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""send_telegram.py - VSURF Bill 텔레그램 '발행' 표준 (Bot API, @coatle_bot)

CLAUDE.md 준수:
- 의무2: BOT_TOKEN / TG_CHANNEL_ID 환경변수. BOT_TOKEN 미설정 시 정지 + CIO 회신.
        (하드코딩 토큰 폴백 제거 -> 시크릿 노출 차단)
- 의무3: 헤더 [REPORT #BU-NNN vN | Bill -> GM:X] 지원.
- 의무4: --selftest 로 compile/smoke 검증(getMe, 채널 노이즈 없음).
- 의무5: 발행(write)/파일첨부 전용. 읽기(read)는 telegram-mcp 사용(경로 혼용 금지).

stdlib(urllib)만 사용 -> requests 의존성/버전 문제 원천 차단.

용법(하위호환 유지):
    python send_telegram.py "메시지"                  # 위치인자(기존)
    echo "메시지" | python send_telegram.py            # stdin(기존)
    python send_telegram.py --file note.md             # 파일 '내용'을 텍스트 발행(기존)
    python send_telegram.py --attach report.xlsx --caption "요약"   # 파일 '첨부' 발행(신규)
    python send_telegram.py --attach a.png --report BU-007 --ver 1 --gm 3 --caption "요약"
    python send_telegram.py --selftest                 # 검증(외부행위 없음)
"""
import os
import sys
import json
import uuid
import argparse
import mimetypes
import urllib.request
import urllib.parse

API = "https://api.telegram.org"
DEFAULT_CHANNEL = "-1003952708285"


TOKEN_VARS = ("BOT_TOKEN", "telegram_bot_token")  # PC간 변수명 불일치 흡수


def get_bot_token():
    """의무2: 토큰 로드. 하드코딩 폴백 없음. 미설정 = 진행 정지.

    탐색 순서(PC간 표준화 - 'bot token 문제' 해결):
      1. env BOT_TOKEN
      2. env telegram_bot_token (기존 스크립트 호환)
      3. (Windows) HKCU\\Environment 레지스트리 직접 읽기
         -> setx 직후 미갱신 셸/headless 에서도 동작
    """
    for v in TOKEN_VARS:
        t = os.environ.get(v)
        if t:
            return t
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
            for v in TOKEN_VARS:
                try:
                    val, _ = winreg.QueryValueEx(k, v)
                    if val:
                        return val
                except FileNotFoundError:
                    continue
    except Exception:
        pass
    sys.stderr.write(
        "[STOP] 봇 토큰 미설정 (BOT_TOKEN/telegram_bot_token/winreg 모두 실패) "
        "- 진행 정지, CIO 회신 필요 (CLAUDE.md 의무2)\n"
    )
    sys.exit(2)


def _channel(override=None):
    return override or os.environ.get("TG_CHANNEL_ID", DEFAULT_CHANNEL)


def _header(report=None, ver=1, gm=0):
    """의무3: 보고 헤더."""
    if report:
        return "[REPORT #{} v{} | Bill -> GM:{}]\n".format(report, ver, gm)
    return ""


def send_message(text, channel=None, parse_mode="Markdown", report=None, ver=1, gm=0):
    token = get_bot_token()
    body = _header(report, ver, gm) + (text or "")
    data = {"chat_id": _channel(channel), "text": body}
    if parse_mode:
        data["parse_mode"] = parse_mode
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request("{}/bot{}/sendMessage".format(API, token), data=encoded)
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.load(resp)
    if result.get("ok"):
        print("[OK] 발행 성공: msg_id={}".format(result["result"]["message_id"]))
    else:
        print("[ERR] 발행 실패: {}".format(result))
    return result


def send_file(path, caption="", channel=None, report=None, ver=1, gm=0):
    """파일 '첨부' 발행 (sendDocument, multipart)."""
    token = get_bot_token()
    if not os.path.isfile(path):
        sys.stderr.write("[ERR] 파일 없음: {}\n".format(path))
        sys.exit(3)
    cap = _header(report, ver, gm) + (caption or "")
    boundary = uuid.uuid4().hex
    fname = os.path.basename(path)
    with open(path, "rb") as f:
        payload = f.read()

    parts = []

    def field(name, value):
        parts.append(
            ("--{}\r\nContent-Disposition: form-data; name=\"{}\"\r\n\r\n{}\r\n"
             .format(boundary, name, value)).encode("utf-8")
        )

    field("chat_id", _channel(channel))
    if cap:
        field("caption", cap)
    ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
    head = ("--{}\r\nContent-Disposition: form-data; name=\"document\"; "
            "filename=\"{}\"\r\nContent-Type: {}\r\n\r\n"
            .format(boundary, fname, ctype)).encode("utf-8")
    body = b"".join(parts) + head + payload + ("\r\n--{}--\r\n".format(boundary)).encode("utf-8")
    req = urllib.request.Request("{}/bot{}/sendDocument".format(API, token), data=body)
    req.add_header("Content-Type", "multipart/form-data; boundary={}".format(boundary))
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.load(resp)
    if result.get("ok"):
        print("[OK] 첨부 발행 성공: msg_id={} file={}".format(
            result["result"]["message_id"], fname))
    else:
        print("[ERR] 첨부 발행 실패: {}".format(result))
    return result


def selftest():
    """의무4: 외부행위 없는 smoke test (getMe)."""
    token = get_bot_token()
    chat = _channel()
    with urllib.request.urlopen("{}/bot{}/getMe".format(API, token), timeout=30) as resp:
        me = json.load(resp)
    assert me.get("ok"), "getMe 실패: {}".format(me)
    print("[OK] selftest getMe: @{} chat={}".format(me["result"]["username"], chat))
    return me


def main():
    parser = argparse.ArgumentParser(description="VSURF Bill 텔레그램 발행 (Bot API)")
    parser.add_argument("message", nargs="?", help="발행할 메시지(위치인자)")
    parser.add_argument("--file", "-f", help="파일 '내용'을 텍스트로 발행")
    parser.add_argument("--attach", "-a", help="파일 '첨부' 발행 (sendDocument)")
    parser.add_argument("--caption", default="", help="첨부 캡션")
    parser.add_argument("--channel", "-c", help="채널 ID 오버라이드")
    parser.add_argument("--report", help="의무3 리포트 번호 (예: BU-007)")
    parser.add_argument("--ver", type=int, default=1)
    parser.add_argument("--gm", default=0)
    parser.add_argument("--plain", action="store_true", help="Markdown 비활성화")
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()

    if args.selftest:
        selftest()
        return

    if args.attach:
        out = send_file(args.attach, args.caption or (args.message or ""),
                        args.channel, args.report, args.ver, args.gm)
    else:
        if args.file:
            with open(args.file, "r", encoding="utf-8") as fh:
                text = fh.read()
        elif args.message:
            text = args.message
        elif not sys.stdin.isatty():
            text = sys.stdin.read()
        else:
            parser.error("메시지를 입력하세요 (인자, --file, --attach, 또는 stdin)")
        parse_mode = "" if args.plain else "Markdown"
        out = send_message(text, args.channel, parse_mode, args.report, args.ver, args.gm)

    sys.exit(0 if out.get("ok") else 1)


if __name__ == "__main__":
    main()
