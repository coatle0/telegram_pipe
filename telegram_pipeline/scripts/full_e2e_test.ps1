# ?뚯씪: scripts/full_e2e_test.ps1
param(
  [string]$Root = "C:\autoai\trea_tchain\osat",
  [string]$StartDay = "2026-02-10",
  [string]$EndDay   = "2026-02-15"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Run([string]$cmd) {
  Write-Host ">> $cmd" -ForegroundColor Cyan
  Invoke-Expression $cmd
  if ($LASTEXITCODE -ne 0) {
    throw "FAILED: $cmd (exit=$LASTEXITCODE)"
  }
}

cd $Root
$env:PYTHONPATH = "$Root\telegram_pipeline"
$env:ALLOW_WRITE = "1"

# 1) ?뺣━ + ?뚯씠?꾨씪??
Run "python telegram_pipeline\scripts\purge_fixture_rows.py --apply"
Run "python telegram_pipeline\cli.py process"
Run "python telegram_pipeline\cli.py extract"

# 2) 湲곌컙 由ы룷???앹꽦
$start = [datetime]::ParseExact($StartDay, "yyyy-MM-dd", $null)
$end   = [datetime]::ParseExact($EndDay,   "yyyy-MM-dd", $null)
if ($end -lt $start) { throw "EndDay must be >= StartDay" }

$days = @()
for ($d = $start; $d -le $end; $d = $d.AddDays(1)) {
  $days += $d.ToString("yyyy-MM-dd")
}

foreach ($day in $days) {
  Run "python telegram_pipeline\cli.py report --day $day"
}

# 3) 由ы룷??寃利?
$blacklist = "EPS|PER|PBR|ROE|ROA|OPM|NPM|GM|GPM|EBITDA|EV|FCF|CFO|CAPEX|ADR|IPO|URL"
$hardFails = New-Object System.Collections.Generic.List[string]
$warns     = New-Object System.Collections.Generic.List[string]

foreach ($day in $days) {
  $file = Join-Path $Root "outputs\reports\report_$day.md"
  if (!(Test-Path $file)) {
    $hardFails.Add("${day}: report file missing")
    continue
  }

  $lines = Get-Content $file
  if ($lines.Count -eq 0) {
    $hardFails.Add("${day}: empty report")
    continue
  }

  # H1 寃??(肄쒕줎 湲덉?, ?섏씠???щ㎎)
  $h1 = $lines[0].Trim()
  if ($h1 -notmatch "^# Daily Report - \d{4}-\d{2}-\d{2}$") {
    $hardFails.Add("${day}: bad H1 -> '$h1'")
  }

  $content = Get-Content $file -Raw

  # 釉붾옓由ъ뒪???ㅼ썙???꾩텧 寃??
  $hits = [regex]::Matches(
    $content,
    "\b($blacklist)\b",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  if ($hits.Count -gt 0) {
    $uniq = ($hits | ForEach-Object { $_.Value.ToUpper() } | Select-Object -Unique) -join ", "
    $hardFails.Add("${day}: blacklist keywords found -> $uniq")
  }

  # tg 留곹겕 ?뺥빀??寃??
  $tgUrls = [regex]::Matches($content, "tg://privatepost\?[^\)\s]+")
  foreach ($m in $tgUrls) {
    $url = $m.Value
    if ($url -notmatch "^tg://privatepost\?channel=\d+&post=\d+$") {
      $hardFails.Add("${day}: malformed tg url -> $url")
    }
    if ($url -match "^tg://privatepost\?channel=1001&post=\d+$") {
      $warns.Add("${day}: contains channel=1001 url -> $url")
    }
  }

  # Evidence 留곹겕 ?쇰꺼 ?щ㎎(寃쎄퀬)
  $mdLinks = [regex]::Matches($content, "\[[^\]]+\]\(tg://privatepost\?channel=\d+&post=\d+\)")
  foreach ($lk in $mdLinks) {
    if ($lk.Value -notmatch "^\[[^|\]]+\s\|\s\d{2}:\d{2}\s\|\s[^\]]+\]\(tg://privatepost\?channel=\d+&post=\d+\)$") {
      $warns.Add("${day}: non-evidence link format -> $($lk.Value)")
    }
  }
}

Write-Host "`n=== SUMMARY ===" -ForegroundColor Yellow

if ($warns.Count -gt 0) {
  Write-Host "Warnings:" -ForegroundColor DarkYellow
  $warns | ForEach-Object { Write-Host " - $_" }
}

if ($hardFails.Count -gt 0) {
  Write-Host "Hard FAIL:" -ForegroundColor Red
  $hardFails | ForEach-Object { Write-Host " - $_" }
  exit 1
}

Write-Host "PASS: full E2E + report validation complete." -ForegroundColor Green
exit 0

