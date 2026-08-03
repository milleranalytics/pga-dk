@echo off
REM ---------------------------------------------------------------------------
REM  PGA Slate Terminal - open the dashboard without running the notebook.
REM
REM  Double-click this file. It rebuilds the weekly slate if it is missing or
REM  stale, starts the local server, and opens the browser.
REM
REM  Why this exists: the notebook's serve_dashboard() runs on a daemon thread
REM  inside the kernel, so the server dies when the kernel does. That is right
REM  for the weekly run and useless for looking something up on Thursday night.
REM  This starts the same server in its own process instead.
REM
REM  Leave the window open - closing it stops the server.
REM  Opening dashboard\dist\index.html directly also works, but without the
REM  Results Browser, DB Query or lineup autosave (file:// blocks all three).
REM
REM  Kept ASCII-only with CRLF endings on purpose: cmd.exe is unreliable with
REM  UTF-8 batch files under a non-UTF-8 codepage, and with bare LF endings.
REM ---------------------------------------------------------------------------

REM Switch the console to UTF-8. Python writes UTF-8 (the progress output has
REM checkmarks and arrows in it); without this the console decodes it as cp1252
REM and every one of them renders as mojibake.
chcp 65001 >nul

REM Run from the repo root regardless of where this was launched from.
cd /d "%~dp0"

REM Prefer the py launcher, fall back to whatever python is on PATH.
set "PY=python"
where py >nul 2>nul && set "PY=py -3"

REM -u keeps output unbuffered so the URL appears immediately rather than
REM only after the server stops, which would look like nothing happened.
%PY% -u -m utils.dashboard --serve --port 8765

if errorlevel 1 (
  echo.
  echo Failed to start. Check that Python is installed and that this file is
  echo sitting in the repo root next to the utils folder.
  pause
)
