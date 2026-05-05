@echo off
title PAS-SCADA Kafka Bridge - STOP
color 0C

echo ============================================================
echo   PAS-SCADA Kafka Bridge - STOP ALL SERVICES
echo ============================================================
echo.

REM ── Stop Docker services ─────────────────────────────────────
echo [1/2] Stopping Docker services (Artemis + Kafka + RabbitMQ)...
cd /d "%~dp0"
docker-compose down
IF %ERRORLEVEL% NEQ 0 (
    echo  WARNING: docker-compose down had issues. Check manually.
) ELSE (
    echo  Docker services stopped.
)

REM ── Kill any running Java process on port 8080 ────────────────
echo [2/2] Stopping Spring Boot app if still running...
FOR /F "tokens=5" %%a IN ('netstat -aon ^| find ":8080" ^| find "LISTENING"') DO (
    echo  Killing PID %%a on port 8080...
    taskkill /PID %%a /F >nul 2>&1
)
echo  Done.

echo.
echo ============================================================
echo   All services stopped.
echo ============================================================
pause
