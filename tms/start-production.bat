@echo off
title PAS-SCADA Kafka Bridge - PRODUCTION START
color 0E

echo ============================================================
echo   PAS-SCADA Kafka Bridge - PRODUCTION START
echo ============================================================
echo.
echo  PRODUCTION MODE — connects to real servers:
echo    Artemis  : 10.12.1.13:61616
echo    Kafka    : 10.12.1.14:9092
echo    RabbitMQ : 10.12.1.11:5672
echo.

REM ── Check required environment variables ──────────────────────
echo [1/3] Checking required environment variables...

IF "%SCADA_AES_KEY%"=="" (
    echo  ERROR: SCADA_AES_KEY not set.
    echo  Set it with:  set SCADA_AES_KEY=^<base64-key^>
    echo  Generate key: openssl rand -base64 32
    pause
    exit /b 1
)
echo  SCADA_AES_KEY: OK

IF "%ARTEMIS_PASS%"=="" (
    echo  ERROR: ARTEMIS_PASS not set.
    echo  Set it with:  set ARTEMIS_PASS=^<password^>
    pause
    exit /b 1
)
echo  ARTEMIS_PASS: OK

IF "%RABBITMQ_PASS%"=="" (
    echo  ERROR: RABBITMQ_PASS not set.
    echo  Set it with:  set RABBITMQ_PASS=^<password^>
    pause
    exit /b 1
)
echo  RABBITMQ_PASS: OK

REM ── Build the JAR ─────────────────────────────────────────────
echo.
echo [2/3] Building application JAR...
cd /d "%~dp0"
mvn clean package -DskipTests
IF %ERRORLEVEL% NEQ 0 (
    echo  ERROR: Build failed. Fix compile errors first.
    pause
    exit /b 1
)
echo  Build OK

REM ── Start the JAR ─────────────────────────────────────────────
echo.
echo [3/3] Starting Kafka Bridge (PRODUCTION)...
echo.
echo  Logs will appear below. Press CTRL+C to stop.
echo ============================================================
echo.

java -jar target\kafka-bridge-1.0.0.jar

echo.
echo  Application stopped.
pause
