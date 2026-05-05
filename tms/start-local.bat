@echo off
title PAS-SCADA Kafka Bridge - LOCAL START
color 0A

echo ============================================================
echo   PAS-SCADA Kafka Bridge - LOCAL DEVELOPMENT START
echo ============================================================
echo.

REM ── Step 1: Check Java ───────────────────────────────────────
echo [1/5] Checking Java...
java -version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo  ERROR: Java not found. Install Java 17 or higher.
    pause
    exit /b 1
)
echo  Java OK

REM ── Step 2: Check Maven ──────────────────────────────────────
echo [2/5] Checking Maven...
mvn -version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo  ERROR: Maven not found. Install Maven 3.8 or higher.
    pause
    exit /b 1
)
echo  Maven OK

REM ── Step 3: Check Docker ─────────────────────────────────────
echo [3/5] Checking Docker...
docker info >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo  ERROR: Docker not running. Please start Docker Desktop first.
    pause
    exit /b 1
)
echo  Docker OK

REM ── Step 4: Start Docker services ────────────────────────────
echo [4/5] Starting Docker services (Artemis + Kafka + RabbitMQ)...
cd /d "%~dp0"
docker-compose up -d
IF %ERRORLEVEL% NEQ 0 (
    echo  ERROR: docker-compose failed. Check docker-compose.yml
    pause
    exit /b 1
)
echo  Docker services started.
echo.
echo  Waiting 15 seconds for services to be ready...
timeout /t 15 /nobreak >nul

REM ── Step 5: Start the Spring Boot app ────────────────────────
echo [5/5] Starting Kafka Bridge application (local profile)...
echo.
echo  App logs will appear below. Press CTRL+C to stop.
echo ============================================================
echo.

SET SCADA_AES_KEY=dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q=
SET ARTEMIS_PASS=testpass123
SET RABBITMQ_PASS=testpass123

mvn spring-boot:run -Dspring.profiles.active=local

echo.
echo  Application stopped.
pause
