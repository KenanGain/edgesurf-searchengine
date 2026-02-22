@echo off
echo =============================================
echo   ULTIMATE SEARCH ENGINE STACK
echo   15 engines, 1 unified API, 100%% free
echo =============================================
echo.

echo [1/3] Starting all containers...
docker compose up -d --build

echo.
echo [2/3] Waiting for services to initialize...
timeout /t 15 /nobreak >nul

echo.
echo [3/3] Checking engine status...
curl -s http://localhost:8000/status 2>nul || echo API not ready yet, give it a few more seconds...

echo.
echo =============================================
echo   ALL SERVICES RUNNING
echo =============================================
echo.
echo   Unified API:        http://localhost:8000
echo   API Docs (Swagger): http://localhost:8000/docs
echo   Health Check:       http://localhost:8000/health
echo   Engine Status:      http://localhost:8000/status
echo.
echo   --- Web Search ---
echo   SearXNG:            http://localhost:8080
echo   Whoogle:            http://localhost:5000
echo   YaCy:               http://localhost:8090
echo.
echo   --- Full-Text Search ---
echo   MeiliSearch:        http://localhost:7700
echo   Typesense:          http://localhost:8108
echo   OpenSearch:         http://localhost:9200
echo   OS Dashboards:      http://localhost:5601
echo   Manticore:          http://localhost:9308
echo   Solr:               http://localhost:8983
echo   ZincSearch:         http://localhost:4080
echo   Quickwit:           http://localhost:7280
echo.
echo   --- Vector Search ---
echo   Qdrant:             http://localhost:6333
echo   Weaviate:           http://localhost:8085
echo   RediSearch:         http://localhost:8001
echo.
echo   --- Example Queries ---
echo   curl "http://localhost:8000/search?q=python+tutorial"
echo   curl "http://localhost:8000/search/web?q=latest+news"
echo   curl "http://localhost:8000/search/images?q=cats"
echo   curl "http://localhost:8000/search/news?q=technology"
echo   curl "http://localhost:8000/search/code?q=fastapi+example"
echo   curl "http://localhost:8000/search/science?q=quantum+computing"
echo =============================================
