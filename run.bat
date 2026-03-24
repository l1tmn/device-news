@echo off
chcp 65001 > nul
echo Starting Device News Scraper...
"C:\Users\kuros\anaconda3\python.exe" "D:\00_GoogleDrive\01_Brise_Audio\02_Electric\06_新製品情報\news\scraper.py"
if %errorlevel% neq 0 (
    echo ERROR: Scraper failed with exit code %errorlevel%
    exit /b %errorlevel%
)
echo Done.
