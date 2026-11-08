# ==============================================================================
# launch_scraper_chrome.ps1
# Dedicated CoinGlass Pipeline Scraper Chrome Launcher (WARP SOCKS5 Proxy Mode)
# ==============================================================================
# Ensures:
# 1. Cloudflare WARP is in WarpProxy mode on port 40000.
# 2. Chrome is launched on remote debugging port 9222 with proxy socks5://127.0.0.1:40000.
# 3. Host PC primary network adapter and IP remain 100% UNTOUCHED (direct ISP).
# ==============================================================================

$ErrorActionPreference = "Continue"

$WarpCli = "C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe"
$ChromeExe = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$ProfileDir = "C:\Users\SIGMA\chrome_debug_profile"
$ProxyPort = 40000
$CdpPort = 9222

Write-Host "[+] Configuring Cloudflare WARP in SOCKS5 proxy mode on port $ProxyPort..."
if (Test-Path $WarpCli) {
    $settings = & $WarpCli settings list 2>&1
    if (-not ($settings -like "*Mode: WarpProxy on port $ProxyPort*")) {
        & $WarpCli disconnect | Out-Null
        & $WarpCli mode proxy | Out-Null
        & $WarpCli proxy port $ProxyPort | Out-Null
        & $WarpCli connect | Out-Null
        Start-Sleep -Seconds 2
    }
    Write-Host "[OK] WARP is active in WarpProxy mode on 127.0.0.1:$ProxyPort"
} else {
    Write-Host "[WARN] WARP CLI not found at $WarpCli"
}

# Check if CDP port 9222 is already responding
$cdpAlive = $false
try {
    $res = Invoke-WebRequest -Uri "http://127.0.0.1:$CdpPort/json/version" -TimeoutSec 2 -UseBasicParsing -ErrorAction SilentlyContinue
    if ($res -and $res.StatusCode -eq 200) {
        $cdpAlive = $true
    }
} catch {
    $cdpAlive = $false
}

if ($cdpAlive) {
    Write-Host "[OK] Scraper Chrome is ALREADY running and responding on port $CdpPort."
} else {
    Write-Host "[+] Launching Dedicated Scraper Chrome on port $CdpPort with proxy socks5://127.0.0.1:$ProxyPort..."
    $chromeArgs = @(
        "--remote-debugging-port=$CdpPort",
        "--proxy-server=socks5://127.0.0.1:$ProxyPort",
        "--user-data-dir=$ProfileDir",
        "--no-first-run",
        "--no-default-browser-check"
    )
    Start-Process -FilePath $ChromeExe -ArgumentList $chromeArgs
    
    $ready = $false
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep -Milliseconds 500
        try {
            $check = Invoke-WebRequest -Uri "http://127.0.0.1:$CdpPort/json/version" -TimeoutSec 1 -UseBasicParsing -ErrorAction SilentlyContinue
            if ($check -and $check.StatusCode -eq 200) {
                $ready = $true
                break
            }
        } catch {}
    }
    if ($ready) {
        Write-Host "[OK] Dedicated Scraper Chrome successfully started and responding on port $CdpPort!"
    } else {
        Write-Host "[!] Timed out waiting for CDP port $CdpPort."
    }
}

# Display IP Verification
Write-Host ""
Write-Host "=============================================================================="
Write-Host "  NETWORK SEPARATION VERIFICATION:"
Write-Host "=============================================================================="
try {
    $directIp = (curl.exe -s --max-time 4 https://api.ipify.org).Trim()
    Write-Host "  Host PC Direct IP (YouTube / Z.ai / MT5 / Personal): $directIp"
} catch {
    Write-Host "  Host PC Direct IP: unable to query"
}

try {
    $proxyIp = (curl.exe -s --max-time 4 --socks5-hostname "127.0.0.1:$ProxyPort" https://api.ipify.org).Trim()
    Write-Host "  CoinGlass Scraper Proxy IP (Chrome CDP port $CdpPort): $proxyIp"
} catch {
    Write-Host "  CoinGlass Scraper Proxy IP: unable to query"
}
Write-Host "=============================================================================="
