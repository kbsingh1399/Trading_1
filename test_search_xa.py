import urllib.request
import json
import asyncio
import websockets

async def inspect_dropdown_source():
    url = 'https://www.coinglass.com/footprint/BTC?bot=footprint_api'
    req = urllib.request.Request(f'http://127.0.0.1:9222/json/new?{url}', method='PUT')
    tab = json.loads(urllib.request.urlopen(req, timeout=5).read().decode())
    tab_id = tab['id']
    ws_url = tab['webSocketDebuggerUrl']
    try:
        await asyncio.sleep(4.0)
        async with websockets.connect(ws_url) as ws:
            # Enable network to capture search API
            await ws.send(json.dumps({'id': 1, 'method': 'Network.enable'}))
            await ws.recv()

            js = """(async () => {
                // Find button with 'Aggregated futures'
                const all = Array.from(document.querySelectorAll('*'));
                const btn = all.find(el => el.innerText && el.innerText.includes('Aggregated futures') && el.innerText.length < 50 && (el.tagName === 'BUTTON' || el.tagName === 'DIV' || el.role === 'button'));
                if (!btn) return { error: 'No btn' };
                btn.click();
                await new Promise(r => setTimeout(r, 600));

                // Find input with placeholder containing 'Search'
                const input = Array.from(document.querySelectorAll('input')).find(i => i.placeholder && i.placeholder.toLowerCase().includes('search'));
                if (!input) return { error: 'No input' };

                // Inspect input's React Fiber
                const key = Object.keys(input).find(k => k.startsWith('__reactFiber$'));
                let fiber = input[key];
                let fiberData = [];
                let depth = 0;
                while (fiber && depth < 15) {
                    if (fiber.memoizedProps) {
                        const pKeys = Object.keys(fiber.memoizedProps);
                        fiberData.push({ depth, pKeys });
                    }
                    fiber = fiber.return;
                    depth++;
                }

                // Type 'xa'
                const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                setter.call(input, 'xa');
                input.dispatchEvent(new Event('input', { bubbles: true }));
                input.dispatchEvent(new Event('change', { bubbles: true }));
                await new Promise(r => setTimeout(r, 1000));

                // Find all rendered rows in the dropdown
                const rows = Array.from(document.querySelectorAll('*'))
                    .filter(el => el.innerText && (el.innerText.includes('XAU') || el.innerText.includes('XAG')))
                    .map(el => ({ tag: el.tagName, text: el.innerText.replace(/\\n/g, ' ') }));

                return {
                    inputPlaceholder: input.placeholder,
                    renderedRowsCount: rows.length,
                    sampleRows: rows.slice(0, 8),
                    fiberData
                };
            })()"""
            await ws.send(json.dumps({'id': 2, 'method': 'Runtime.evaluate', 'params': {'expression': js, 'returnByValue': True, 'awaitPromise': True}}))
            while True:
                msg = json.loads(await ws.recv())
                if msg.get('id') == 2:
                    val = msg.get('result', {}).get('result', {}).get('value', {})
                    print(json.dumps(val, indent=2))
                    break
    finally:
        urllib.request.urlopen(f'http://127.0.0.1:9222/json/close/{tab_id}', timeout=3)

if __name__ == "__main__":
    asyncio.run(inspect_dropdown_source())
