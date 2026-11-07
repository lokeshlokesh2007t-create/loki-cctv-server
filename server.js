const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors({ origin: '*' }));
app.use(bodyParser.raw({ type: ['image/jpeg', 'application/octet-stream', 'audio/*'], limit: '10mb' }));
app.use(express.json());

// Global In-Memory Buffers
let latestFrame = null;
let lastFrameTime = 0;
let streamClients = [];
let pendingCommands = [];
let commandCounter = 1;
let cameraStreamClient = null; // Real-Time Push Tunnel Socket
let systemTelemetry = {
  status: 'online',
  uptime: 0,
  rssi: -55,
  volume: 25,
  sensor: true,
  sd_ready: true,
  photos: 0,
  lastSeen: new Date().toISOString()
};

// Handle incoming frame from ESP32 (supports both /upload-frame and /api/device/frame)
function handleFrameUpload(req, res) {
  if (req.body && req.body.length > 0) {
    latestFrame = req.body;
    lastFrameTime = Date.now();

    // Broadcast frame to all active MJPEG mobile & web viewers
    for (let i = streamClients.length - 1; i >= 0; i--) {
      const client = streamClients[i];
      try {
        client.write('--boundarydonotcross\r\n');
        client.write('Content-Type: image/jpeg\r\n');
        client.write('Content-Length: ' + latestFrame.length + '\r\n\r\n');
        client.write(latestFrame, 'binary');
        client.write('\r\n');
      } catch (err) {
        streamClients.splice(i, 1);
      }
    }
    res.status(200).send('OK');
  } else {
    res.status(400).send('No Frame Data');
  }
}

app.post('/upload-frame', handleFrameUpload);
app.post('/api/device/frame', handleFrameUpload);

// 2. Live MJPEG Stream Endpoint (For Loki Mobile App & Web)
app.get('/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace;boundary=boundarydonotcross',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
    'Connection': 'close',
    'Access-Control-Allow-Origin': '*'
  });

  streamClients.push(res);
  console.log(`[Stream] Client connected. Active viewers: ${streamClients.length}`);

  // Send current frame immediately if available
  if (latestFrame) {
    res.write('--boundarydonotcross\r\n');
    res.write('Content-Type: image/jpeg\r\n');
    res.write('Content-Length: ' + latestFrame.length + '\r\n\r\n');
    res.write(latestFrame, 'binary');
    res.write('\r\n');
  }

  req.on('close', () => {
    const idx = streamClients.indexOf(res);
    if (idx !== -1) streamClients.splice(idx, 1);
    console.log(`[Stream] Client disconnected. Remaining viewers: ${streamClients.length}`);
  });
});

// 3. HD Snapshot Endpoint
app.get('/capture', (req, res) => {
  if (latestFrame) {
    res.writeHead(200, {
      'Content-Type': 'image/jpeg',
      'Content-Length': latestFrame.length,
      'Access-Control-Allow-Origin': '*'
    });
    res.end(latestFrame);
  } else {
    res.status(503).json({ error: 'Camera feed offline or waiting for first frame' });
  }
});

// 4. Telemetry Reporting & Status
function handleTelemetry(req, res) {
  if (req.body) {
    systemTelemetry = { ...systemTelemetry, ...req.body, lastSeen: new Date().toISOString() };
  }
  res.status(200).json({ status: 'ok' });
}

app.post('/api/telemetry', handleTelemetry);
app.post('/api/device/telemetry', handleTelemetry);

app.get('/api/status', (req, res) => {
  const isCameraActive = (Date.now() - lastFrameTime) < 15000;
  res.json({
    ...systemTelemetry,
    camera_live: isCameraActive,
    active_viewers: streamClients.length,
    server_time: new Date().toISOString()
  });
});

// 5. Remote Command Queue for ESP32
app.get('/api/poll-command', (req, res) => {
  if (pendingCommands.length > 0) {
    const cmd = pendingCommands.shift();
    return res.json({ has_command: true, command: cmd.action, param: cmd.param || '' });
  }
  res.json({ has_command: false });
});

// New Render Device Command Polling (Newline-delimited protocol)
app.get('/api/device/commands', (req, res) => {
  const since = parseInt(req.query.since || '0', 10);
  const isWatching = streamClients.length > 0 ? 1 : 0;
  
  let response = `W|${isWatching}\n`;
  const toSend = pendingCommands.filter(c => c.id > since);
  for (const cmd of toSend) {
    response += `${cmd.id}|${cmd.action}|${cmd.param || ''}\n`;
  }
  // Clear old executed commands
  if (pendingCommands.length > 30) {
    pendingCommands = pendingCommands.slice(-20);
  }

  res.setHeader('Content-Type', 'text/plain');
  res.status(200).send(response);
});

// Real-Time Push Tunnel (Zero-Lag Push)
app.get('/api/live-tunnel', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*'
  });
  res.write('CONNECTED:OK\n');
  cameraStreamClient = res;
  console.log('[Real-Time Push Tunnel] ESP32 Camera Connected!');

  req.on('close', () => {
    if (cameraStreamClient === res) cameraStreamClient = null;
    console.log('[Real-Time Push Tunnel] ESP32 Camera disconnected.');
  });
});

function queueCommand(action, param = '') {
  const cmdId = commandCounter++;
  const payload = `CMD:${action}:${param}\n`;
  if (cameraStreamClient) {
    try {
      cameraStreamClient.write(payload);
    } catch (e) {
      cameraStreamClient = null;
    }
  }
  pendingCommands.push({ id: cmdId, action, param, timestamp: Date.now() });
  if (pendingCommands.length > 30) pendingCommands.shift();
}

// Control Commands
app.get('/api/panic', (req, res) => {
  queueCommand('alarm');
  res.json({ status: 'ok', msg: 'Emergency SOS Panic Dispatched to ESP32!' });
});

app.get('/api/alarm', (req, res) => {
  queueCommand('alarm');
  res.json({ status: 'ok', msg: 'Siren Alarm Triggered!' });
});

app.get('/api/stopalarm', (req, res) => {
  queueCommand('stopalarm');
  res.json({ status: 'ok', msg: 'Siren / Audio Muted' });
});

app.get('/api/flash', (req, res) => {
  const state = req.query.state || '1';
  queueCommand('flash', state);
  res.json({ status: 'ok', msg: state === '1' ? 'Flash ON' : 'Flash OFF' });
});

app.get('/api/volume', (req, res) => {
  const val = req.query.val || '25';
  queueCommand('volume', val);
  res.json({ status: 'ok', msg: `Volume set to ${val}` });
});

app.get('/api/sensor', (req, res) => {
  const state = req.query.state || '1';
  queueCommand('sensor', state);
  res.json({ status: 'ok', msg: state === '1' ? 'Motion Guard ARMED' : 'Motion Guard DISARMED' });
});

app.get('/api/reboot', (req, res) => {
  queueCommand('reboot');
  res.json({ status: 'ok', msg: 'Reboot Command Sent to ESP32' });
});

app.get('/api/nightvision', (req, res) => {
  const state = req.query.state || '1';
  queueCommand('night', state);
  res.json({ status: 'ok', msg: state === '1' ? 'Night Vision Mode ENABLED' : 'Day Color Mode RESTORED' });
});

app.get('/api/speak', (req, res) => {
  const track = req.query.track || '5';
  queueCommand('speak', track);
  let label = 'Warning Voice';
  if (track === '5') label = '"Warning: Step away from door!"';
  else if (track === '6') label = '"Hello, how can I help you?"';
  else if (track === '7') label = '"Delivery accepted. Leave at door."';
  res.json({ status: 'ok', msg: `2-Way Voice Talkback Dispatched: ${label}` });
});

app.post('/api/live-speech', (req, res) => {
  if (req.body && req.body.length > 0) {
    const speechData = req.body;
    console.log(`[Live Walkie-Talkie] Received ${speechData.length} bytes of speech`);
    if (cameraStreamClient) {
      try {
        cameraStreamClient.write(`CMD:SPEECH:${speechData.length}\n`);
        cameraStreamClient.write(speechData);
      } catch (e) {
        cameraStreamClient = null;
      }
    }
    res.status(200).json({ status: 'ok', msg: 'Speech transmitted to camera speaker' });
  } else {
    res.status(400).send('No speech audio');
  }
});

// 6. Web Live Dashboard Console with 2-Way Communication
app.get('/', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Loki CCTV 24/7 Cloud Hub & 2-Way Talk</title>
  <style>
    * { box-sizing: border-box; font-family: 'Segoe UI', system-ui, sans-serif; }
    body { background: #0b0f19; color: #f8fafc; margin: 0; padding: 20px; display: flex; justify-content: center; }
    .container { max-width: 650px; width: 100%; background: #161e2e; border: 1px solid #1f293d; border-radius: 16px; padding: 24px; box-shadow: 0 10px 30px rgba(0,0,0,0.6); }
    h2 { color: #38bdf8; margin: 0 0 4px 0; text-align: center; }
    p.sub { text-align: center; color: #94a3b8; font-size: 13px; margin: 0 0 16px 0; }
    .badge { display: inline-flex; align-items: center; gap: 6px; padding: 6px 12px; background: #064e3b; color: #34d399; border-radius: 20px; font-weight: bold; font-size: 12px; }
    .video-card { width: 100%; height: 280px; background: #000; border: 1px solid #334155; border-radius: 12px; overflow: hidden; position: relative; margin: 16px 0; display: flex; align-items: center; justify-content: center; }
    .video-card img { width: 100%; height: 100%; object-fit: contain; }
    .section-title { font-size: 13px; font-weight: bold; color: #38bdf8; text-transform: uppercase; letter-spacing: 1px; margin: 16px 0 8px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 12px; }
    button { padding: 12px; font-size: 13px; font-weight: bold; border-radius: 8px; border: none; cursor: pointer; transition: 0.2s; }
    button:hover { opacity: 0.9; transform: translateY(-1px); }
    .btn-red { background: #dc2626; color: white; }
    .btn-blue { background: #0284c7; color: white; }
    .btn-green { background: #059669; color: white; }
    .btn-purple { background: #7c3aed; color: white; }
    .btn-orange { background: #d97706; color: white; }
    .btn-gray { background: #334155; color: white; }
    .sos-btn { width: 100%; background: #991b1b; border: 2px solid #ef4444; color: white; padding: 14px; font-size: 15px; font-weight: 900; border-radius: 10px; margin-bottom: 14px; cursor: pointer; }
    .status-box { background: #0f172a; padding: 12px; border-radius: 8px; font-size: 12px; color: #cbd5e1; border: 1px solid #1e293b; margin-top: 14px; }
  </style>
</head>
<body>
  <div class="container">
    <h2>LOKI CCTV CLOUD HUB</h2>
    <p class="sub">Worldwide 24/7 Live Stream & 2-Way Talkback</p>
    <div style="text-align: center;"><span class="badge" id="liveBadge">24/7 CLOUD ACTIVE</span></div>
    
    <div class="video-card">
      <img src="/stream" alt="Live Stream" onerror="this.src=''; this.alt='Connecting to Camera...';" />
    </div>

    <button class="sos-btn" onclick="executeApi('/api/panic')">1-TAP EMERGENCY SOS PANIC</button>

    <div class="section-title">2-Way Communication & Talkback</div>
    <div class="grid">
      <button class="btn-orange" onclick="executeApi('/api/speak?track=5')">Warning: Step Away!</button>
      <button class="btn-blue" onclick="executeApi('/api/speak?track=6')">Intercom: Hello!</button>
    </div>
    <div class="grid">
      <button class="btn-green" onclick="executeApi('/api/speak?track=7')">Leave Package</button>
      <button class="btn-gray" onclick="executeApi('/api/stopalarm')">Mute Audio</button>
    </div>

    <div class="section-title">Siren & Flash Controls</div>
    <div class="grid">
      <button class="btn-red" onclick="executeApi('/api/alarm')">Trigger Siren</button>
      <button class="btn-gray" onclick="executeApi('/api/stopalarm')">Stop Siren</button>
    </div>
    <div class="grid">
      <button class="btn-green" onclick="executeApi('/api/flash?state=1')">Flashlight ON</button>
      <button class="btn-gray" onclick="executeApi('/api/flash?state=0')">Flashlight OFF</button>
    </div>
    <div class="grid">
      <button class="btn-blue" onclick="window.open('/capture', '_blank')">HD Snapshot</button>
      <button class="btn-purple" onclick="executeApi('/api/nightvision?state=1')">Night Vision ON</button>
    </div>

    <div class="status-box" id="statusBox">
      Loading Camera Health & Telemetry...
    </div>
  </div>

  <script>
    async function executeApi(endpoint) {
      try {
        const res = await fetch(endpoint);
        const data = await res.json();
        alert(data.msg || 'Command Sent!');
        fetchStatus();
      } catch (err) {
        alert('Error sending command: ' + err.message);
      }
    }

    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        document.getElementById('statusBox').innerHTML = 
          '<b>WiFi Signal:</b> ' + (data.rssi || '-55') + ' dBm &nbsp;|&nbsp; ' +
          '<b>Uptime:</b> ' + (data.uptime || 0) + 's &nbsp;|&nbsp; ' +
          '<b>Active Viewers:</b> ' + (data.active_viewers || 0) + '<br>' +
          '<b>Motion Guard:</b> ' + (data.sensor ? 'ARMED' : 'DISARMED') + ' &nbsp;|&nbsp; ' +
          '<b>Live Status:</b> ' + (data.camera_live ? '<span style="color:#34d399;">ONLINE</span>' : '<span style="color:#f87171;">WAITING FOR ESP32</span>');
      } catch (e) {}
    }
    fetchStatus();
    setInterval(fetchStatus, 3000);
  </script>
</body>
</html>`);
});

app.listen(PORT, () => {
  console.log(`[Loki CCTV Cloud] Server running on port ${PORT}`);
});
