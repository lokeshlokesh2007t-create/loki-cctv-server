const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors({ origin: '*' }));
app.use(bodyParser.raw({ type: ['image/jpeg', 'application/octet-stream', 'audio/*'], limit: '10mb' }));
app.use(express.json());

// =====================================================================================
// MULTI-CAMERA IN-MEMORY STATE (Supports Cam 1, Cam 2, Cam 3)
// =====================================================================================
const cameras = {
  1: {
    id: 1,
    name: 'Camera 1 (Main ESP32-S3)',
    latestFrame: null,
    lastFrameTime: 0,
    streamClients: [],
    frameCounter: 0,
    telemetry: {
      status: 'online',
      uptime: 0,
      rssi: -55,
      volume: 25,
      sensor: true,
      sd_ready: true,
      lastSeen: new Date().toISOString()
    }
  },
  2: {
    id: 2,
    name: 'Camera 2 (Angle 2 Small Board)',
    latestFrame: null,
    lastFrameTime: 0,
    streamClients: [],
    frameCounter: 0,
    telemetry: {
      status: 'online',
      uptime: 0,
      rssi: -60,
      sensor: true,
      lastSeen: new Date().toISOString()
    }
  },
  3: {
    id: 3,
    name: 'Camera 3 (Angle 3 Small Board)',
    latestFrame: null,
    lastFrameTime: 0,
    streamClients: [],
    frameCounter: 0,
    telemetry: {
      status: 'online',
      uptime: 0,
      rssi: -60,
      sensor: true,
      lastSeen: new Date().toISOString()
    }
  }
};

let pendingCommands = [];
let commandCounter = 1;
let cameraStreamClient = null; // Real-Time Push Tunnel Socket
let pendingSpeech = null;
let speechSeq = 0;

function resolveCamId(req) {
  const c = req.query.cam || req.headers['x-camera-id'] || req.params.camId;
  if (c === '2' || c === 2 || c === 'cam2') return 2;
  if (c === '3' || c === 3 || c === 'cam3') return 3;
  return 1; // Default to Camera 1 for 100% backwards compatibility
}

// =====================================================================================
// 1. FRAME INGESTION ENDPOINTS (Supports All 3 Cameras)
// =====================================================================================
function handleFrameUpload(req, res) {
  const camId = resolveCamId(req);
  const cam = cameras[camId] || cameras[1];

  if (req.body && req.body.length > 0) {
    cam.latestFrame = req.body;
    cam.lastFrameTime = Date.now();
    cam.frameCounter++;

    if (cam.frameCounter === 1 || cam.frameCounter % 20 === 0) {
      console.log(`[Frame Cam ${camId}] Ingested frame #${cam.frameCounter} (${cam.latestFrame.length} bytes). Viewers: ${cam.streamClients.length}`);
    }

    // Broadcast frame to all active MJPEG viewers of this specific camera
    for (let i = cam.streamClients.length - 1; i >= 0; i--) {
      const client = cam.streamClients[i];
      try {
        client.write('--boundarydonotcross\r\n');
        client.write('Content-Type: image/jpeg\r\n');
        client.write('Content-Length: ' + cam.latestFrame.length + '\r\n\r\n');
        client.write(cam.latestFrame, 'binary');
        client.write('\r\n');
      } catch (err) {
        cam.streamClients.splice(i, 1);
      }
    }
    res.status(200).send('OK');
  } else {
    res.status(400).send('No Frame Data');
  }
}

app.post('/upload-frame', handleFrameUpload);
app.post('/upload-frame/:camId', handleFrameUpload);
app.post('/api/device/frame', handleFrameUpload);
app.post('/api/device/frame/:camId', handleFrameUpload);

// =====================================================================================
// 2. LIVE MJPEG STREAMS (Supports Cam 1, Cam 2, Cam 3)
// =====================================================================================
function handleStreamRequest(req, res) {
  const camId = resolveCamId(req);
  const cam = cameras[camId] || cameras[1];

  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace;boundary=boundarydonotcross',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Expires': '0',
    'Connection': 'close',
    'Access-Control-Allow-Origin': '*'
  });

  cam.streamClients.push(res);
  console.log(`[Stream Cam ${camId}] Viewer connected. Total viewers: ${cam.streamClients.length}`);

  // Send current frame immediately if available
  if (cam.latestFrame) {
    res.write('--boundarydonotcross\r\n');
    res.write('Content-Type: image/jpeg\r\n');
    res.write('Content-Length: ' + cam.latestFrame.length + '\r\n\r\n');
    res.write(cam.latestFrame, 'binary');
    res.write('\r\n');
  }

  req.on('close', () => {
    const idx = cam.streamClients.indexOf(res);
    if (idx !== -1) cam.streamClients.splice(idx, 1);
    console.log(`[Stream Cam ${camId}] Viewer disconnected. Remaining: ${cam.streamClients.length}`);
  });
}

app.get('/stream', handleStreamRequest);
app.get('/stream/:camId', handleStreamRequest);

// =====================================================================================
// 3. HD SNAPSHOT ENDPOINTS
// =====================================================================================
function handleCaptureRequest(req, res) {
  const camId = resolveCamId(req);
  const cam = cameras[camId] || cameras[1];

  if (cam.latestFrame) {
    res.writeHead(200, {
      'Content-Type': 'image/jpeg',
      'Content-Length': cam.latestFrame.length,
      'Access-Control-Allow-Origin': '*'
    });
    res.end(cam.latestFrame);
  } else {
    res.status(503).json({ error: `Camera ${camId} offline or waiting for first frame` });
  }
}

app.get('/capture', handleCaptureRequest);
app.get('/capture/:camId', handleCaptureRequest);

// =====================================================================================
// 4. TELEMETRY & MULTI-CAMERA HEALTH STATUS
// =====================================================================================
function handleTelemetry(req, res) {
  const camId = resolveCamId(req);
  const cam = cameras[camId] || cameras[1];
  if (req.body) {
    cam.telemetry = { ...cam.telemetry, ...req.body, lastSeen: new Date().toISOString() };
  }
  res.status(200).json({ status: 'ok', cam: camId });
}

app.post('/api/telemetry', handleTelemetry);
app.post('/api/device/telemetry', handleTelemetry);

app.get('/api/status', (req, res) => {
  const now = Date.now();
  const cam1Live = (now - cameras[1].lastFrameTime) < 15000;
  const cam2Live = (now - cameras[2].lastFrameTime) < 15000;
  const cam3Live = (now - cameras[3].lastFrameTime) < 15000;

  res.json({
    status: 'online',
    camera_live: cam1Live || cam2Live || cam3Live,
    cameras: {
      1: {
        name: cameras[1].name,
        live: cam1Live,
        viewers: cameras[1].streamClients.length,
        frames: cameras[1].frameCounter,
        rssi: cameras[1].telemetry.rssi || -55,
        lastSeen: cameras[1].lastFrameTime ? new Date(cameras[1].lastFrameTime).toISOString() : null
      },
      2: {
        name: cameras[2].name,
        live: cam2Live,
        viewers: cameras[2].streamClients.length,
        frames: cameras[2].frameCounter,
        rssi: cameras[2].telemetry.rssi || -60,
        lastSeen: cameras[2].lastFrameTime ? new Date(cameras[2].lastFrameTime).toISOString() : null
      },
      3: {
        name: cameras[3].name,
        live: cam3Live,
        viewers: cameras[3].streamClients.length,
        frames: cameras[3].frameCounter,
        rssi: cameras[3].telemetry.rssi || -60,
        lastSeen: cameras[3].lastFrameTime ? new Date(cameras[3].lastFrameTime).toISOString() : null
      }
    },
    total_viewers: cameras[1].streamClients.length + cameras[2].streamClients.length + cameras[3].streamClients.length,
    timestamp: new Date().toISOString()
  });
});

// =====================================================================================
// 5. HARDWARE BIDIRECTIONAL CONTROL CHANNEL (Camera 1 Actuators)
// =====================================================================================
app.get('/api/device/commands', (req, res) => {
  if (pendingCommands.length > 0) {
    const cmd = pendingCommands.shift();
    res.json(cmd);
  } else {
    res.status(204).send();
  }
});

app.get('/camera-stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/plain; charset=utf-8',
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

function queueCommand(action, param = '', targetCam = 1) {
  const cmdId = commandCounter++;
  const payload = `CMD:${action}:${param}\n`;
  if (cameraStreamClient) {
    try {
      cameraStreamClient.write(payload);
    } catch (e) {
      cameraStreamClient = null;
    }
  }
  pendingCommands.push({ id: cmdId, action, param, targetCam, timestamp: Date.now() });
  if (pendingCommands.length > 30) pendingCommands.shift();
}

// Actuator Endpoints
app.get('/api/panic', (req, res) => {
  queueCommand('alarm');
  res.json({ status: 'ok', msg: 'Emergency SOS Panic Dispatched to ESP32!' });
});

app.get('/api/alarm', (req, res) => {
  queueCommand('alarm');
  res.json({ status: 'ok', msg: 'Siren Alarm Triggered!' });
});

app.get('/api/play', (req, res) => {
  const track = req.query.track || '1';
  queueCommand('play', track);
  res.json({ status: 'ok', msg: `Playing Track #${track} on DFPlayer!` });
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

app.get('/api/flip', (req, res) => {
  queueCommand('flip');
  res.json({ status: 'ok', msg: 'Flip Command Queued for ESP32' });
});

app.get('/api/mirror', (req, res) => {
  queueCommand('mirror');
  res.json({ status: 'ok', msg: 'Mirror Command Queued for ESP32' });
});

app.get('/api/rotate', (req, res) => {
  queueCommand('rotate');
  res.json({ status: 'ok', msg: 'Rotate 180 Command Queued for ESP32' });
});

app.get('/api/wide', (req, res) => {
  queueCommand('wide');
  res.json({ status: 'ok', msg: 'Wide Angle (SVGA) Queued for ESP32' });
});

app.get('/api/vga', (req, res) => {
  queueCommand('vga');
  res.json({ status: 'ok', msg: 'Standard VGA Queued for ESP32' });
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

// Live Speech Walkie-Talkie
app.post('/api/live-speech', (req, res) => {
  if (req.body && req.body.length > 0) {
    const speechData = req.body;
    speechSeq++;
    pendingSpeech = {
      id: speechSeq,
      data: speechData,
      timestamp: Date.now()
    };
    console.log(`[Live Walkie-Talkie] Received speech #${speechSeq} (${speechData.length} bytes)`);
    queueCommand('speech', String(speechData.length));
    res.status(200).json({ status: 'ok', msg: 'Speech transmitted to camera speaker' });
  } else {
    res.status(400).send('No speech audio');
  }
});

app.get('/api/device/speech', (req, res) => {
  if (pendingSpeech && (Date.now() - pendingSpeech.timestamp < 30000)) {
    const speechBuf = pendingSpeech.data;
    pendingSpeech = null;
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': speechBuf.length,
      'Connection': 'close'
    });
    res.end(speechBuf);
  } else {
    res.status(204).send();
  }
});

// APK Download
function sendApkDownload(req, res) {
  const apkPath = path.join(__dirname, 'CCTVLoki.apk');
  if (fs.existsSync(apkPath)) {
    res.download(apkPath, 'CCTVLoki.apk');
  } else {
    res.status(404).send('APK file not uploaded to server yet.');
  }
}

app.get('/download', sendApkDownload);
app.get('/app', sendApkDownload);

// =====================================================================================
// 6. MODERN TRIPLE-CAMERA DASHBOARD CONSOLE (3-SPLIT & TAB SELECTOR)
// =====================================================================================
app.get('/', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Loki CCTV - Triple-Camera Security Console</title>
  <style>
    * { box-sizing: border-box; font-family: 'Segoe UI', system-ui, sans-serif; }
    body { background: #0b0f19; color: #f8fafc; margin: 0; padding: 16px; display: flex; justify-content: center; }
    .container { max-width: 1050px; width: 100%; background: #161e2e; border: 1px solid #1f293d; border-radius: 16px; padding: 20px; box-shadow: 0 10px 30px rgba(0,0,0,0.6); }
    h2 { color: #38bdf8; margin: 0 0 4px 0; text-align: center; }
    p.sub { text-align: center; color: #94a3b8; font-size: 13px; margin: 0 0 16px 0; }
    .tab-bar { display: flex; gap: 8px; justify-content: center; margin-bottom: 16px; flex-wrap: wrap; }
    .tab-btn { background: #1e293b; color: #94a3b8; border: 1px solid #334155; padding: 8px 16px; border-radius: 8px; font-size: 13px; font-weight: bold; cursor: pointer; transition: 0.2s; }
    .tab-btn.active { background: #0284c7; color: white; border-color: #38bdf8; }
    .cam-layout { display: grid; gap: 14px; margin-bottom: 16px; }
    .cam-layout.all { grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
    .cam-layout.single { grid-template-columns: 1fr; }
    .cam-card { background: #0f172a; border: 1px solid #334155; border-radius: 12px; overflow: hidden; display: flex; flex-direction: column; }
    .cam-header { background: #1e293b; padding: 8px 12px; display: flex; justify-content: space-between; align-items: center; font-size: 12px; font-weight: bold; }
    .cam-feed { width: 100%; height: 240px; background: #000; display: flex; align-items: center; justify-content: center; overflow: hidden; position: relative; }
    .cam-feed img { width: 100%; height: 100%; object-fit: contain; }
    .cam-footer { padding: 8px 12px; display: flex; gap: 8px; background: #161e2e; }
    .badge { padding: 4px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; }
    .badge-live { background: #064e3b; color: #34d399; }
    .badge-wait { background: #7f1d1d; color: #f87171; }
    .section-title { font-size: 12px; font-weight: bold; color: #38bdf8; text-transform: uppercase; letter-spacing: 1px; margin: 16px 0 8px 0; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    button { padding: 12px; font-size: 13px; font-weight: bold; border-radius: 8px; border: none; cursor: pointer; transition: 0.2s; }
    button:hover { opacity: 0.9; transform: translateY(-1px); }
    .btn-red { background: #dc2626; color: white; }
    .btn-blue { background: #0284c7; color: white; }
    .btn-green { background: #059669; color: white; }
    .btn-purple { background: #7c3aed; color: white; }
    .btn-orange { background: #d97706; color: white; }
    .btn-gray { background: #334155; color: white; }
    .sos-btn { width: 100%; background: #991b1b; border: 2px solid #ef4444; color: white; padding: 14px; font-size: 15px; font-weight: 900; border-radius: 10px; margin-bottom: 14px; cursor: pointer; }
    .status-box { background: #0f172a; padding: 12px; border-radius: 8px; font-size: 12px; color: #cbd5e1; border: 1px solid #1e293b; margin-top: 14px; line-height: 1.5; }
  </style>
</head>
<body>
  <div class="container">
    <h2>LOKI CCTV MULTI-ANGLE HUB</h2>
    <p class="sub">24/7 Triple-Camera Cloud Console & Real-Time Security</p>

    <!-- Tab View Switcher -->
    <div class="tab-bar">
      <button class="tab-btn active" id="tabAll" onclick="setViewMode('all')">📺 Triple View (All 3)</button>
      <button class="tab-btn" id="tabCam1" onclick="setViewMode('cam1')">📹 Cam 1 (Main S3)</button>
      <button class="tab-btn" id="tabCam2" onclick="setViewMode('cam2')">📹 Cam 2 (Angle 2)</button>
      <button class="tab-btn" id="tabCam3" onclick="setViewMode('cam3')">📹 Cam 3 (Angle 3)</button>
    </div>

    <!-- Multi-Camera Video Layout -->
    <div class="cam-layout all" id="camLayout">
      <!-- Camera 1 Box -->
      <div class="cam-card" id="cardCam1">
        <div class="cam-header">
          <span>📹 CAM 1 (Main ESP32-S3)</span>
          <span class="badge badge-wait" id="badgeCam1">CHECKING...</span>
        </div>
        <div class="cam-feed">
          <img id="imgCam1" src="/stream?cam=1" alt="Camera 1 Feed" onerror="this.alt='Connecting to Cam 1...';" />
        </div>
        <div class="cam-footer">
          <button class="btn-blue" style="flex:1;padding:8px;" id="rotBtn1" onclick="cycleRotate(1)">🔄 Rotate Cam 1</button>
          <button class="btn-gray" style="flex:1;padding:8px;" onclick="window.open('/capture?cam=1', '_blank')">📸 Snapshot</button>
        </div>
      </div>

      <!-- Camera 2 Box -->
      <div class="cam-card" id="cardCam2">
        <div class="cam-header">
          <span>📹 CAM 2 (Side Angle 1)</span>
          <span class="badge badge-wait" id="badgeCam2">CHECKING...</span>
        </div>
        <div class="cam-feed">
          <img id="imgCam2" src="/stream?cam=2" alt="Camera 2 Feed" onerror="this.alt='Connecting to Cam 2...';" />
        </div>
        <div class="cam-footer">
          <button class="btn-blue" style="flex:1;padding:8px;" id="rotBtn2" onclick="cycleRotate(2)">🔄 Rotate Cam 2</button>
          <button class="btn-gray" style="flex:1;padding:8px;" onclick="window.open('/capture?cam=2', '_blank')">📸 Snapshot</button>
        </div>
      </div>

      <!-- Camera 3 Box -->
      <div class="cam-card" id="cardCam3">
        <div class="cam-header">
          <span>📹 CAM 3 (Side Angle 2)</span>
          <span class="badge badge-wait" id="badgeCam3">CHECKING...</span>
        </div>
        <div class="cam-feed">
          <img id="imgCam3" src="/stream?cam=3" alt="Camera 3 Feed" onerror="this.alt='Connecting to Cam 3...';" />
        </div>
        <div class="cam-footer">
          <button class="btn-blue" style="flex:1;padding:8px;" id="rotBtn3" onclick="cycleRotate(3)">🔄 Rotate Cam 3</button>
          <button class="btn-gray" style="flex:1;padding:8px;" onclick="window.open('/capture?cam=3', '_blank')">📸 Snapshot</button>
        </div>
      </div>
    </div>

    <!-- Emergency SOS Siren -->
    <button class="sos-btn" onclick="executeApi('/api/panic')">🚨 EMERGENCY SOS ALARM (MAX SOUND + STROBE)</button>

    <!-- Live Push-To-Talk Walkie-Talkie -->
    <button class="btn-orange" style="width:100%;margin-bottom:14px;padding:14px;" id="pttBtn">🗣️ HOLD TO SPEAK (LIVE MIC WALKIE-TALKIE)</button>

    <!-- Camera 1 Actuator Controls -->
    <div class="section-title">Hardware Actuators & Voice Intercom (Camera 1)</div>
    <div class="grid">
      <button class="btn-green" onclick="executeApi('/api/speak?track=5')">🗣️ Voice: Step Away!</button>
      <button class="btn-green" onclick="executeApi('/api/speak?track=6')">🗣️ Voice: Can I Help You?</button>
      <button class="btn-green" onclick="executeApi('/api/speak?track=7')">📦 Voice: Leave Package</button>
      <button class="btn-red" onclick="executeApi('/api/alarm')">📢 Sound Loud Siren</button>
      <button class="btn-gray" onclick="executeApi('/api/stopalarm')">🔇 Mute Audio</button>
      <button class="btn-purple" onclick="executeApi('/api/flash?state=1')">💡 Flash LED ON</button>
      <button class="btn-gray" onclick="executeApi('/api/flash?state=0')">💡 Flash LED OFF</button>
      <button class="btn-purple" onclick="executeApi('/api/nightvision?state=1')">🌙 Night Vision ON</button>
      <button class="btn-blue" onclick="executeApi('/api/nightvision?state=0')">☀️ Day Color Mode</button>
      <button class="btn-orange" onclick="executeApi('/api/sensor?state=1')">🛡️ Arm PIR Guard</button>
      <button class="btn-gray" onclick="executeApi('/api/sensor?state=0')">🔓 Disarm Guard</button>
      <button class="btn-red" onclick="executeApi('/api/reboot')">🔄 Reboot Main ESP32</button>
    </div>

    <!-- Status & Telemetry Console -->
    <div class="status-box" id="statusBox">
      Connecting to Telemetry Services...
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
        alert('Error: ' + err.message);
      }
    }

    function setViewMode(mode) {
      const layout = document.getElementById('camLayout');
      const c1 = document.getElementById('cardCam1');
      const c2 = document.getElementById('cardCam2');
      const c3 = document.getElementById('cardCam3');
      const tAll = document.getElementById('tabAll');
      const tCam1 = document.getElementById('tabCam1');
      const tCam2 = document.getElementById('tabCam2');
      const tCam3 = document.getElementById('tabCam3');

      tAll.classList.remove('active');
      tCam1.classList.remove('active');
      tCam2.classList.remove('active');
      tCam3.classList.remove('active');

      if (mode === 'all') {
        layout.className = 'cam-layout all';
        c1.style.display = 'flex';
        c2.style.display = 'flex';
        c3.style.display = 'flex';
        tAll.classList.add('active');
      } else if (mode === 'cam1') {
        layout.className = 'cam-layout single';
        c1.style.display = 'flex';
        c2.style.display = 'none';
        c3.style.display = 'none';
        tCam1.classList.add('active');
      } else if (mode === 'cam2') {
        layout.className = 'cam-layout single';
        c1.style.display = 'none';
        c2.style.display = 'flex';
        c3.style.display = 'none';
        tCam2.classList.add('active');
      } else if (mode === 'cam3') {
        layout.className = 'cam-layout single';
        c1.style.display = 'none';
        c2.style.display = 'none';
        c3.style.display = 'flex';
        tCam3.classList.add('active');
      }
    }

    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        const c1Live = data.cameras && data.cameras[1] && data.cameras[1].live;
        const c2Live = data.cameras && data.cameras[2] && data.cameras[2].live;
        const c3Live = data.cameras && data.cameras[3] && data.cameras[3].live;

        const b1 = document.getElementById('badgeCam1');
        const b2 = document.getElementById('badgeCam2');
        const b3 = document.getElementById('badgeCam3');

        b1.className = 'badge ' + (c1Live ? 'badge-live' : 'badge-wait');
        b1.innerText = c1Live ? 'LIVE ONLINE' : 'OFFLINE';

        b2.className = 'badge ' + (c2Live ? 'badge-live' : 'badge-wait');
        b2.innerText = c2Live ? 'LIVE ONLINE' : 'WAITING FOR FEED';

        b3.className = 'badge ' + (c3Live ? 'badge-live' : 'badge-wait');
        b3.innerText = c3Live ? 'LIVE ONLINE' : 'WAITING FOR FEED';

        document.getElementById('statusBox').innerHTML = 
          '<b>Camera 1 (Main S3):</b> ' + (c1Live ? '<span style="color:#34d399;">ONLINE</span>' : '<span style="color:#f87171;">OFFLINE</span>') + ' | Signal: ' + (data.cameras[1].rssi || '-55') + ' dBm | Frames: ' + (data.cameras[1].frames || 0) + '<br>' +
          '<b>Camera 2 (Angle 2):</b> ' + (c2Live ? '<span style="color:#34d399;">ONLINE</span>' : '<span style="color:#f87171;">WAITING</span>') + ' | Signal: ' + (data.cameras[2].rssi || '-60') + ' dBm | Frames: ' + (data.cameras[2].frames || 0) + '<br>' +
          '<b>Camera 3 (Angle 3):</b> ' + (c3Live ? '<span style="color:#34d399;">ONLINE</span>' : '<span style="color:#f87171;">WAITING</span>') + ' | Signal: ' + (data.cameras[3].rssi || '-60') + ' dBm | Frames: ' + (data.cameras[3].frames || 0) + '<br>' +
          '<b>Total Cloud Viewers:</b> ' + (data.total_viewers || 0);
      } catch (e) {}
    }
    fetchStatus();
    setInterval(fetchStatus, 3000);

    let rotCam1 = parseInt(localStorage.getItem('loki_cam1_rot') || '0', 10);
    let rotCam2 = parseInt(localStorage.getItem('loki_cam2_rot') || '0', 10);
    let rotCam3 = parseInt(localStorage.getItem('loki_cam3_rot') || '0', 10);

    function applyRot(camId) {
      if (camId === 1) {
        document.getElementById('imgCam1').style.transform = 'rotate(' + rotCam1 + 'deg)';
        document.getElementById('rotBtn1').innerText = '🔄 Rotate Cam 1 (' + rotCam1 + '°)';
        localStorage.setItem('loki_cam1_rot', rotCam1);
      } else if (camId === 2) {
        document.getElementById('imgCam2').style.transform = 'rotate(' + rotCam2 + 'deg)';
        document.getElementById('rotBtn2').innerText = '🔄 Rotate Cam 2 (' + rotCam2 + '°)';
        localStorage.setItem('loki_cam2_rot', rotCam2);
      } else if (camId === 3) {
        document.getElementById('imgCam3').style.transform = 'rotate(' + rotCam3 + 'deg)';
        document.getElementById('rotBtn3').innerText = '🔄 Rotate Cam 3 (' + rotCam3 + '°)';
        localStorage.setItem('loki_cam3_rot', rotCam3);
      }
    }

    function cycleRotate(camId) {
      if (camId === 1) {
        rotCam1 = (rotCam1 + 90) % 360;
        applyRot(1);
      } else if (camId === 2) {
        rotCam2 = (rotCam2 + 90) % 360;
        applyRot(2);
      } else if (camId === 3) {
        rotCam3 = (rotCam3 + 90) % 360;
        applyRot(3);
      }
    }
    applyRot(1);
    applyRot(2);
    applyRot(3);

    // Push To Talk Live Walkie-Talkie
    const pBtn = document.getElementById('pttBtn');
    let pttStream, pttProc, pttCtx, pcmData = [];

    async function startPtt(e) {
      if (e) e.preventDefault();
      try {
        pttStream = await navigator.mediaDevices.getUserMedia({ audio: { sampleRate: 16000, channelCount: 1 } });
        pttCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 16000 });
        let src = pttCtx.createMediaStreamSource(pttStream);
        pttProc = pttCtx.createScriptProcessor(1024, 1, 1);
        pcmData = [];
        pttProc.onaudioprocess = ev => {
          let f = ev.inputBuffer.getChannelData(0);
          for (let i = 0; i < f.length; i++) {
            let s = Math.max(-1, Math.min(1, f[i]));
            pcmData.push(s < 0 ? s * 0x8000 : s * 0x7FFF);
          }
        };
        src.connect(pttProc);
        pttProc.connect(pttCtx.destination);
        pBtn.innerText = '🔴 Speaking Live... Release to Send';
        pBtn.style.background = '#dc2626';
      } catch (err) {
        alert('Microphone error: ' + err.message);
      }
    }

    function stopPtt(e) {
      if (e) e.preventDefault();
      if (!pttStream) return;
      pttStream.getTracks().forEach(t => t.stop());
      if (pttCtx) pttCtx.close();
      let buf = new Int16Array(pcmData);
      if (buf.length > 0) {
        fetch('/api/live-speech', {
          method: 'POST',
          body: buf.buffer,
          headers: { 'Content-Type': 'application/octet-stream' }
        }).then(r => r.json()).then(d => {
          pBtn.innerText = '✅ Speech Broadcasted!';
          pBtn.style.background = '#059669';
          setTimeout(() => {
            pBtn.innerText = '🗣️ HOLD TO SPEAK (LIVE MIC WALKIE-TALKIE)';
            pBtn.style.background = '#ea580c';
          }, 1500);
        }).catch(err => {
          alert('Failed to send speech: ' + err.message);
          pBtn.innerText = '🗣️ HOLD TO SPEAK (LIVE MIC WALKIE-TALKIE)';
          pBtn.style.background = '#ea580c';
        });
      }
      pttStream = null;
    }

    pBtn.addEventListener('mousedown', startPtt);
    pBtn.addEventListener('touchstart', startPtt);
    pBtn.addEventListener('mouseup', stopPtt);
    pBtn.addEventListener('touchend', stopPtt);
  </script>
</body>
</html>`);
});

app.listen(PORT, () => {
  console.log(`[Loki CCTV Cloud] Triple-Camera Multi-Angle Server running on port ${PORT}`);
});
