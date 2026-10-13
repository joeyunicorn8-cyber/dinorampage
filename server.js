const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 40;
const CHAT_HISTORY = 50;
const CHAT_MAX_LEN = 200;
const CHAT_MIN_INTERVAL_MS = 500;
const TICK_MS = 66; // ~15 state broadcasts per second
const WORLD_RADIUS = 200;

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Dino Rampage server running');
});

const wss = new WebSocketServer({ server, maxPayload: 4096 });

let nextId = 1;
const players = new Map(); // id -> player
const chatLog = [];

function clean(str, max) {
  return String(str == null ? '' : str).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, max);
}

function num(v, fallback = 0) {
  v = Number(v);
  return Number.isFinite(v) ? v : fallback;
}

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}

function broadcast(msg, exceptId) {
  const data = JSON.stringify(msg);
  for (const p of players.values()) {
    if (p.id !== exceptId && p.ws.readyState === 1) p.ws.send(data);
  }
}

function publicInfo(p) {
  return { id: p.id, name: p.name, emoji: p.emoji, color: p.color, skin: p.skin, inArena: p.inArena };
}

function lobbyList() {
  return Array.from(players.values()).map(publicInfo);
}

wss.on('connection', (ws) => {
  if (players.size >= MAX_PLAYERS) {
    send(ws, { t: 'full' });
    ws.close();
    return;
  }

  const player = {
    id: nextId++,
    ws,
    name: 'Player',
    emoji: '🦖',
    color: '#39ff14',
    skin: 'ranger',
    inArena: false,
    joined: false,
    x: 0, y: 0, z: 10, yaw: 0, pitch: 0,
    moving: false,
    mount: null,
    lastChat: 0
  };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.t !== 'string') return;

    if (msg.t === 'hello') {
      if (player.joined) return;
      player.name = clean(msg.name, 20) || 'Player';
      player.emoji = clean(msg.emoji, 8) || '🦖';
      player.color = /^#[0-9a-fA-F]{6}$/.test(msg.color) ? msg.color : '#39ff14';
      player.skin = clean(msg.skin, 20) || 'ranger';
      player.joined = true;
      players.set(player.id, player);
      send(ws, { t: 'welcome', id: player.id, players: lobbyList(), chat: chatLog });
      broadcast({ t: 'join', player: publicInfo(player) }, player.id);
      return;
    }

    if (!player.joined) return;

    if (msg.t === 'chat') {
      const now = Date.now();
      const text = clean(msg.text, CHAT_MAX_LEN);
      if (!text || now - player.lastChat < CHAT_MIN_INTERVAL_MS) return;
      player.lastChat = now;
      const entry = { user: player.name, text, time: now };
      chatLog.push(entry);
      if (chatLog.length > CHAT_HISTORY) chatLog.shift();
      broadcast({ t: 'chat', ...entry });
    } else if (msg.t === 'scene') {
      player.inArena = !!msg.inArena;
      broadcast({ t: 'update', player: publicInfo(player) });
    } else if (msg.t === 'skin') {
      player.skin = clean(msg.skin, 20) || player.skin;
      broadcast({ t: 'update', player: publicInfo(player) });
    } else if (msg.t === 'pos') {
      let x = num(msg.x), z = num(msg.z);
      const d = Math.hypot(x, z);
      if (d > WORLD_RADIUS) { x *= WORLD_RADIUS / d; z *= WORLD_RADIUS / d; }
      player.x = x;
      player.y = Math.max(-50, Math.min(200, num(msg.y)));
      player.z = z;
      player.yaw = num(msg.yaw);
      player.pitch = num(msg.pitch);
      player.moving = !!msg.moving;
      player.mount = msg.mount ? clean(msg.mount, 12) : null;
    }
  });

  ws.on('close', () => {
    if (player.joined && players.delete(player.id)) {
      broadcast({ t: 'leave', id: player.id });
    }
  });

  ws.on('error', () => {});
});

// Broadcast positions of everyone in the arena, ~15 times a second.
setInterval(() => {
  const arena = [];
  for (const p of players.values()) {
    if (!p.inArena) continue;
    arena.push({
      id: p.id,
      x: +p.x.toFixed(2), y: +p.y.toFixed(2), z: +p.z.toFixed(2),
      yaw: +p.yaw.toFixed(3), pitch: +p.pitch.toFixed(3),
      moving: p.moving, mount: p.mount
    });
  }
  if (arena.length < 2) return;
  broadcast({ t: 'state', players: arena });
}, TICK_MS);

// Drop dead connections (also keeps free hosts from idling the socket).
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => {
  console.log(`Dino Rampage server listening on port ${PORT}`);
});
