const http = require('http');
const fs = require('fs');
const path = require('path');
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

const wss = new WebSocketServer({ server, maxPayload: 32768 });

let nextId = 1;
const players = new Map(); // id -> player
const chatLog = [];
const worldSeed = Math.floor(Math.random() * 2147483646) + 1;
let hostId = null;

// Event kinds clients may relay to each other, and who may send them.
const HOST_ONLY_KINDS = new Set(['dsnap', 'wsnap', 'dmg', 'dkill', 'tamed', 'mountdead']);
const ALLOWED_KINDS = new Set([
  'dsnap', 'wsnap', 'reqsnap', 'dhit', 'feed', 'mount', 'dismount', 'stay', 'ride',
  'dmg', 'dkill', 'tamed', 'mountdead', 'mdmg', 'thit', 'sgc', 'lplace', 'lhit', 'phit', 'pkill'
]);
const PVP_KILL_WINDOW_MS = 6000;
const PVP_KILL_PAIR_COOLDOWN_MS = 15000; // caps how fast the same two players can trade kills
const PVP_RANGE = 8;
const PVP_MIN_INTERVAL_MS = 250;

// Every name that has ever joined, so the lobby can list offline players too.
// Kept in memory and saved to a file when possible (free hosts may reset the file).
const REGISTRY_FILE = path.join(__dirname, 'registry.json');
const registry = new Map();
const killCooldowns = new Map();
try {
  JSON.parse(fs.readFileSync(REGISTRY_FILE, 'utf8')).forEach((e) => registry.set(e.name.toLowerCase(), e));
} catch (e) { /* no saved registry yet */ }
let registrySaveTimer = null;
function saveRegistrySoon() {
  clearTimeout(registrySaveTimer);
  registrySaveTimer = setTimeout(() => {
    fs.writeFile(REGISTRY_FILE, JSON.stringify(Array.from(registry.values())), () => {});
  }, 2000);
}
function knownList() {
  return Array.from(registry.values()).map((e) => ({ name: e.name, emoji: e.emoji, color: e.color }));
}
const EVENT_RATE_LIMIT = 120; // events per second per player

// The host's browser runs the dino simulation, and browsers freeze hidden or
// covered windows — so prefer a visible arena player, and keep the current
// host while it stays visible (no flip-flopping).
function electHost() {
  const current = players.get(hostId);
  if (current && current.inArena && current.visible) return;
  let bestVisible = null;
  let bestAny = null;
  for (const p of players.values()) {
    if (!p.inArena) continue;
    if (bestAny === null || p.id < bestAny) bestAny = p.id;
    if (p.visible && (bestVisible === null || p.id < bestVisible)) bestVisible = p.id;
  }
  let best = bestVisible !== null ? bestVisible : (current && current.inArena ? hostId : bestAny);
  if (best !== hostId) {
    hostId = best;
    broadcast({ t: 'host', id: hostId });
  }
}

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
    lastChat: 0,
    evWindowStart: 0,
    evCount: 0,
    visible: true,
    lastHit: 0,
    lastPvp: null
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
      registry.set(player.name.toLowerCase(), { name: player.name, emoji: player.emoji, color: player.color });
      saveRegistrySoon();
      send(ws, { t: 'welcome', id: player.id, players: lobbyList(), chat: chatLog, seed: worldSeed, host: hostId, known: knownList() });
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
      electHost();
    } else if (msg.t === 'vis') {
      player.visible = !!msg.visible;
      electHost();
    } else if (msg.t === 'ev') {
      if (typeof msg.k !== 'string' || !ALLOWED_KINDS.has(msg.k)) return;
      if (HOST_ONLY_KINDS.has(msg.k) && player.id !== hostId) return;
      const now = Date.now();
      if (msg.k === 'phit') {
        const victim = typeof msg.to === 'number' ? players.get(msg.to) : null;
        if (!victim || victim === player || !victim.inArena || !player.inArena) return;
        if (Math.hypot(victim.x - player.x, victim.z - player.z) > PVP_RANGE) return;
        if (now - player.lastHit < PVP_MIN_INTERVAL_MS) return;
        player.lastHit = now;
        player.lastPvp = { victim: victim.id, time: now };
      }
      if (msg.k === 'pkill') {
        // A kill reward only goes to a player who really hit this victim moments ago.
        const killer = typeof msg.to === 'number' ? players.get(msg.to) : null;
        if (!killer || !killer.lastPvp || killer.lastPvp.victim !== player.id) return;
        if (now - killer.lastPvp.time > PVP_KILL_WINDOW_MS) return;
        const pairKey = killer.id + ':' + player.id;
        if (now - (killCooldowns.get(pairKey) || 0) < PVP_KILL_PAIR_COOLDOWN_MS) return;
        killCooldowns.set(pairKey, now);
        killer.lastPvp = null;
      }
      if (now - player.evWindowStart > 1000) { player.evWindowStart = now; player.evCount = 0; }
      if (++player.evCount > EVENT_RATE_LIMIT) return;
      const out = JSON.stringify({ t: 'ev', from: player.id, k: msg.k, d: msg.d });
      if (msg.to === 'host') {
        const h = players.get(hostId);
        if (h && h !== player && h.ws.readyState === 1) h.ws.send(out);
      } else if (typeof msg.to === 'number') {
        const target = players.get(msg.to);
        if (target && target.ws.readyState === 1) target.ws.send(out);
      } else {
        for (const p of players.values()) {
          if (p !== player && p.inArena && p.ws.readyState === 1) p.ws.send(out);
        }
      }
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
      electHost();
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
