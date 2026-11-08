const express = require('express');
const { createServer } = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');

const app = express();

// Wide-open CORS — required when the frontend runs on a sandbox / Vercel / etc.
const corsOptions = { origin: '*', methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS', allowedHeaders: 'Content-Type,Authorization,X-Requested-With', credentials: false };
app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // Explicitly handle all OPTIONS preflights
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ limit: '10mb', extended: false }));

const PORT = process.env.PORT || 3001;

// ── In-memory stores ─────────────────────────────────────────────────────────
const chars       = new Map();   // shareId -> charData
const subscribers = new Map();   // shareId -> Set<WebSocket>

// ── Round state — one active round at a time ────────────────────────────────
// A "round" is when the DM pauses and waits for all players to declare actions.
// required: shareId[]  submitted: Map<shareId, {charName, text}>
let activeRound = null;

// ── Broadcast to all live subscribers of a shareId ───────────────────────────
function broadcast(shareId, msg) {
  const subs = subscribers.get(shareId);
  if (!subs) return;
  const str = JSON.stringify(msg);
  for (const ws of subs) {
    if (ws.readyState === ws.OPEN) ws.send(str);
    else subs.delete(ws); // prune dead sockets lazily
  }
}

// ── Health check (Railway pings this to verify the service is up) ─────────────
app.get('/', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));
app.get('/health', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));

// ── REST — Character ─────────────────────────────────────────────────────────
app.post('/api/char', (req, res) => {
  const char = req.body;
  if (!char?.shareId) return res.status(400).json({ error: 'Missing shareId' });
  chars.set(char.shareId, char);
  broadcast(char.shareId, { type: 'char_updated', char });
  res.json({ ok: true, shareId: char.shareId });
});

app.get('/api/char/:shareId', (req, res) => {
  const char = chars.get(req.params.shareId);
  if (!char) return res.status(404).json({ error: 'Not found' });
  res.json(char);
});

// ── Round helper — record a player submission and notify DM ─────────────────
function updateRound(shareId, charName, text) {
  if (!activeRound || !activeRound.required.includes(shareId)) return;
  activeRound.submitted.set(shareId, { charName, text });
  const submittedList = Array.from(activeRound.submitted.entries()).map(([id, v]) => ({ shareId: id, ...v }));
  const waiting = activeRound.required.filter(id => !activeRound.submitted.has(id));
  const complete = waiting.length === 0;
  const update = { type: 'round_update', roundId: activeRound.id, submitted: submittedList, waiting, complete };
  if (activeRound.dmWs?.readyState === 1 /* OPEN */) activeRound.dmWs.send(JSON.stringify(update));
  if (complete) { console.log(`[round] ${activeRound.id} complete`); activeRound = null; }
}

// ── REST — Player Action ─────────────────────────────────────────────────────
// Mobile players POST here; server broadcasts the action to the DM's browser.
app.post('/api/action', (req, res) => {
  const { shareId, charName, text } = req.body;
  if (!shareId || !text?.trim()) {
    return res.status(400).json({ error: 'Missing shareId or text' });
  }
  const msg = { type: 'player_action', shareId, charName: charName || 'Unknown', text: text.trim(), ts: Date.now() };
  broadcast(shareId, msg);
  console.log(`[action] ${charName} (${shareId}): ${text.slice(0, 80)}`);
  updateRound(shareId, charName || 'Unknown', text.trim()); // feed into round if active
  res.json({ ok: true });
});

// ── WebSocket ─────────────────────────────────────────────────────────────────
const server = createServer(app);
const wss    = new WebSocketServer({ server });

// Keep-alive ping every 30s so Railway doesn't close idle connections
const PING_INTERVAL = 30_000;
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL);

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  const joined = new Set();

  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw);

      // ── Subscribe to a shareId channel ──────────────────────────────────────
      if (msg.type === 'subscribe' && msg.shareId) {
        joined.add(msg.shareId);
        if (!subscribers.has(msg.shareId)) subscribers.set(msg.shareId, new Set());
        subscribers.get(msg.shareId).add(ws);
      }

      // ── DM starts a round — waits for all players to declare actions ────────
      if (msg.type === 'round_start') {
        activeRound = {
          id: Date.now().toString(36),
          required: Array.isArray(msg.shareIds) ? msg.shareIds : [],
          submitted: new Map(), // shareId → {charName, text}
          dmWs: ws,
        };
        // Notify every player channel
        activeRound.required.forEach(shareId => {
          broadcast(shareId, { type: 'round_open', roundId: activeRound.id, prompt: msg.prompt || '¿Qué hace tu personaje?' });
        });
        // Acknowledge to DM with initial state
        ws.send(JSON.stringify({ type: 'round_ack', roundId: activeRound.id, waiting: activeRound.required, submitted: [] }));
        console.log(`[round] Started ${activeRound.id}, waiting for: ${activeRound.required.join(', ')}`);
      }

      // ── DM cancels the round ────────────────────────────────────────────────
      if (msg.type === 'round_cancel') {
        if (activeRound) {
          activeRound.required.forEach(shareId => broadcast(shareId, { type: 'round_closed' }));
          activeRound = null;
          console.log('[round] Cancelled by DM');
        }
      }

      // ── Player submits action via WebSocket (bypasses REST / CSP issues) ────
      if (msg.type === 'player_action_ws') {
        const { shareId, charName, text, roundId } = msg;
        if (!shareId || !text?.trim()) return;
        const actionMsg = { type: 'player_action', shareId, charName: charName || 'Unknown', text: text.trim(), ts: Date.now() };
        broadcast(shareId, actionMsg);
        console.log(`[ws-action] ${charName} (${shareId}): ${text.slice(0, 80)}`);
        updateRound(shareId, charName || 'Unknown', text.trim());
        broadcast(shareId, { type: 'round_action_ack', charName });
      }

      // ── DM sends a roll request to a specific player ─────────────────────────
      if (msg.type === 'roll_request') {
        const { shareId, charName, roll, dc, reason } = msg;
        if (shareId) {
          broadcast(shareId, { type: 'roll_request', charName, roll, dc, reason, ts: Date.now() });
          console.log(`[roll-req] ${charName} (${shareId}): ${reason} ${roll} DC${dc}`);
        }
      }

    } catch {}
  });

  ws.on('close', () => {
    for (const id of joined) subscribers.get(id)?.delete(ws);
    // If the DM disconnects during a round, clean up
    if (activeRound?.dmWs === ws) activeRound = null;
  });
});

// ── Global error handler — prevents unhandled exceptions returning 502 ───────
// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('[error]', err?.message || err);
  res.status(err.status || 500).json({ error: err.message || 'Internal server error' });
});

// ── Start — bind to 0.0.0.0 so Railway can reach it ─────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`✦ DnD Sync server  http://0.0.0.0:${PORT}`);
  console.log(`  WebSocket        ws://0.0.0.0:${PORT}`);
  console.log(`  Node             ${process.version}`);
  console.log(`  ENV PORT         ${process.env.PORT ?? '(not set, using 3001)'}`);
});

server.on('error', (err) => {
  console.error('[startup] Server error:', err.message);
  process.exit(1);
});

process.on('uncaughtException',  err => { console.error('[uncaught]', err); process.exit(1); });
process.on('unhandledRejection', err => { console.error('[unhandled]', err); });
