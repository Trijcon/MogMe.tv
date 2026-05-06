const express    = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors       = require('cors');
const http       = require('http');

const app    = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

/* ════════════════════════════════
   FIREBASE ADMIN SDK
   Gives server trusted write access
   to Firestore — client can never
   fake ELO since server owns writes
════════════════════════════════ */
let db = null;
try {
  const admin = require('firebase-admin');
  if (!admin.apps.length) {
    // Service account JSON stored in Railway env var FIREBASE_SERVICE_ACCOUNT
    const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
      ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
      : null;

    if (serviceAccount) {
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: 'mogmetv',
      });
      db = admin.firestore();
      console.log('[Firebase] Admin SDK connected ✓');
    } else {
      console.warn('[Firebase] No service account — ELO will not persist to Firestore');
    }
  }
} catch(e) {
  console.warn('[Firebase] Admin SDK not available:', e.message);
}

/* ════════════════════════════════
   ADMIN KEY — from environment
════════════════════════════════ */
const ADMIN_KEY = process.env.ADMIN_KEY || 'mogmetv_admin_local_only';

/* ════════════════════════════════
   STATE
════════════════════════════════ */
const users         = new Map();
const chatHistory   = [];
const matchQueue    = [];
const activeMatches = new Map();
const privateRooms  = new Map();
const bannedUsers   = new Set();
const globalStats   = { totalUsers:0, totalMatches:0, onlineNow:0 };
const MAX_CHAT      = 100;
const CHAT_RESET_MS = 45 * 60 * 1000;
const CHAT_WARN_MS  = 40 * 60 * 1000;
const ELO_BAND      = 400; // Max ELO diff for instant match
const BAND_WIDEN_MS = 30000; // Widen band after 30s in queue

/* ════════════════════════════════
   TIER SYSTEM
════════════════════════════════ */
function getTier(elo) {
  if (elo >= 5001) return { name:'Slayer',   emoji:'💀' };
  if (elo >= 3501) return { name:'Chad',     emoji:'👑' };
  if (elo >= 2001) return { name:'Chadlite', emoji:'🔥' };
  if (elo >= 1501) return { name:'HTN',      emoji:'⭐' };
  if (elo >= 1001) return { name:'MTN',      emoji:'⚡' };
  if (elo >= 501)  return { name:'LTN',      emoji:'🌙' };
  if (elo >= 1)    return { name:'Sub3',     emoji:'🔴' };
  return                   { name:'Molecule',emoji:'🧪' };
}

function calcEloChange(won, opponentElo, myElo) {
  const K        = 32;
  const expected = 1 / (1 + Math.pow(10, (opponentElo - myElo) / 400));
  return Math.round(K * ((won ? 1 : 0) - expected));
}

/* ════════════════════════════════
   FETCH REAL ELO FROM FIRESTORE
   Called on set_user so server
   always has the real values
════════════════════════════════ */
async function fetchUserFromFirestore(uid) {
  if (!db || !uid) return null;
  try {
    const snap = await db.collection('users').doc(uid).get();
    return snap.exists ? snap.data() : null;
  } catch(e) {
    console.error('[Firestore] fetchUser error:', e.message);
    return null;
  }
}

/* ════════════════════════════════
   SAVE ELO TO FIRESTORE (server-authoritative)
   Only the server writes ELO —
   client browser cannot fake this
════════════════════════════════ */
async function saveEloToFirestore(uid, elo, wins, losses, matchEntry) {
  if (!db || !uid) return;
  try {
    const ref    = db.collection('users').doc(uid);
    const snap   = await ref.get();
    const hist   = snap.exists ? (snap.data().matchHistory || []) : [];
    if (matchEntry) hist.push(matchEntry);
    // Keep last 50 matches
    const trimmed = hist.slice(-50);
    await ref.update({
      elo, wins, losses,
      matchHistory: trimmed,
      lastSeen: new Date(),
    });
    console.log(`[Firestore] ELO saved: ${uid} → ${elo}`);
  } catch(e) {
    console.error('[Firestore] saveElo error:', e.message);
  }
}

/* ════════════════════════════════
   CHAT RESET — every 45 minutes
════════════════════════════════ */
function scheduleChatReset() {
  setTimeout(() => {
    const warn = {
      id:uuidv4(), userId:'system', name:'SYSTEM',
      text:'⚠️ Chat resets in 5 minutes. Save anything important!',
      tier:'System', tierEmoji:'⚠️', timestamp:Date.now(), isSystem:true,
    };
    chatHistory.push(warn);
    broadcast({ type:'chat', message:warn });
  }, CHAT_WARN_MS);

  setTimeout(() => {
    chatHistory.length = 0;
    const reset = {
      id:uuidv4(), userId:'system', name:'SYSTEM',
      text:'💬 Chat has been reset. Fresh start!',
      tier:'System', tierEmoji:'💬', timestamp:Date.now(), isSystem:true,
    };
    chatHistory.push(reset);
    broadcast({ type:'chat_reset', message:reset });
    scheduleChatReset();
  }, CHAT_RESET_MS);
}
scheduleChatReset();

/* ════════════════════════════════
   WEBSOCKET
════════════════════════════════ */
wss.on('connection', (ws) => {
  const socketId = uuidv4();
  const user = {
    id:socketId, ws,
    name:'Anonymous', username:'', uid:null, photoURL:'',
    elo:400, wins:0, losses:0,
    inQueue:false, inMatch:false, matchId:null,
    queuedAt:null,
    connectedAt:Date.now(),
  };

  users.set(socketId, user);
  globalStats.onlineNow = users.size;
  globalStats.totalUsers++;
  console.log(`[+] ${socketId} | Online: ${users.size}`);

  // Get display online count (respects admin override)
  send(ws, {
    type:'welcome', socketId,
    chatHistory: chatHistory.slice(-30),
    onlineCount: getDisplayOnline(),
    stats: globalStats,
  });
  broadcastOnlineCount();

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch(msg.type) {

      /* ── SET USER — fetch real ELO from Firestore ── */
      case 'set_user': {
        user.name     = (msg.name     || 'Anonymous').slice(0, 24);
        user.username = (msg.username || '').slice(0, 24);
        user.uid      = msg.uid || null;
        user.photoURL = msg.photoURL || '';

        if (user.uid) {
          // Fetch real stats from Firestore — never trust client values
          const fsData = await fetchUserFromFirestore(user.uid);
          if (fsData) {
            user.elo    = fsData.elo    || 400;
            user.wins   = fsData.wins   || 0;
            user.losses = fsData.losses || 0;
            user.name   = fsData.username || user.name;
            user.username = fsData.username || user.username;
            console.log(`[Auth] ${user.username} | ELO: ${user.elo} (from Firestore)`);
          }
        } else {
          // Guest — use client-provided values but cap them
          user.elo    = Math.min(Math.max(parseInt(msg.elo) || 400, 0), 10000);
          user.wins   = parseInt(msg.wins)   || 0;
          user.losses = parseInt(msg.losses) || 0;
        }

        // Check if banned
        if (user.username && bannedUsers.has(user.username.toLowerCase())) {
          send(ws, { type:'banned', message:'You have been banned from MogMe.TV.' });
          ws.close();
          return;
        }

        send(ws, { type:'user_updated', user:publicUser(user) });
        break;
      }

      /* ── CHAT — logged-in users only ── */
      case 'chat': {
        if (!user.uid || !user.username) {
          send(ws, { type:'chat_error', error:'Sign in to chat' });
          break;
        }
        if (bannedUsers.has(user.username.toLowerCase())) break;
        if (!msg.text || !msg.text.trim()) break;

        const tier = getTier(user.elo);
        const chatMsg = {
          id:uuidv4(), userId:socketId,
          name:user.username,
          photoURL:user.photoURL,
          text:msg.text.slice(0, 200).trim(),
          elo:user.elo,
          tier:tier.name, tierEmoji:tier.emoji,
          timestamp:Date.now(),
        };
        chatHistory.push(chatMsg);
        if (chatHistory.length > MAX_CHAT) chatHistory.shift();
        broadcast({ type:'chat', message:chatMsg });
        break;
      }

      /* ── JOIN QUEUE ── */
      case 'join_queue':
        if (user.inQueue || user.inMatch) break;
        user.inQueue  = true;
        user.queuedAt = Date.now();
        matchQueue.push(socketId);
        send(ws, { type:'queue_joined', position:matchQueue.length });
        broadcast({ type:'queue_update', size:matchQueue.length });
        tryMatch();
        break;

      /* ── LEAVE QUEUE ── */
      case 'leave_queue': {
        user.inQueue = false;
        const idx = matchQueue.indexOf(socketId);
        if (idx > -1) matchQueue.splice(idx, 1);
        send(ws, { type:'queue_left' });
        broadcast({ type:'queue_update', size:matchQueue.length });
        break;
      }

      /* ── SUBMIT SCORE — server validates ── */
      case 'submit_score': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;

        // Validate score range
        const rawScore = parseFloat(msg.score);
        if (isNaN(rawScore) || rawScore < 1 || rawScore > 10) {
          send(ws, { type:'score_error', error:'Invalid score' });
          break;
        }

        // Rate limiting — can't submit twice
        if (match.scores[socketId] !== undefined) break;

        match.scores[socketId] = rawScore;
        match.submitTimes      = match.submitTimes || {};
        match.submitTimes[socketId] = Date.now();

        if (Object.keys(match.scores).length === 2) {
          resolveMatch(match);
        } else {
          send(ws, { type:'score_submitted', waiting:true });
        }
        break;
      }

      /* ── SKIP ── */
      case 'skip_opponent': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        const oppId = match.players.find(id => id !== socketId);
        const opp   = users.get(oppId);
        if (opp) {
          send(opp.ws, { type:'opponent_skipped' });
          opp.inMatch = false; opp.matchId = null;
        }
        activeMatches.delete(user.matchId);
        user.inMatch = false; user.matchId = null;
        user.inQueue  = true; user.queuedAt = Date.now();
        matchQueue.push(socketId);
        send(ws, { type:'queue_joined', position:matchQueue.length });
        tryMatch();
        break;
      }

      /* ── WEBRTC SIGNALING ── */
      case 'webrtc_offer': {
        const t = users.get(msg.targetId);
        if (t) send(t.ws, { type:'webrtc_offer', offer:msg.offer, fromId:socketId });
        break;
      }
      case 'webrtc_answer': {
        const t = users.get(msg.targetId);
        if (t) send(t.ws, { type:'webrtc_answer', answer:msg.answer, fromId:socketId });
        break;
      }
      case 'webrtc_ice': {
        const t = users.get(msg.targetId);
        if (t) send(t.ws, { type:'webrtc_ice', candidate:msg.candidate, fromId:socketId });
        break;
      }

      /* ── PRIVATE ROOMS ── */
      case 'create_private_room': {
        const code = generateRoomCode();
        privateRooms.set(code, { code, host:socketId, guest:null, createdAt:Date.now() });
        setTimeout(() => {
          const r = privateRooms.get(code);
          if (r && !r.guest) privateRooms.delete(code);
        }, 10 * 60 * 1000);
        send(ws, { type:'room_created', code });
        break;
      }
      case 'join_private_room': {
        const code = (msg.code || '').toUpperCase().trim();
        const room  = privateRooms.get(code);
        if (!room)               { send(ws,{type:'room_error',error:'Invalid or expired code'}); break; }
        if (room.host===socketId){ send(ws,{type:'room_error',error:'Cannot join your own room'}); break; }
        if (room.guest)          { send(ws,{type:'room_error',error:'Room is full'}); break; }
        room.guest = socketId;
        const matchId = uuidv4();
        const match   = { id:matchId, players:[room.host,socketId], scores:{}, type:'private', startedAt:Date.now() };
        activeMatches.set(matchId, match);
        const host  = users.get(room.host);
        const guest = users.get(socketId);
        if (host)  { host.inMatch=true;  host.matchId=matchId;  send(host.ws,  {type:'private_match_start',matchId,opponent:publicUser(guest),role:'offerer'}); }
        if (guest) { guest.inMatch=true; guest.matchId=matchId; send(guest.ws, {type:'private_match_start',matchId,opponent:publicUser(host), role:'answerer'}); }
        privateRooms.delete(code);
        globalStats.totalMatches++;
        break;
      }

      /* ── REPORT USER ── */
      case 'report_user': {
        console.log(`[REPORT] ${user.username} reported ${msg.targetId}: ${msg.reason}`);
        send(ws, { type:'report_received' });
        break;
      }

      case 'ping':
        send(ws, { type:'pong', timestamp:Date.now() });
        break;
    }
  });

  ws.on('close', () => {
    console.log(`[-] ${socketId} | Online: ${users.size - 1}`);
    const qi = matchQueue.indexOf(socketId);
    if (qi > -1) matchQueue.splice(qi, 1);
    if (user.inMatch && user.matchId) {
      const match = activeMatches.get(user.matchId);
      if (match) {
        const oppId = match.players.find(id => id !== socketId);
        const opp   = users.get(oppId);
        if (opp) { send(opp.ws,{type:'opponent_disconnected'}); opp.inMatch=false; opp.matchId=null; }
        activeMatches.delete(user.matchId);
      }
    }
    users.delete(socketId);
    globalStats.onlineNow = users.size;
    broadcastOnlineCount();
    broadcast({ type:'queue_update', size:matchQueue.length });
  });

  ws.on('error', err => console.error(`WS error ${socketId}:`, err.message));
});

/* ════════════════════════════════
   MATCHMAKING — ELO-BAND
   Matches within 400 ELO first,
   widens band after 30s wait
════════════════════════════════ */
function tryMatch() {
  if (matchQueue.length < 2) return;

  for (let i = 0; i < matchQueue.length; i++) {
    const id1 = matchQueue[i];
    const u1  = users.get(id1);
    if (!u1 || u1.ws.readyState !== WebSocket.OPEN) { matchQueue.splice(i,1); i--; continue; }

    const waitMs  = Date.now() - (u1.queuedAt || Date.now());
    // Widen ELO band the longer they wait
    const band    = ELO_BAND + Math.floor(waitMs / BAND_WIDEN_MS) * 200;

    for (let j = i + 1; j < matchQueue.length; j++) {
      const id2 = matchQueue[j];
      const u2  = users.get(id2);
      if (!u2 || u2.ws.readyState !== WebSocket.OPEN) { matchQueue.splice(j,1); j--; continue; }

      const eloDiff = Math.abs(u1.elo - u2.elo);

      // Match if within band OR if either player has waited > 2 minutes
      const longWait = waitMs > 120000 || (Date.now() - (u2.queuedAt||Date.now())) > 120000;

      if (eloDiff <= band || longWait) {
        // Remove both from queue
        matchQueue.splice(j, 1);
        matchQueue.splice(i, 1);
        createMatch(id1, id2, u1, u2);
        // Recurse to match remaining
        setTimeout(tryMatch, 100);
        return;
      }
    }
  }
}

function createMatch(id1, id2, u1, u2) {
  const matchId = uuidv4();
  const match   = { id:matchId, players:[id1,id2], scores:{}, type:'ranked', startedAt:Date.now() };
  activeMatches.set(matchId, match);
  u1.inQueue=false; u1.inMatch=true; u1.matchId=matchId;
  u2.inQueue=false; u2.inMatch=true; u2.matchId=matchId;
  globalStats.totalMatches++;
  send(u1.ws, { type:'match_found', matchId, opponent:publicUser(u2), role:'offerer'  });
  send(u2.ws, { type:'match_found', matchId, opponent:publicUser(u1), role:'answerer' });
  console.log(`[MATCH] ${u1.name} (${u1.elo}) vs ${u2.name} (${u2.elo}) | diff:${Math.abs(u1.elo-u2.elo)}`);
}

/* ════════════════════════════════
   RESOLVE MATCH
   Server is authoritative for ELO
   Writes to Firestore via Admin SDK
════════════════════════════════ */
async function resolveMatch(match) {
  const [id1, id2] = match.players;
  const u1 = users.get(id1);
  const u2 = users.get(id2);
  const s1 = match.scores[id1] || 0;
  const s2 = match.scores[id2] || 0;

  // Detect conflict — both submitted same score
  const tied = Math.abs(s1 - s2) < 0.1;

  if (u1) {
    const won = tied ? false : s1 > s2;
    const chg = calcEloChange(won, u2 ? u2.elo : 400, u1.elo);
    u1.elo    = Math.max(0, u1.elo + chg);
    if (won) u1.wins++; else u1.losses++;
    u1.inMatch = false; u1.matchId = null;

    const matchEntry = {
      matchId:match.id, won, myScore:s1, oppScore:s2,
      opponentName:u2?.username||u2?.name||'Unknown',
      opponentElo:u2?.elo||400,
      eloChange:chg, newElo:u1.elo,
      date:new Date().toISOString(),
    };

    send(u1.ws, { type:'match_result', won, myScore:s1, opponentScore:s2, eloChange:chg, newElo:u1.elo, newTier:getTier(u1.elo) });

    // Server writes ELO to Firestore — browser cannot override
    if (u1.uid) {
      saveEloToFirestore(u1.uid, u1.elo, u1.wins, u1.losses, matchEntry);
    }
  }

  if (u2) {
    const won = tied ? false : s2 > s1;
    const chg = calcEloChange(won, u1 ? u1.elo : 400, u2.elo);
    u2.elo    = Math.max(0, u2.elo + chg);
    if (won) u2.wins++; else u2.losses++;
    u2.inMatch = false; u2.matchId = null;

    const matchEntry = {
      matchId:match.id, won, myScore:s2, oppScore:s1,
      opponentName:u1?.username||u1?.name||'Unknown',
      opponentElo:u1?.elo||400,
      eloChange:chg, newElo:u2.elo,
      date:new Date().toISOString(),
    };

    send(u2.ws, { type:'match_result', won, myScore:s2, opponentScore:s1, eloChange:chg, newElo:u2.elo, newTier:getTier(u2.elo) });

    if (u2.uid) {
      saveEloToFirestore(u2.uid, u2.elo, u2.wins, u2.losses, matchEntry);
    }
  }

  activeMatches.delete(match.id);
  console.log(`[RESULT] ${s1} vs ${s2}${tied?' (TIED)':''}`);
}

/* ════════════════════════════════
   REST ENDPOINTS
════════════════════════════════ */
app.get('/', (req,res) => res.json({ status:'MogMe.TV Server 🔱', online:users.size, matches:globalStats.totalMatches }));

app.get('/stats', (req,res) => res.json({
  ...globalStats,
  onlineNow:    getDisplayOnline(),
  realOnline:   users.size,
  queueSize:    matchQueue.length,
  activeMatches:activeMatches.size,
}));

/* ── ADMIN ENDPOINTS ── */
function adminAuth(req, res) {
  if (req.body?.adminKey !== ADMIN_KEY) {
    res.status(403).json({ error:'Unauthorized' });
    return false;
  }
  return true;
}

app.post('/admin/broadcast', (req,res) => {
  if (!adminAuth(req,res)) return;
  const message = req.body?.message;
  if (!message) { res.status(400).json({ error:'No message' }); return; }
  const sysMsg = {
    id:uuidv4(), userId:'system', name:'ADMIN',
    text:'📢 '+message,
    tier:'System', tierEmoji:'📢', timestamp:Date.now(), isSystem:true,
  };
  chatHistory.push(sysMsg);
  broadcast({ type:'chat', message:sysMsg });
  res.json({ ok:true });
});

app.post('/admin/reset-chat', (req,res) => {
  if (!adminAuth(req,res)) return;
  chatHistory.length = 0;
  const msg = {
    id:uuidv4(), userId:'system', name:'SYSTEM',
    text:'💬 Chat reset by admin.',
    tier:'System', tierEmoji:'💬', timestamp:Date.now(), isSystem:true,
  };
  chatHistory.push(msg);
  broadcast({ type:'chat_reset', message:msg });
  res.json({ ok:true });
});

app.post('/admin/ban', (req,res) => {
  if (!adminAuth(req,res)) return;
  const { username } = req.body;
  if (!username) { res.status(400).json({ error:'No username' }); return; }
  bannedUsers.add(username.toLowerCase());
  // Kick if currently connected
  users.forEach(u => {
    if (u.username?.toLowerCase() === username.toLowerCase()) {
      send(u.ws, { type:'banned', message:'You have been banned.' });
      u.ws.close();
    }
  });
  res.json({ ok:true });
});

app.post('/admin/unban', (req,res) => {
  if (!adminAuth(req,res)) return;
  bannedUsers.delete((req.body?.username||'').toLowerCase());
  res.json({ ok:true });
});

app.post('/admin/set-online-override', (req,res) => {
  if (!adminAuth(req,res)) return;
  const count = parseInt(req.body?.count);
  if (isNaN(count)) { res.status(400).json({ error:'Invalid count' }); return; }
  globalStats.onlineOverride = count >= 0 ? count : null;
  // Broadcast new count to all clients
  broadcastOnlineCount();
  res.json({ ok:true, override:globalStats.onlineOverride });
});

app.post('/admin/clear-override', (req,res) => {
  if (!adminAuth(req,res)) return;
  globalStats.onlineOverride = null;
  broadcastOnlineCount();
  res.json({ ok:true });
});

/* ════════════════════════════════
   HELPERS
════════════════════════════════ */
function getDisplayOnline() {
  return globalStats.onlineOverride != null ? globalStats.onlineOverride : users.size;
}

function broadcastOnlineCount() {
  broadcast({ type:'online_count', count:getDisplayOnline() });
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

function broadcast(data, excludeId=null) {
  const msg = JSON.stringify(data);
  users.forEach((u,id) => {
    if (id !== excludeId && u.ws.readyState === WebSocket.OPEN) u.ws.send(msg);
  });
}

function publicUser(u) {
  if (!u) return null;
  return { id:u.id, name:u.username||u.name, username:u.username, uid:u.uid, photoURL:u.photoURL, elo:u.elo, wins:u.wins, losses:u.losses, tier:getTier(u.elo) };
}

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  for (let i=0; i<6; i++) c += chars[Math.floor(Math.random()*chars.length)];
  return privateRooms.has(c) ? generateRoomCode() : c;
}

// Periodically clean dead players from queue
setInterval(() => {
  for (let i = matchQueue.length-1; i >= 0; i--) {
    const u = users.get(matchQueue[i]);
    if (!u || u.ws.readyState !== WebSocket.OPEN) matchQueue.splice(i,1);
  }
  tryMatch();
}, 10000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🔱 MogMe.TV Server on port ${PORT}`));
