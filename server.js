const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors  = require('cors');
const http  = require('http');

const app = express();
app.use(cors());
app.use(express.json());

/* ════════════════════════════════
   RATE LIMITING — express-rate-limit
   Prevents API flood / cost spikes
════════════════════════════════ */
let rateLimit;
try {
  rateLimit = require('express-rate-limit');
  const limiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 60,             // 60 requests per minute per IP
    message: { error: 'Too many requests — slow down' },
    standardHeaders: true,
    legacyHeaders: false,
  });
  app.use('/admin', rateLimit({
    windowMs: 60 * 1000,
    max: 20, // stricter for admin endpoints
    message: { error: 'Rate limit exceeded' },
  }));
  app.use(limiter);
  console.log('[Rate Limit] Active ✓');
} catch(e) {
  console.warn('[Rate Limit] express-rate-limit not installed — skipping');
}

const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

/* ════════════════════════════════
   FIREBASE ADMIN SDK
   JWT verification + Firestore writes
   Server is the ONLY writer of ELO
════════════════════════════════ */
let db    = null;
let admin = null;

try {
  admin = require('firebase-admin');
  if (!admin.apps.length) {
    const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
      ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
      : null;

    if (serviceAccount) {
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: 'mogmetv',
      });
      db = admin.firestore();
      console.log('[Firebase] Admin SDK connected — JWT verification active ✓');
    } else {
      console.warn('[Firebase] No FIREBASE_SERVICE_ACCOUNT env var — JWT verification disabled');
    }
  }
} catch(e) {
  console.warn('[Firebase] Admin SDK unavailable:', e.message);
}

/* ════════════════════════════════
   JWT VERIFICATION
   Verifies Firebase ID token sent
   by client — prevents UID spoofing
════════════════════════════════ */
async function verifyToken(idToken) {
  if (!admin || !idToken) return null;
  try {
    const decoded = await admin.auth().verifyIdToken(idToken);
    return decoded; // { uid, email, ... }
  } catch(e) {
    console.warn('[JWT] Verification failed:', e.message);
    return null;
  }
}

/* ════════════════════════════════
   CONSTANTS
════════════════════════════════ */
const ADMIN_KEY       = process.env.ADMIN_KEY || 'mogmetv_admin_local_only';
const MAX_CHAT        = 100;
const CHAT_RESET_MS   = 45 * 60 * 1000;
const CHAT_WARN_MS    = 40 * 60 * 1000;
const CHAT_COOLDOWN   = 1500;   // ms between chat messages per user
const ELO_BAND        = 400;    // initial ELO match band
const BAND_WIDEN_MS   = 30000;  // widen band every 30s
const WEBRTC_TIMEOUT  = 15000;  // 15s WebRTC connection timeout
const HEARTBEAT_MS    = 5000;   // queue heartbeat check
const SCORE_MIN       = 1.0;
const SCORE_MAX       = 10.0;

/* ════════════════════════════════
   TIER SYSTEM
════════════════════════════════ */
const TIERS = [
  { name:'Slayer',   emoji:'💀', min:5001, max:Infinity },
  { name:'Chad',     emoji:'👑', min:3501, max:5000 },
  { name:'Chadlite', emoji:'🔥', min:2001, max:3500 },
  { name:'HTN',      emoji:'⭐', min:1501, max:2000 },
  { name:'MTN',      emoji:'⚡', min:1001, max:1500 },
  { name:'LTN',      emoji:'🌙', min:501,  max:1000 },
  { name:'Sub3',     emoji:'🔴', min:1,    max:500  },
  { name:'Molecule', emoji:'🧪', min:-Infinity, max:0 },
];

function getTier(elo) {
  return TIERS.find(t => elo >= t.min && elo <= t.max) || TIERS[TIERS.length-1];
}

/* ── Dynamic ELO progress (accurate tier floor/ceiling) ── */
function calcEloProgress(elo) {
  const tier = getTier(elo);
  const tierIdx = TIERS.findIndex(t => t.name === tier.name);
  const nextTier = tierIdx > 0 ? TIERS[tierIdx - 1] : null;
  const floor = tier.min === -Infinity ? 0 : tier.min;
  const ceiling = nextTier ? nextTier.min : tier.max;
  const range = ceiling - floor;
  const progress = range > 0 ? Math.min(100, Math.max(0, ((elo - floor) / range) * 100)) : 100;
  return {
    tier, nextTier,
    floor, ceiling,
    progress: Math.round(progress),
    eloNeeded: nextTier ? Math.max(0, nextTier.min - elo) : 0,
  };
}

function calcEloChange(won, opponentElo, myElo) {
  const K = 32;
  const expected = 1 / (1 + Math.pow(10, (opponentElo - myElo) / 400));
  return Math.round(K * ((won ? 1 : 0) - expected));
}

/* ════════════════════════════════
   STATE
════════════════════════════════ */
const users         = new Map();
const chatHistory   = [];
const matchQueue    = [];
const activeMatches = new Map();
const privateRooms  = new Map();
const bannedUsers   = new Set();
const globalStats   = { totalUsers:0, totalMatches:0, onlineNow:0, onlineOverride:null };

/* ════════════════════════════════
   FIRESTORE HELPERS
════════════════════════════════ */
async function fetchUserFromFirestore(uid) {
  if (!db || !uid) return null;
  try {
    const snap = await db.collection('users').doc(uid).get();
    return snap.exists ? snap.data() : null;
  } catch(e) {
    console.error('[Firestore] fetchUser:', e.message);
    return null;
  }
}

async function saveEloToFirestore(uid, elo, wins, losses, matchEntry, peakElo) {
  if (!db || !uid) return;
  try {
    const ref  = db.collection('users').doc(uid);
    const snap = await ref.get();
    const hist = snap.exists ? (snap.data().matchHistory || []) : [];
    const existingPeak = snap.exists ? (snap.data().peakElo||0) : 0;
    if (matchEntry) hist.push(matchEntry);
    const update = {
      elo, wins, losses,
      matchHistory: hist.slice(-50),
      lastSeen: new Date(),
    };
    if (peakElo && peakElo > existingPeak) update.peakElo = peakElo;
    if (matchEntry?.winStreak !== undefined) update.winStreak = matchEntry.winStreak;
    await ref.update(update);
    console.log(`[Firestore] ELO saved: ${uid} → ${elo}`);
  } catch(e) {
    console.error('[Firestore] saveElo:', e.message);
  }
}

/* ════════════════════════════════
   CHAT RESET — every 45 minutes
════════════════════════════════ */
function scheduleChatReset() {
  setTimeout(() => {
    const warn = makeSysMsg('⚠️ Chat resets in 5 minutes.');
    chatHistory.push(warn);
    broadcast({ type:'chat', message:warn });
  }, CHAT_WARN_MS);

  setTimeout(() => {
    chatHistory.length = 0;
    const reset = makeSysMsg('💬 Chat has been reset. Fresh start!');
    chatHistory.push(reset);
    broadcast({ type:'chat_reset', message:reset });
    scheduleChatReset();
  }, CHAT_RESET_MS);
}
scheduleChatReset();

function makeSysMsg(text) {
  return { id:uuidv4(), userId:'system', name:'SYSTEM', text, tier:'System', tierEmoji:'⚙️', timestamp:Date.now(), isSystem:true };
}

/* ════════════════════════════════
   MATCHMAKING HEARTBEAT
   Removes dead sockets from queue
   every 5 seconds
════════════════════════════════ */
setInterval(() => {
  let changed = false;
  for (let i = matchQueue.length - 1; i >= 0; i--) {
    const u = users.get(matchQueue[i]);
    if (!u || u.ws.readyState !== WebSocket.OPEN) {
      matchQueue.splice(i, 1);
      changed = true;
    }
  }
  if (changed) {
    broadcast({ type:'queue_update', size:matchQueue.length });
    tryMatch();
  }
}, HEARTBEAT_MS);

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
    queuedAt:null, lastChat:0,
    verified:false, // true once JWT verified
    connectedAt:Date.now(),
  };

  users.set(socketId, user);
  globalStats.onlineNow = users.size;
  globalStats.totalUsers++;
  console.log(`[+] ${socketId} | Online: ${users.size}`);

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

      /* ── SET USER — verify JWT token ── */
      case 'set_user': {
        user.name     = (msg.name     || 'Anonymous').slice(0, 24);
        user.username = (msg.username || '').slice(0, 24);
        user.photoURL = msg.photoURL || '';

        if (msg.idToken && admin) {
          // ── VERIFIED PATH: client sends Firebase ID token ──
          const decoded = await verifyToken(msg.idToken);
          if (decoded) {
            user.uid      = decoded.uid;
            user.verified = true;
            console.log(`[JWT] Verified: ${decoded.email}`);

            // Fetch real stats from Firestore — client cannot fake these
            const fsData = await fetchUserFromFirestore(decoded.uid);
            if (fsData) {
              user.elo      = fsData.elo      || 400;
              user.wins     = fsData.wins     || 0;
              user.losses   = fsData.losses   || 0;
              user.username = fsData.username || user.username;
              user.name     = fsData.username || user.name;
            }
          } else {
            // Token invalid — treat as guest
            user.uid      = null;
            user.verified = false;
          }
        } else if (msg.uid && !admin) {
          // ── UNVERIFIED PATH (no Admin SDK): trust uid from client ──
          // This is less secure but works when Admin SDK not configured
          user.uid = msg.uid;
          user.elo = Math.min(Math.max(parseInt(msg.elo)||400, 0), 10000);
          user.wins   = parseInt(msg.wins)  || 0;
          user.losses = parseInt(msg.losses)|| 0;
        }

        // Check ban
        if (user.username && bannedUsers.has(user.username.toLowerCase())) {
          send(ws, { type:'banned', message:'You have been banned from MogMe.TV.' });
          ws.close(); return;
        }

        const prog = calcEloProgress(user.elo);
        send(ws, {
          type:'user_updated',
          user: publicUser(user),
          progress: prog,
        });
        break;
      }

      /* ── CHAT — rate limited, logged-in only ── */
      case 'chat': {
        if (!user.uid || !user.username) {
          send(ws, { type:'chat_error', error:'Sign in to chat' });
          break;
        }
        if (bannedUsers.has(user.username.toLowerCase())) break;
        if (!msg.text || !msg.text.trim()) break;

        // Per-socket chat cooldown (1.5s)
        const now = Date.now();
        if (now - user.lastChat < CHAT_COOLDOWN) {
          send(ws, { type:'chat_error', error:'Slow down — cooldown active' });
          break;
        }
        user.lastChat = now;

        const tier = getTier(user.elo);
        const chatMsg = {
          id:uuidv4(), userId:socketId,
          name:user.username, photoURL:user.photoURL,
          text:msg.text.slice(0, 200).trim(),
          elo:user.elo, tier:tier.name, tierEmoji:tier.emoji,
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

      /* ── SUBMIT SCORE — server validates strictly ── */
      case 'submit_score': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;

        const raw = parseFloat(msg.score);
        if (isNaN(raw) || raw < SCORE_MIN || raw > SCORE_MAX) {
          send(ws, { type:'score_error', error:'Invalid score — must be 1–10' });
          break;
        }
        if (match.scores[socketId] !== undefined) break; // no double submit

        match.scores[socketId] = raw;
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
        if (opp) { send(opp.ws,{type:'opponent_skipped'}); opp.inMatch=false; opp.matchId=null; }
        if (match.rtcTimeout) clearTimeout(match.rtcTimeout);
        activeMatches.delete(user.matchId);
        user.inMatch=false; user.matchId=null;
        user.inQueue=true; user.queuedAt=Date.now();
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

      /* ── WEBRTC CONNECTED — cancel timeout ── */
      case 'webrtc_connected': {
        if (user.matchId) {
          const match = activeMatches.get(user.matchId);
          if (match && match.rtcTimeout) {
            clearTimeout(match.rtcTimeout);
            match.rtcTimeout = null;
            match.rtcConnected = true;
          }
        }
        break;
      }

      /* ── PRIVATE ROOMS ── */
      case 'create_private_room': {
        const code = generateRoomCode();
        privateRooms.set(code, { code, host:socketId, guest:null, createdAt:Date.now() });
        setTimeout(() => { const r=privateRooms.get(code); if(r&&!r.guest) privateRooms.delete(code); }, 10*60*1000);
        send(ws, { type:'room_created', code });
        break;
      }
      case 'join_private_room': {
        const code = (msg.code||'').toUpperCase().trim();
        const room  = privateRooms.get(code);
        if (!room)               { send(ws,{type:'room_error',error:'Invalid or expired code'}); break; }
        if (room.host===socketId){ send(ws,{type:'room_error',error:'Cannot join your own room'}); break; }
        if (room.guest)          { send(ws,{type:'room_error',error:'Room is full'}); break; }
        room.guest = socketId;
        const matchId = uuidv4();
        const match   = { id:matchId, players:[room.host,socketId], scores:{}, type:'private', startedAt:Date.now(), rtcTimeout:null };
        activeMatches.set(matchId, match);
        const host  = users.get(room.host);
        const guest = users.get(socketId);
        if (host)  { host.inMatch=true;  host.matchId=matchId;  send(host.ws,  {type:'private_match_start',matchId,opponent:publicUser(guest),role:'offerer'}); }
        if (guest) { guest.inMatch=true; guest.matchId=matchId; send(guest.ws, {type:'private_match_start',matchId,opponent:publicUser(host), role:'answerer'}); }
        privateRooms.delete(code);
        globalStats.totalMatches++;
        startRtcTimeout(match);
        break;
      }

      case 'request_rematch': {
        const rematchOpp = msg.targetId ? users.get(msg.targetId) : null;
        if (!rematchOpp || !user.lastMatchId) break;
        user.wantsRematch = true;
        if (rematchOpp.wantsRematch && rematchOpp.lastOpponentId === socketId) {
          user.wantsRematch = false; rematchOpp.wantsRematch = false;
          send(user.ws,     { type:'rematch_ready' });
          send(rematchOpp.ws, { type:'rematch_ready' });
          setTimeout(() => {
            if (user.ws.readyState===WebSocket.OPEN && rematchOpp.ws.readyState===WebSocket.OPEN
              && !user.inMatch && !rematchOpp.inMatch) {
              createMatch(socketId, msg.targetId, user, rematchOpp);
            }
          }, 800);
        } else {
          send(rematchOpp.ws, { type:'opponent_wants_rematch' });
        }
        break;
      }

      case 'report_user':
        console.log(`[REPORT] ${user.username} → ${msg.targetId}: ${msg.reason}`);
        send(ws, { type:'report_received' });
        break;

      case 'ping':
        send(ws, { type:'pong', timestamp:Date.now() });
        break;
    }
  });

  ws.on('close', () => {
    console.log(`[-] ${socketId} | Online: ${users.size-1}`);
    const qi = matchQueue.indexOf(socketId);
    if (qi > -1) matchQueue.splice(qi, 1);
    if (user.inMatch && user.matchId) {
      const match = activeMatches.get(user.matchId);
      if (match) {
        if (match.rtcTimeout) clearTimeout(match.rtcTimeout);
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
   WEBRTC TIMEOUT — 15 seconds
   If cameras don't connect, cancel
   match and return users to queue
════════════════════════════════ */
function startRtcTimeout(match) {
  match.rtcTimeout = setTimeout(() => {
    if (match.rtcConnected) return; // already connected — no action
    console.log(`[RTC Timeout] Match ${match.id} — returning to queue`);
    match.players.forEach(id => {
      const u = users.get(id);
      if (!u) return;
      send(u.ws, { type:'rtc_timeout', message:'Camera connection timed out — returning to queue' });
      u.inMatch = false; u.matchId = null;
      if (u.inQueue) return;
      u.inQueue = true; u.queuedAt = Date.now();
      matchQueue.push(id);
      send(u.ws, { type:'queue_joined', position:matchQueue.length });
    });
    activeMatches.delete(match.id);
    broadcast({ type:'queue_update', size:matchQueue.length });
    tryMatch();
  }, WEBRTC_TIMEOUT);
}

/* ════════════════════════════════
   ELO-BAND MATCHMAKING
════════════════════════════════ */
function tryMatch() {
  if (matchQueue.length < 2) return;

  for (let i = 0; i < matchQueue.length; i++) {
    const id1 = matchQueue[i];
    const u1  = users.get(id1);
    if (!u1 || u1.ws.readyState !== WebSocket.OPEN) { matchQueue.splice(i,1); i--; continue; }

    const waitMs = Date.now() - (u1.queuedAt || Date.now());
    const band   = ELO_BAND + Math.floor(waitMs / BAND_WIDEN_MS) * 200;

    for (let j = i+1; j < matchQueue.length; j++) {
      const id2 = matchQueue[j];
      const u2  = users.get(id2);
      if (!u2 || u2.ws.readyState !== WebSocket.OPEN) { matchQueue.splice(j,1); j--; continue; }

      const eloDiff  = Math.abs(u1.elo - u2.elo);
      const longWait = waitMs > 120000 || (Date.now()-(u2.queuedAt||Date.now())) > 120000;

      if (eloDiff <= band || longWait) {
        matchQueue.splice(j,1); matchQueue.splice(i,1);
        createMatch(id1, id2, u1, u2);
        setTimeout(tryMatch, 100);
        return;
      }
    }
  }
}

function createMatch(id1, id2, u1, u2) {
  const matchId = uuidv4();
  const match   = { id:matchId, players:[id1,id2], scores:{}, type:'ranked', startedAt:Date.now(), rtcTimeout:null, rtcConnected:false };
  activeMatches.set(matchId, match);
  u1.inQueue=false; u1.inMatch=true; u1.matchId=matchId;
  u2.inQueue=false; u2.inMatch=true; u2.matchId=matchId;
  globalStats.totalMatches++;

  const p1prog = calcEloProgress(u1.elo);
  const p2prog = calcEloProgress(u2.elo);

  send(u1.ws, { type:'match_found', matchId, opponent:publicUser(u2), role:'offerer',  progress:p1prog });
  send(u2.ws, { type:'match_found', matchId, opponent:publicUser(u1), role:'answerer', progress:p2prog });
  console.log(`[MATCH] ${u1.name}(${u1.elo}) vs ${u2.name}(${u2.elo}) | diff:${Math.abs(u1.elo-u2.elo)}`);

  // Start 15-second WebRTC connection timeout
  startRtcTimeout(match);
}

/* ════════════════════════════════
   RESOLVE MATCH — server owns ELO
   Streak multiplier: 3W=1.2x, 5W=1.35x, 7W+=1.5x
════════════════════════════════ */
async function resolveMatch(match) {
  const [id1,id2] = match.players;
  const u1=users.get(id1), u2=users.get(id2);
  const s1=match.scores[id1]||0, s2=match.scores[id2]||0;
  if (match.rtcTimeout) clearTimeout(match.rtcTimeout);

  async function settle(u, myScore, oppScore, opp) {
    if (!u) return;
    const won = myScore > oppScore;

    // Streak tracking
    if (won) { u.winStreak = (u.winStreak||0) + 1; u.lossStreak = 0; }
    else      { u.lossStreak = (u.lossStreak||0) + 1; u.winStreak = 0; }

    // Streak multiplier on ELO gains (losses unaffected)
    let chg = calcEloChange(won, opp?opp.elo:400, u.elo);
    if (won && u.winStreak >= 7) chg = Math.round(chg * 1.5);
    else if (won && u.winStreak >= 5) chg = Math.round(chg * 1.35);
    else if (won && u.winStreak >= 3) chg = Math.round(chg * 1.2);

    const prevElo = u.elo;
    u.elo = Math.max(0, u.elo + chg);
    if (u.elo > (u.peakElo||0)) u.peakElo = u.elo;
    if (won) u.wins++; else u.losses++;
    u.inMatch=false; u.matchId=null;
    // Store for rematch matching
    u.lastMatchId = match.id;
    u.lastOpponentId = opp?.id||null;

    const prog = calcEloProgress(u.elo);
    send(u.ws, {
      type:'match_result', won, myScore, opponentScore:oppScore,
      eloChange:chg, newElo:u.elo, newTier:getTier(u.elo), progress:prog,
      winStreak: u.winStreak, peakElo: u.peakElo||u.elo,
    });

    if (u.uid) {
      const entry = {
        matchId:match.id, won, myScore, oppScore,
        opponentName:opp?.username||opp?.name||'Unknown',
        opponentElo:opp?.elo||400, eloChange:chg, newElo:u.elo,
        winStreak:u.winStreak, date:new Date().toISOString(),
      };
      saveEloToFirestore(u.uid, u.elo, u.wins, u.losses, entry, u.peakElo);
    }
  }

  await Promise.all([settle(u1,s1,s2,u2), settle(u2,s2,s1,u1)]);
  activeMatches.delete(match.id);
  console.log(`[RESULT] ${s1.toFixed(1)} vs ${s2.toFixed(1)}`);
}

/* ════════════════════════════════
   REST ENDPOINTS
════════════════════════════════ */
app.get('/', (req,res) => res.json({ status:'MogMe.TV 🔱', online:getDisplayOnline(), matches:globalStats.totalMatches }));
app.get('/stats', (req,res) => res.json({ ...globalStats, onlineNow:getDisplayOnline(), realOnline:users.size, queueSize:matchQueue.length, activeMatches:activeMatches.size }));

// ELO progress endpoint — for frontend to get accurate bar %
app.get('/elo-progress/:elo', (req,res) => {
  const elo = parseInt(req.params.elo);
  if (isNaN(elo)) { res.status(400).json({ error:'Invalid ELO' }); return; }
  res.json(calcEloProgress(elo));
});

/* ── ADMIN ── */
function adminAuth(req,res) {
  if (req.body?.adminKey !== ADMIN_KEY) { res.status(403).json({error:'Unauthorized'}); return false; }
  return true;
}

app.post('/admin/broadcast', (req,res) => {
  if (!adminAuth(req,res)) return;
  const message = req.body?.message;
  if (!message) { res.status(400).json({error:'No message'}); return; }
  const m = makeSysMsg('📢 '+message);
  chatHistory.push(m);
  broadcast({ type:'chat', message:m });
  res.json({ ok:true });
});

app.post('/admin/reset-chat', (req,res) => {
  if (!adminAuth(req,res)) return;
  chatHistory.length = 0;
  const m = makeSysMsg('💬 Chat reset by admin.');
  chatHistory.push(m);
  broadcast({ type:'chat_reset', message:m });
  res.json({ ok:true });
});

app.post('/admin/ban', (req,res) => {
  if (!adminAuth(req,res)) return;
  const { username } = req.body;
  if (!username) { res.status(400).json({error:'No username'}); return; }
  bannedUsers.add(username.toLowerCase());
  users.forEach(u => {
    if (u.username?.toLowerCase()===username.toLowerCase()) {
      send(u.ws,{type:'banned',message:'You have been banned.'});
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
  globalStats.onlineOverride = isNaN(count) || count < 0 ? null : count;
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
  if (ws && ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(data, excludeId=null) {
  const msg = JSON.stringify(data);
  users.forEach((u,id) => { if (id!==excludeId && u.ws.readyState===WebSocket.OPEN) u.ws.send(msg); });
}
function publicUser(u) {
  if (!u) return null;
  return { id:u.id, name:u.username||u.name, username:u.username, uid:u.uid, photoURL:u.photoURL, elo:u.elo, wins:u.wins, losses:u.losses, tier:getTier(u.elo), verified:u.verified };
}
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c = '';
  for (let i=0;i<6;i++) c+=chars[Math.floor(Math.random()*chars.length)];
  return privateRooms.has(c) ? generateRoomCode() : c;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🔱 MogMe.TV on port ${PORT}`));
