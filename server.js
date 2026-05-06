const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const http = require('http');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const users        = new Map();  // socketId -> user
const chatHistory  = [];
const matchQueue   = [];
const activeMatches= new Map();
const privateRooms = new Map();
const globalStats  = { totalUsers:0, totalMatches:0, onlineNow:0 };

const MAX_CHAT_HISTORY = 100;
const CHAT_RESET_MS    = 45 * 60 * 1000; // 45 minutes
const CHAT_WARN_MS     = 40 * 60 * 1000; // warn at 40 minutes

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
  const K = 32;
  const expected = 1 / (1 + Math.pow(10, (opponentElo - myElo) / 400));
  return Math.round(K * ((won ? 1 : 0) - expected));
}

/* ════════════════════════════════
   CHAT RESET — every 45 minutes
════════════════════════════════ */
function scheduleChatReset() {
  // Warn at 40 minutes
  setTimeout(() => {
    const warnMsg = {
      id: uuidv4(), userId: 'system', name: 'SYSTEM',
      text: '⚠️ Chat resets in 5 minutes. Save anything important!',
      tier: 'System', tierEmoji: '⚠️', timestamp: Date.now(), isSystem: true,
    };
    chatHistory.push(warnMsg);
    broadcast({ type: 'chat', message: warnMsg });
    console.log('[CHAT] Reset warning sent');
  }, CHAT_WARN_MS);

  // Reset at 45 minutes
  setTimeout(() => {
    chatHistory.length = 0;
    const resetMsg = {
      id: uuidv4(), userId: 'system', name: 'SYSTEM',
      text: '💬 Chat has been reset. Fresh start!',
      tier: 'System', tierEmoji: '💬', timestamp: Date.now(), isSystem: true,
    };
    chatHistory.push(resetMsg);
    broadcast({ type: 'chat_reset', message: resetMsg });
    console.log('[CHAT] Reset executed');
    scheduleChatReset(); // Schedule next reset
  }, CHAT_RESET_MS);
}
scheduleChatReset();

/* ════════════════════════════════
   WEBSOCKET
════════════════════════════════ */
wss.on('connection', (ws) => {
  const socketId = uuidv4();
  const user = {
    id: socketId, ws,
    name: 'Anonymous',
    username: '',
    uid: null,        // Firebase UID
    photoURL: '',
    elo: 400,
    wins: 0, losses: 0,
    labScore: null,
    inQueue: false, inMatch: false, matchId: null,
    connectedAt: Date.now(),
  };

  users.set(socketId, user);
  globalStats.onlineNow = users.size;
  globalStats.totalUsers++;
  console.log(`[+] ${socketId} | Online: ${users.size}`);

  send(ws, {
    type: 'welcome', socketId,
    chatHistory: chatHistory.slice(-30),
    onlineCount: users.size,
    stats: globalStats,
  });
  broadcast({ type: 'online_count', count: users.size });

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    switch(msg.type) {

      /* ── SET USER (called after login) ── */
      case 'set_user': {
        user.name     = (msg.name     || 'Anonymous').slice(0, 24);
        user.username = (msg.username || '').slice(0, 24);
        user.uid      = msg.uid      || null;
        user.photoURL = msg.photoURL || '';
        // Load ELO from client (synced from Firestore)
        if (msg.elo !== undefined) user.elo = parseInt(msg.elo) || 400;
        if (msg.wins !== undefined) user.wins = parseInt(msg.wins) || 0;
        if (msg.losses !== undefined) user.losses = parseInt(msg.losses) || 0;
        send(ws, { type: 'user_updated', user: publicUser(user) });
        break;
      }

      /* ── CHAT ── */
      case 'chat': {
        // Only logged-in users with a username can chat
        if (!user.uid || !user.username) {
          send(ws, { type: 'chat_error', error: 'You must be signed in to chat' });
          break;
        }
        if (!msg.text || !msg.text.trim()) break;
        const tier = getTier(user.elo);
        const chatMsg = {
          id: uuidv4(), userId: socketId,
          name: user.username,
          photoURL: user.photoURL,
          text: msg.text.slice(0, 200).trim(),
          elo: user.elo,
          tier: tier.name, tierEmoji: tier.emoji,
          timestamp: Date.now(),
        };
        chatHistory.push(chatMsg);
        if (chatHistory.length > MAX_CHAT_HISTORY) chatHistory.shift();
        broadcast({ type: 'chat', message: chatMsg });
        break;
      }

      /* ── MATCHMAKING ── */
      case 'join_queue':
        if (user.inQueue || user.inMatch) break;
        user.inQueue = true;
        matchQueue.push(socketId);
        send(ws, { type: 'queue_joined', position: matchQueue.length });
        broadcast({ type: 'queue_update', size: matchQueue.length });
        tryMatch();
        break;

      case 'leave_queue': {
        user.inQueue = false;
        const idx = matchQueue.indexOf(socketId);
        if (idx > -1) matchQueue.splice(idx, 1);
        send(ws, { type: 'queue_left' });
        broadcast({ type: 'queue_update', size: matchQueue.length });
        break;
      }

      /* ── SUBMIT SCORE ── */
      case 'submit_score': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        match.scores[socketId] = parseFloat(msg.score) || 0;
        if (Object.keys(match.scores).length === 2) resolveMatch(match);
        else send(ws, { type: 'score_submitted', waiting: true });
        break;
      }

      /* ── SKIP ── */
      case 'skip_opponent': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        const oppId = match.players.find(id => id !== socketId);
        const opp   = users.get(oppId);
        if (opp) { send(opp.ws, { type:'opponent_skipped' }); opp.inMatch=false; opp.matchId=null; }
        activeMatches.delete(user.matchId);
        user.inMatch=false; user.matchId=null; user.inQueue=true;
        matchQueue.push(socketId);
        send(ws, { type:'queue_joined', position:matchQueue.length });
        tryMatch();
        break;
      }

      /* ── LAB SCORE ── */
      case 'save_lab_score':
        user.labScore = parseFloat(msg.score) || null;
        send(ws, { type:'lab_score_saved', score:user.labScore });
        break;

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
        setTimeout(() => { const r=privateRooms.get(code); if(r&&!r.guest) privateRooms.delete(code); }, 10*60*1000);
        send(ws, { type:'room_created', code });
        break;
      }
      case 'join_private_room': {
        const code = (msg.code||'').toUpperCase().trim();
        const room  = privateRooms.get(code);
        if (!room)                { send(ws,{type:'room_error',error:'Invalid or expired code'}); break; }
        if (room.host===socketId) { send(ws,{type:'room_error',error:'Cannot join your own room'}); break; }
        if (room.guest)           { send(ws,{type:'room_error',error:'Room is full'}); break; }
        room.guest = socketId;
        const matchId = uuidv4();
        const match = { id:matchId, players:[room.host,socketId], scores:{}, type:'private', startedAt:Date.now() };
        activeMatches.set(matchId, match);
        const host  = users.get(room.host);
        const guest = users.get(socketId);
        if (host)  { host.inMatch=true;  host.matchId=matchId;  send(host.ws,  {type:'private_match_start',matchId,opponent:publicUser(guest),role:'offerer'}); }
        if (guest) { guest.inMatch=true; guest.matchId=matchId; send(guest.ws, {type:'private_match_start',matchId,opponent:publicUser(host), role:'answerer'}); }
        privateRooms.delete(code);
        globalStats.totalMatches++;
        break;
      }

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
        const oppId = match.players.find(id => id !== socketId);
        const opp   = users.get(oppId);
        if (opp) { send(opp.ws,{type:'opponent_disconnected'}); opp.inMatch=false; opp.matchId=null; }
        activeMatches.delete(user.matchId);
      }
    }
    users.delete(socketId);
    globalStats.onlineNow = users.size;
    broadcast({ type:'online_count', count:users.size });
    broadcast({ type:'queue_update', size:matchQueue.length });
  });

  ws.on('error', err => console.error(`WS error ${socketId}:`, err.message));
});

/* ════════════════════════════════
   MATCHMAKING
════════════════════════════════ */
function tryMatch() {
  while (matchQueue.length >= 2) {
    const id1 = matchQueue.shift(), id2 = matchQueue.shift();
    const u1 = users.get(id1), u2 = users.get(id2);
    if (!u1||!u2) continue;
    if (u1.ws.readyState!==WebSocket.OPEN||u2.ws.readyState!==WebSocket.OPEN) continue;
    const matchId = uuidv4();
    const match = { id:matchId, players:[id1,id2], scores:{}, type:'ranked', startedAt:Date.now() };
    activeMatches.set(matchId, match);
    u1.inQueue=false; u1.inMatch=true; u1.matchId=matchId;
    u2.inQueue=false; u2.inMatch=true; u2.matchId=matchId;
    globalStats.totalMatches++;
    send(u1.ws, { type:'match_found', matchId, opponent:publicUser(u2), role:'offerer'  });
    send(u2.ws, { type:'match_found', matchId, opponent:publicUser(u1), role:'answerer' });
    console.log(`[MATCH] ${u1.name} vs ${u2.name}`);
  }
}

/* ════════════════════════════════
   RESOLVE MATCH + ELO
════════════════════════════════ */
function resolveMatch(match) {
  const [id1,id2] = match.players;
  const u1=users.get(id1), u2=users.get(id2);
  const s1=match.scores[id1]||0, s2=match.scores[id2]||0;
  const u1Won=s1>s2, u2Won=s2>s1;

  if (u1) {
    const chg = calcEloChange(u1Won, u2?u2.elo:400, u1.elo);
    u1.elo = Math.max(0, u1.elo+chg);
    if (u1Won) u1.wins++; else u1.losses++;
    u1.inMatch=false; u1.matchId=null;
    send(u1.ws, { type:'match_result', won:u1Won, myScore:s1, opponentScore:s2, eloChange:chg, newElo:u1.elo, newTier:getTier(u1.elo) });
  }
  if (u2) {
    const chg = calcEloChange(u2Won, u1?u1.elo:400, u2.elo);
    u2.elo = Math.max(0, u2.elo+chg);
    if (u2Won) u2.wins++; else u2.losses++;
    u2.inMatch=false; u2.matchId=null;
    send(u2.ws, { type:'match_result', won:u2Won, myScore:s2, opponentScore:s1, eloChange:chg, newElo:u2.elo, newTier:getTier(u2.elo) });
  }
  activeMatches.delete(match.id);
  console.log(`[RESULT] ${s1} vs ${s2} | ELO updated`);
}

/* ════════════════════════════════
   REST ENDPOINTS
════════════════════════════════ */
app.get('/', (req,res) => res.json({ status:'MogMe.TV Server 🔱', online:users.size, matches:globalStats.totalMatches }));
app.get('/stats', (req,res) => res.json({ ...globalStats, onlineNow:users.size, queueSize:matchQueue.length, activeMatches:activeMatches.size }));

/* ════════════════════════════════
   HELPERS
════════════════════════════════ */
function send(ws, data) {
  if (ws && ws.readyState===WebSocket.OPEN) ws.send(JSON.stringify(data));
}
function broadcast(data, excludeId=null) {
  const msg=JSON.stringify(data);
  users.forEach((u,id)=>{ if(id!==excludeId&&u.ws.readyState===WebSocket.OPEN) u.ws.send(msg); });
}
function publicUser(u) {
  if (!u) return null;
  return { id:u.id, name:u.username||u.name, username:u.username, uid:u.uid, photoURL:u.photoURL, elo:u.elo, wins:u.wins, losses:u.losses, tier:getTier(u.elo) };
}
function generateRoomCode() {
  const chars='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c=''; for(let i=0;i<6;i++) c+=chars[Math.floor(Math.random()*chars.length)];
  return privateRooms.has(c)?generateRoomCode():c;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🔱 MogMe.TV Server on port ${PORT}`));
