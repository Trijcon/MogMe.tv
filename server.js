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

const users = new Map();
const chatHistory = [];
const matchQueue = [];
const activeMatches = new Map();
const privateRooms = new Map();
const globalStats = { totalUsers: 0, totalMatches: 0, onlineNow: 0 };
const MAX_CHAT_HISTORY = 50;

function getTier(elo) {
  if (elo >= 3000) return { name: 'Chad',     emoji: '🔱', color: '#c9a84c' };
  if (elo >= 2000) return { name: 'Chadlite', emoji: '👑', color: '#9b7de0' };
  if (elo >= 1500) return { name: 'HTN',      emoji: '💎', color: '#00e5ff' };
  if (elo >= 1000) return { name: 'MTN',      emoji: '⚡', color: '#60c890' };
  if (elo >= 500)  return { name: 'LTN',      emoji: '🌙', color: '#7090d0' };
  return                   { name: 'Sub3',     emoji: '🔴', color: '#ff4d6d' };
}

function calcEloChange(won, opponentElo, myElo) {
  const K = 32;
  const expected = 1 / (1 + Math.pow(10, (opponentElo - myElo) / 400));
  return Math.round(K * ((won ? 1 : 0) - expected));
}

wss.on('connection', (ws) => {
  const socketId = uuidv4();
  const user = {
    id: socketId, ws, name: 'Anonymous', elo: 400,
    wins: 0, losses: 0, labScore: null,
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
    onlineCount: users.size, stats: globalStats,
  });
  broadcast({ type: 'online_count', count: users.size });

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'set_user':
        user.name = (msg.name || 'Anonymous').slice(0, 20);
        send(ws, { type: 'user_updated', user: publicUser(user) });
        break;

      case 'chat': {
        if (!msg.text || !msg.text.trim()) break;
        const tier = getTier(user.elo);
        const chatMsg = {
          id: uuidv4(), userId: socketId,
          name: user.name, text: msg.text.slice(0, 200).trim(),
          elo: user.elo, tier: tier.name,
          tierEmoji: tier.emoji, tierColor: tier.color,
          timestamp: Date.now(),
        };
        chatHistory.push(chatMsg);
        if (chatHistory.length > MAX_CHAT_HISTORY) chatHistory.shift();
        broadcast({ type: 'chat', message: chatMsg });
        break;
      }

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

      case 'submit_score': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        match.scores[socketId] = parseFloat(msg.score) || 0;
        if (Object.keys(match.scores).length === 2) resolveMatch(match);
        else send(ws, { type: 'score_submitted', waiting: true });
        break;
      }

      case 'skip_opponent': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        const oppId = match.players.find(id => id !== socketId);
        const opp = users.get(oppId);
        if (opp) {
          send(opp.ws, { type: 'opponent_skipped' });
          opp.inMatch = false; opp.matchId = null;
        }
        activeMatches.delete(user.matchId);
        user.inMatch = false; user.matchId = null;
        user.inQueue = true;
        matchQueue.push(socketId);
        send(ws, { type: 'queue_joined', position: matchQueue.length });
        tryMatch();
        break;
      }

      case 'save_lab_score':
        user.labScore = parseFloat(msg.score) || null;
        send(ws, { type: 'lab_score_saved', score: user.labScore });
        break;

      /* ══════════════════════════════
         WEBRTC SIGNALING
         Server only relays — never
         touches the actual video data
      ══════════════════════════════ */
      case 'webrtc_offer': {
        const target = users.get(msg.targetId);
        if (target) send(target.ws, { type: 'webrtc_offer', offer: msg.offer, fromId: socketId });
        break;
      }

      case 'webrtc_answer': {
        const target = users.get(msg.targetId);
        if (target) send(target.ws, { type: 'webrtc_answer', answer: msg.answer, fromId: socketId });
        break;
      }

      case 'webrtc_ice': {
        const target = users.get(msg.targetId);
        if (target) send(target.ws, { type: 'webrtc_ice', candidate: msg.candidate, fromId: socketId });
        break;
      }

      /* ══════════════════════════════
         PRIVATE ROOMS
      ══════════════════════════════ */
      case 'create_private_room': {
        const code = generateRoomCode();
        privateRooms.set(code, { code, host: socketId, guest: null, createdAt: Date.now() });
        setTimeout(() => { const r = privateRooms.get(code); if (r && !r.guest) privateRooms.delete(code); }, 10 * 60 * 1000);
        send(ws, { type: 'room_created', code });
        break;
      }

      case 'join_private_room': {
        const code = (msg.code || '').toUpperCase().trim();
        const room = privateRooms.get(code);
        if (!room)               { send(ws, { type: 'room_error', error: 'Invalid or expired code' }); break; }
        if (room.host===socketId){ send(ws, { type: 'room_error', error: 'You cannot join your own room' }); break; }
        if (room.guest)          { send(ws, { type: 'room_error', error: 'Room is already full' }); break; }
        room.guest = socketId;
        const matchId = uuidv4();
        const match = { id: matchId, players: [room.host, socketId], scores: {}, type: 'private', startedAt: Date.now() };
        activeMatches.set(matchId, match);
        const host  = users.get(room.host);
        const guest = users.get(socketId);
        if (host)  { host.inMatch  = true; host.matchId  = matchId; send(host.ws,  { type: 'private_match_start', matchId, opponent: publicUser(guest), role: 'offerer'  }); }
        if (guest) { guest.inMatch = true; guest.matchId = matchId; send(guest.ws, { type: 'private_match_start', matchId, opponent: publicUser(host),  role: 'answerer' }); }
        privateRooms.delete(code);
        globalStats.totalMatches++;
        break;
      }

      case 'ping':
        send(ws, { type: 'pong', timestamp: Date.now() });
        break;
    }
  });

  ws.on('close', () => {
    console.log(`[-] ${socketId} | Online: ${users.size - 1}`);
    const qIdx = matchQueue.indexOf(socketId);
    if (qIdx > -1) matchQueue.splice(qIdx, 1);
    if (user.inMatch && user.matchId) {
      const match = activeMatches.get(user.matchId);
      if (match) {
        const oppId = match.players.find(id => id !== socketId);
        const opp = users.get(oppId);
        if (opp) { send(opp.ws, { type: 'opponent_disconnected' }); opp.inMatch = false; opp.matchId = null; }
        activeMatches.delete(user.matchId);
      }
    }
    users.delete(socketId);
    globalStats.onlineNow = users.size;
    broadcast({ type: 'online_count', count: users.size });
    broadcast({ type: 'queue_update', size: matchQueue.length });
  });

  ws.on('error', (err) => console.error(`WS error ${socketId}:`, err.message));
});

/* ════════════════════════════════
   MATCHMAKING
════════════════════════════════ */
function tryMatch() {
  while (matchQueue.length >= 2) {
    const id1 = matchQueue.shift();
    const id2 = matchQueue.shift();
    const u1 = users.get(id1);
    const u2 = users.get(id2);
    if (!u1 || !u2) continue;
    if (u1.ws.readyState !== WebSocket.OPEN || u2.ws.readyState !== WebSocket.OPEN) continue;

    const matchId = uuidv4();
    const match = { id: matchId, players: [id1, id2], scores: {}, type: 'ranked', startedAt: Date.now() };
    activeMatches.set(matchId, match);
    u1.inQueue = false; u1.inMatch = true; u1.matchId = matchId;
    u2.inQueue = false; u2.inMatch = true; u2.matchId = matchId;
    globalStats.totalMatches++;

    // u1 = offerer (initiates WebRTC), u2 = answerer
    send(u1.ws, { type: 'match_found', matchId, opponent: publicUser(u2), role: 'offerer'  });
    send(u2.ws, { type: 'match_found', matchId, opponent: publicUser(u1), role: 'answerer' });
    console.log(`[MATCH] ${u1.name} vs ${u2.name} | ${matchId}`);
  }
}

/* ════════════════════════════════
   RESOLVE MATCH
════════════════════════════════ */
function resolveMatch(match) {
  const [id1, id2] = match.players;
  const u1 = users.get(id1);
  const u2 = users.get(id2);
  const s1 = match.scores[id1] || 0;
  const s2 = match.scores[id2] || 0;

  if (u1) {
    const chg = calcEloChange(s1>s2, u2?u2.elo:400, u1.elo);
    u1.elo = Math.max(0, u1.elo + chg);
    if (s1>s2) u1.wins++; else u1.losses++;
    u1.inMatch = false; u1.matchId = null;
    send(u1.ws, { type:'match_result', won:s1>s2, myScore:s1, opponentScore:s2, eloChange:chg, newElo:u1.elo, newTier:getTier(u1.elo) });
  }
  if (u2) {
    const chg = calcEloChange(s2>s1, u1?u1.elo:400, u2.elo);
    u2.elo = Math.max(0, u2.elo + chg);
    if (s2>s1) u2.wins++; else u2.losses++;
    u2.inMatch = false; u2.matchId = null;
    send(u2.ws, { type:'match_result', won:s2>s1, myScore:s2, opponentScore:s1, eloChange:chg, newElo:u2.elo, newTier:getTier(u2.elo) });
  }
  activeMatches.delete(match.id);
  console.log(`[RESULT] ${s1} vs ${s2}`);
}

/* ════════════════════════════════
   REST
════════════════════════════════ */
app.get('/', (req, res) => res.json({ status: 'OMOGGLE Server 🔱', online: users.size, matches: globalStats.totalMatches }));

app.get('/leaderboard', (req, res) => {
  const board = Array.from(users.values())
    .sort((a,b) => b.elo - a.elo).slice(0,20)
    .map(u => ({ name:u.name, elo:u.elo, tier:getTier(u.elo), wins:u.wins, losses:u.losses, labScore:u.labScore }));
  res.json(board);
});

app.get('/stats', (req, res) => res.json({ ...globalStats, onlineNow:users.size, queueSize:matchQueue.length, activeMatches:activeMatches.size }));

/* ════════════════════════════════
   HELPERS
════════════════════════════════ */
function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

function broadcast(data, excludeId=null) {
  const msg = JSON.stringify(data);
  users.forEach((u,id) => { if (id!==excludeId && u.ws.readyState===WebSocket.OPEN) u.ws.send(msg); });
}

function publicUser(u) {
  if (!u) return null;
  return { id:u.id, name:u.name, elo:u.elo, wins:u.wins, losses:u.losses, tier:getTier(u.elo), labScore:u.labScore };
}

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i=0; i<6; i++) code += chars[Math.floor(Math.random()*chars.length)];
  return privateRooms.has(code) ? generateRoomCode() : code;
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🔱 OMOGGLE Server on port ${PORT}`));
