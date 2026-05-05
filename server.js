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

/* ════════════════════════════════
   IN-MEMORY STORE
   (persists while server is running)
════════════════════════════════ */
const users = new Map();        // socketId -> user object
const chatHistory = [];         // last 50 messages
const matchQueue = [];          // users waiting for 1v1
const activeMatches = new Map();// matchId -> match object
const privateRooms = new Map(); // code -> room object
const globalStats = {
  totalUsers: 0,
  totalMatches: 0,
  onlineNow: 0,
};

const MAX_CHAT_HISTORY = 50;

/* ════════════════════════════════
   TIER SYSTEM
════════════════════════════════ */
function getTier(elo) {
  if (elo >= 3000) return { name: 'Chad', emoji: '🔱', color: '#c9a84c' };
  if (elo >= 2000) return { name: 'Chadlite', emoji: '👑', color: '#9b7de0' };
  if (elo >= 1500) return { name: 'HTN', emoji: '💎', color: '#00e5ff' };
  if (elo >= 1000) return { name: 'MTN', emoji: '⚡', color: '#60c890' };
  if (elo >= 500)  return { name: 'LTN', emoji: '🌙', color: '#7090d0' };
  return { name: 'Sub3', emoji: '🔴', color: '#ff4d6d' };
}

function calcEloChange(won, opponentElo, myElo) {
  const K = 32;
  const expected = 1 / (1 + Math.pow(10, (opponentElo - myElo) / 400));
  const actual = won ? 1 : 0;
  return Math.round(K * (actual - expected));
}

/* ════════════════════════════════
   WEBSOCKET CONNECTION
════════════════════════════════ */
wss.on('connection', (ws) => {
  const socketId = uuidv4();
  
  // Default user object
  const user = {
    id: socketId,
    ws,
    name: 'Anonymous',
    elo: 400,
    wins: 0,
    losses: 0,
    labScore: null,
    inQueue: false,
    inMatch: false,
    matchId: null,
    connectedAt: Date.now(),
  };

  users.set(socketId, user);
  globalStats.onlineNow = users.size;
  globalStats.totalUsers++;

  console.log(`[+] User connected: ${socketId} | Online: ${users.size}`);

  // Send welcome payload
  send(ws, {
    type: 'welcome',
    socketId,
    chatHistory: chatHistory.slice(-30),
    onlineCount: users.size,
    stats: globalStats,
  });

  // Broadcast updated online count to all
  broadcast({ type: 'online_count', count: users.size });

  /* ── INCOMING MESSAGES ── */
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // User sets their display name
      case 'set_user': {
        user.name = (msg.name || 'Anonymous').slice(0, 20);
        send(ws, { type: 'user_updated', user: publicUser(user) });
        break;
      }

      // Chat message
      case 'chat': {
        if (!msg.text || msg.text.trim().length === 0) break;
        const text = msg.text.slice(0, 200).trim();
        const tier = getTier(user.elo);
        const chatMsg = {
          id: uuidv4(),
          userId: socketId,
          name: user.name,
          text,
          elo: user.elo,
          tier: tier.name,
          tierEmoji: tier.emoji,
          tierColor: tier.color,
          timestamp: Date.now(),
        };
        chatHistory.push(chatMsg);
        if (chatHistory.length > MAX_CHAT_HISTORY) chatHistory.shift();
        broadcast({ type: 'chat', message: chatMsg });
        break;
      }

      // Join 1v1 matchmaking queue
      case 'join_queue': {
        if (user.inQueue || user.inMatch) break;
        user.inQueue = true;
        matchQueue.push(socketId);
        send(ws, { type: 'queue_joined', position: matchQueue.length });
        broadcast({ type: 'queue_update', size: matchQueue.length });
        tryMatch();
        break;
      }

      // Leave queue
      case 'leave_queue': {
        user.inQueue = false;
        const idx = matchQueue.indexOf(socketId);
        if (idx > -1) matchQueue.splice(idx, 1);
        send(ws, { type: 'queue_left' });
        broadcast({ type: 'queue_update', size: matchQueue.length });
        break;
      }

      // Submit face scan score for match
      case 'submit_score': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        match.scores[socketId] = parseFloat(msg.score) || 0;
        // If both scores submitted, resolve match
        if (Object.keys(match.scores).length === 2) {
          resolveMatch(match);
        } else {
          send(ws, { type: 'score_submitted', waiting: true });
        }
        break;
      }

      // Skip current opponent
      case 'skip_opponent': {
        if (!user.inMatch || !user.matchId) break;
        const match = activeMatches.get(user.matchId);
        if (!match) break;
        // Notify opponent they were skipped
        const opponentId = match.players.find(id => id !== socketId);
        const opponent = users.get(opponentId);
        if (opponent) {
          send(opponent.ws, { type: 'opponent_skipped' });
          opponent.inMatch = false;
          opponent.matchId = null;
        }
        // Put skipper back in queue
        activeMatches.delete(user.matchId);
        user.inMatch = false;
        user.matchId = null;
        user.inQueue = true;
        matchQueue.push(socketId);
        send(ws, { type: 'queue_joined', position: matchQueue.length });
        tryMatch();
        break;
      }

      // Save lab score to user profile
      case 'save_lab_score': {
        user.labScore = parseFloat(msg.score) || null;
        send(ws, { type: 'lab_score_saved', score: user.labScore });
        break;
      }

      // Generate private room
      case 'create_private_room': {
        const code = generateRoomCode();
        const room = {
          code,
          host: socketId,
          guest: null,
          createdAt: Date.now(),
        };
        privateRooms.set(code, room);
        // Expire room after 10 minutes
        setTimeout(() => {
          if (privateRooms.has(code) && !privateRooms.get(code).guest) {
            privateRooms.delete(code);
          }
        }, 10 * 60 * 1000);
        send(ws, { type: 'room_created', code });
        break;
      }

      // Join private room with code
      case 'join_private_room': {
        const code = (msg.code || '').toUpperCase().trim();
        const room = privateRooms.get(code);
        if (!room) {
          send(ws, { type: 'room_error', error: 'Invalid or expired code' });
          break;
        }
        if (room.host === socketId) {
          send(ws, { type: 'room_error', error: 'You cannot join your own room' });
          break;
        }
        if (room.guest) {
          send(ws, { type: 'room_error', error: 'Room is already full' });
          break;
        }
        room.guest = socketId;
        // Start private match
        const matchId = uuidv4();
        const match = {
          id: matchId,
          players: [room.host, socketId],
          scores: {},
          type: 'private',
          startedAt: Date.now(),
        };
        activeMatches.set(matchId, match);
        const host = users.get(room.host);
        const guest = users.get(socketId);
        if (host) {
          host.inMatch = true; host.matchId = matchId;
          send(host.ws, {
            type: 'private_match_start',
            matchId,
            opponent: publicUser(guest),
          });
        }
        if (guest) {
          guest.inMatch = true; guest.matchId = matchId;
          send(guest.ws, {
            type: 'private_match_start',
            matchId,
            opponent: publicUser(host),
          });
        }
        privateRooms.delete(code);
        globalStats.totalMatches++;
        break;
      }

      // Heartbeat
      case 'ping': {
        send(ws, { type: 'pong', timestamp: Date.now() });
        break;
      }
    }
  });

  /* ── DISCONNECT ── */
  ws.on('close', () => {
    console.log(`[-] User disconnected: ${socketId} | Online: ${users.size - 1}`);

    // Remove from queue
    const qIdx = matchQueue.indexOf(socketId);
    if (qIdx > -1) matchQueue.splice(qIdx, 1);

    // Handle active match disconnect
    if (user.inMatch && user.matchId) {
      const match = activeMatches.get(user.matchId);
      if (match) {
        const opponentId = match.players.find(id => id !== socketId);
        const opponent = users.get(opponentId);
        if (opponent) {
          send(opponent.ws, { type: 'opponent_disconnected' });
          opponent.inMatch = false;
          opponent.matchId = null;
        }
        activeMatches.delete(user.matchId);
      }
    }

    users.delete(socketId);
    globalStats.onlineNow = users.size;
    broadcast({ type: 'online_count', count: users.size });
    broadcast({ type: 'queue_update', size: matchQueue.length });
  });

  ws.on('error', (err) => console.error(`WS error for ${socketId}:`, err.message));
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

    if (!u1 || !u2 || !u1.ws || !u2.ws) continue;
    if (u1.ws.readyState !== WebSocket.OPEN || u2.ws.readyState !== WebSocket.OPEN) continue;

    const matchId = uuidv4();
    const match = {
      id: matchId,
      players: [id1, id2],
      scores: {},
      type: 'ranked',
      startedAt: Date.now(),
    };

    activeMatches.set(matchId, match);
    u1.inQueue = false; u1.inMatch = true; u1.matchId = matchId;
    u2.inQueue = false; u2.inMatch = true; u2.matchId = matchId;
    globalStats.totalMatches++;

    send(u1.ws, { type: 'match_found', matchId, opponent: publicUser(u2) });
    send(u2.ws, { type: 'match_found', matchId, opponent: publicUser(u1) });

    console.log(`[MATCH] ${u1.name} vs ${u2.name} | Match: ${matchId}`);
  }
}

/* ════════════════════════════════
   RESOLVE MATCH
════════════════════════════════ */
function resolveMatch(match) {
  const [id1, id2] = match.players;
  const u1 = users.get(id1);
  const u2 = users.get(id2);
  const score1 = match.scores[id1] || 0;
  const score2 = match.scores[id2] || 0;

  const u1Won = score1 > score2;
  const u2Won = score2 > score1;

  if (u1) {
    const change = calcEloChange(u1Won, u2 ? u2.elo : 400, u1.elo);
    u1.elo = Math.max(0, u1.elo + change);
    if (u1Won) u1.wins++; else u1.losses++;
    u1.inMatch = false; u1.matchId = null;
    send(u1.ws, {
      type: 'match_result',
      won: u1Won,
      myScore: score1,
      opponentScore: score2,
      eloChange: change,
      newElo: u1.elo,
      newTier: getTier(u1.elo),
    });
  }

  if (u2) {
    const change = calcEloChange(u2Won, u1 ? u1.elo : 400, u2.elo);
    u2.elo = Math.max(0, u2.elo + change);
    if (u2Won) u2.wins++; else u2.losses++;
    u2.inMatch = false; u2.matchId = null;
    send(u2.ws, {
      type: 'match_result',
      won: u2Won,
      myScore: score2,
      opponentScore: score1,
      eloChange: change,
      newElo: u2.elo,
      newTier: getTier(u2.elo),
    });
  }

  activeMatches.delete(match.id);
  console.log(`[RESULT] Score: ${score1} vs ${score2} | ELO updated`);
}

/* ════════════════════════════════
   REST ENDPOINTS
════════════════════════════════ */
// Health check
app.get('/', (req, res) => {
  res.json({ status: 'OMOGGLE Server Running 🔱', online: users.size, matches: globalStats.totalMatches });
});

// Get leaderboard (top 20 by ELO)
app.get('/leaderboard', (req, res) => {
  const board = Array.from(users.values())
    .sort((a, b) => b.elo - a.elo)
    .slice(0, 20)
    .map(u => ({ name: u.name, elo: u.elo, tier: getTier(u.elo), wins: u.wins, losses: u.losses, labScore: u.labScore }));
  res.json(board);
});

// Get server stats
app.get('/stats', (req, res) => {
  res.json({ ...globalStats, onlineNow: users.size, queueSize: matchQueue.length, activeMatches: activeMatches.size });
});

/* ════════════════════════════════
   HELPERS
════════════════════════════════ */
function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  users.forEach((user, id) => {
    if (id !== excludeId && user.ws.readyState === WebSocket.OPEN) {
      user.ws.send(msg);
    }
  });
}

function publicUser(user) {
  if (!user) return null;
  return {
    id: user.id,
    name: user.name,
    elo: user.elo,
    wins: user.wins,
    losses: user.losses,
    tier: getTier(user.elo),
    labScore: user.labScore,
  };
}

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  // Make sure it's unique
  if (privateRooms.has(code)) return generateRoomCode();
  return code;
}

/* ════════════════════════════════
   START SERVER
════════════════════════════════ */
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔱 OMOGGLE Server running on port ${PORT}`);
});
