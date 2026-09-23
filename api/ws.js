import { Redis } from '@upstash/redis';
import bcrypt from 'bcryptjs';
import { WebSocketServer } from 'ws';

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const localClients = new Set();
let isListeningToRedis = false;

async function startRedisListener() {
  if (isListeningToRedis) return;
  isListeningToRedis = true;
  
  while (true) {
    try {
      const messageData = await redis.rpop('minechat_global_stream');
      if (messageData) {
        localClients.forEach((client) => {
          if (client.readyState === 1) {
            client.send(JSON.stringify(messageData));
          }
        });
      }
    } catch (err) {
      console.error("Redis multi-instance loop exception sync error:", err);
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
}

export default async function handler(req, res) {
  if (req.headers.upgrade && req.headers.upgrade.toLowerCase() === 'websocket') {
    const wss = new WebSocketServer({ noServer: true });
    
    wss.on('connection', (ws) => {
      localClients.add(ws);
      startRedisListener();

      ws.on('message', async (messageString) => {
        try {
          const parsed = JSON.parse(messageString);
          if (parsed.type === 'send_message') {
            const payload = { username: parsed.username, content: parsed.content };
            await redis.lpush('minechat_global_stream', payload);
          }
        } catch (e) {
          console.error("Failed mapping inbound pipeline stream chunk data", e);
        }
      });

      ws.on('close', () => {
        localClients.delete(ws);
      });
    });

    wss.handleUpgrade(req, req.socket, Buffer.alloc(0), (ws) => {
      wss.emit('connection', ws, req);
    });
    return; 
  }

  const { method, url } = req;

  if (method === 'POST' && url.includes('/api/signup')) {
    const { email, password, username } = req.body;
    if (!email || !password || !username) return res.status(400).json({ error: "Missing fields" });

    const exists = await redis.hget('minechat_users', email);
    if (exists) return res.status(400).json({ error: "Email already taken" });

    const hashedPassword = await bcrypt.hash(password, 10);
    await redis.hset('minechat_users', { [email]: JSON.stringify({ email, password: hashedPassword, username }) });
    return res.status(200).json({ message: "Registration successful" });
  }

  if (method === 'POST' && url.includes('/api/login')) {
    const { email, password } = req.body;
    const userData = await redis.hget('minechat_users', email);
    if (!userData) return res.status(400).json({ error: "User profile records missing!" });

    const isMatch = await bcrypt.compare(password, userData.password);
    if (!isMatch) return res.status(400).json({ error: "Invalid password matching authentication!" });

    return res.status(200).json({ username: userData.username });
  }

  if (method === 'GET' && url.includes('/api/posts')) {
    const posts = await redis.lrange('minechat_posts', 0, 50) || [];
    return res.status(200).json(posts);
  }

  if (method === 'POST' && url.includes('/api/posts')) {
    const { username, content, category } = req.body;
    await redis.lpush('minechat_posts', { username, content, category });
    return res.status(200).json({ success: true });
  }

  return res.status(404).json({ error: "Missing handling function route mapping parameters" });
}
