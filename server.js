// server.js
require('dotenv').config();

const express = require('express');
const path = require('path');
const multer = require('multer');
const csv = require('csv-parser');
const cloudinary = require('cloudinary').v2;
const { v4: uuidv4 } = require('uuid');

const db = require('./db'); // your DB helper
const { RiskEstimator } = require('./fintrack-risk-lib'); // your risk scoring lib

// ----------------- configuration & setup -----------------
const PORT = process.env.PORT || 8000;
const app = express();
app.use(express.json());

// Multer - use memory storage so we can stream Buffer to cloudinary
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 } // 50 MB
});

// Cloudinary config (read from env)
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME || '',
  api_key: process.env.CLOUDINARY_API_KEY || '',
  api_secret: process.env.CLOUDINARY_API_SECRET || ''
});
const CLOUDINARY_OK = !!(process.env.CLOUDINARY_CLOUD_NAME && process.env.CLOUDINARY_API_KEY && process.env.CLOUDINARY_API_SECRET);

// Risk estimator instance
const estimator = new RiskEstimator();

// ----------------- DB initialization -----------------
async function initTables() {
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL
      )`);
    await db.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        user_id TEXT,
        amount NUMERIC,
        country TEXT,
        merchant TEXT,
        timestamp TIMESTAMP,
        risk_score NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`);
    await db.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        type TEXT,
        payload JSONB,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`);
    console.log('DB tables initialized');
  } catch (e) {
    console.error('initTables error', e && e.message ? e.message : e);
    throw e;
  }
}

// ----------------- helper utilities -----------------
function safeParseNumber(v) {
  if (v === null || v === undefined) return 0;
  const s = String(v).replace(/[^0-9.-]+/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function isLikelyUserId(id) {
  if (!id || typeof id !== 'string') return false;
  // loose check: at least 8 alnum/ - _ chars
  return /^[A-Za-z0-9\-_]{8,}$/.test(id);
}

// ----------------- API routes -----------------

// Simple auth/register echo
app.post('/api/auth/echo', async (req, res) => {
  const { email } = req.body || {};
  if (!email) return res.status(400).json({ error: 'email required' });
  try {
    const r = await db.query('SELECT id,email FROM users WHERE email=$1 LIMIT 1', [email]);
    if (r.rowCount > 0) return res.json(r.rows[0]);
    const id = uuidv4();
    await db.query('INSERT INTO users(id,email) VALUES($1,$2)', [id, email]);
    res.json({ id, email });
  } catch (e) {
    console.error('/api/auth/echo error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// Create transaction
app.post('/api/transactions', async (req, res) => {
  try {
    const body = req.body || {};
    if (!body.user_id) return res.status(400).json({ error: 'user_id required' });

    const amount = safeParseNumber(body.amount ?? body.amountString ?? 0);

    const tx = {
      user_id: body.user_id,
      amount,
      country: body.country ?? 'Ireland',
      merchant: body.merchant ?? 'unknown',
      timestamp: body.timestamp ?? new Date().toISOString()
    };

    const id = uuidv4();
    const score = estimator.scoreTransaction(tx);

    await db.query(
      `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6,$7)`,
      [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
    );

    res.json({ id, risk_score: score });
  } catch (e) {
    console.error('/api/transactions error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// List transactions for a user
app.get('/api/transactions', async (req, res) => {
  try {
    const user_id = req.query.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id query required' });
    const r = await db.query('SELECT * FROM transactions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1000', [user_id]);
    res.json(r.rows);
  } catch (e) {
    console.error('/api/transactions GET error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// Upload CSV endpoint
// - expects multipart form field 'file' and form field 'user_id'
app.post('/api/transactions/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'file required' });

    const user_id = (req.body.user_id || '').trim();
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    if (!isLikelyUserId(user_id)) return res.status(400).json({ error: 'user_id appears invalid' });

    if (!CLOUDINARY_OK) {
      // If you want to save locally instead, implement local disk storage here.
      return res.status(500).json({ error: 'upload failed', details: 'Cloudinary not configured. Set CLOUDINARY_API_KEY / CLOUDINARY_API_SECRET / CLOUDINARY_CLOUD_NAME' });
    }

    // stream buffer to Cloudinary (raw resource type)
    const streamUpload = (buffer) => new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream({ resource_type: 'raw', folder: 'fintrack_uploads' }, (error, result) => {
        if (result) resolve(result);
        else reject(error || new Error('cloudinary upload failed'));
      });
      stream.end(buffer);
    });

    const result = await streamUpload(req.file.buffer);
    if (!result || !result.secure_url) {
      console.error('cloudinary upload returned unexpected result', result);
      return res.status(500).json({ error: 'upload failed', details: 'cloudinary upload did not return a URL' });
    }

    // enqueue job to parse CSV from the uploaded URL
    const jobId = uuidv4();
    const payload = { user_id, url: result.secure_url };
    await db.query('INSERT INTO jobs(id,type,payload,status) VALUES($1,$2,$3,$4)', [jobId, 'parse_csv', payload, 'pending']);

    return res.json({ uploaded: true, url: result.secure_url, job_id: jobId });
  } catch (e) {
    console.error('/api/transactions/upload error', e && e.message ? e.message : e);
    return res.status(500).json({ error: 'upload failed', details: (e && e.message) || String(e) });
  }
});

// Trigger rescore job
app.post('/api/rescore', async (req, res) => {
  try {
    const { user_id } = req.body || {};
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    const jobId = uuidv4();
    const payload = { user_id };
    await db.query('INSERT INTO jobs(id,type,payload,status) VALUES($1,$2,$3,$4)', [jobId, 'rescore_all', payload, 'pending']);
    res.json({ job_id: jobId });
  } catch (e) {
    console.error('/api/rescore error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// Dashboard summary
app.get('/api/dashboard', async (req, res) => {
  try {
    const user_id = req.query.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    const r = await db.query('SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1000', [user_id]);
    const txs = r.rows;
    const total = txs.reduce((s, t) => s + Number(t.amount || 0), 0);
    const byMonth = {};
    for (const t of txs) {
      const month = (new Date(t.timestamp)).toISOString().slice(0, 7);
      byMonth[month] = (byMonth[month] || 0) + Number(t.amount || 0);
    }
    res.json({ count: txs.length, total, byMonth });
  } catch (e) {
    console.error('/api/dashboard error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// Serve static UI (if present)
app.use(express.static(path.join(__dirname, 'frontend')));

// ----------------- Worker loop to process jobs -----------------
async function processParseCsvJob(job) {
  try {
    const payload = job.payload || {};
    const user_id = payload.user_id;
    const url = payload.url;
    if (!user_id || !url) {
      console.warn('parse_csv job missing user_id or url', job.id);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }

    console.log(`Processing CSV for job=${job.id} user=${user_id} url=${url}`);

    // Fetch the CSV file (Node 18+ global fetch)
    const resp = await fetch(url);
    if (!resp.ok) {
      console.error('Failed to fetch CSV url', url, 'status', resp.status);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }

    // stream parse and insert rows
    const stream = resp.body.pipe(csv({ mapHeaders: ({ header }) => header.trim() }));

    let inserted = 0;
    for await (const row of stream) {
      try {
        const tx = {
          user_id,
          amount: safeParseNumber(row.amount ?? row.Amount ?? 0),
          country: row.country ?? row.Country ?? 'Ireland',
          merchant: row.merchant ?? row.Merchant ?? 'unknown',
          timestamp: row.timestamp ?? row.Timestamp ?? new Date().toISOString()
        };
        const id = uuidv4();
        const score = estimator.scoreTransaction(tx);
        await db.query(
          `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6,$7)`,
          [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
        );
        inserted++;
      } catch (innerErr) {
        console.error('Error inserting row from CSV', innerErr && innerErr.message ? innerErr.message : innerErr);
        // continue processing other rows
      }
    }

    await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', job.id]);
    console.log(`CSV job ${job.id} complete - inserted ${inserted} rows`);
  } catch (err) {
    console.error('processParseCsvJob error', err && err.message ? err.message : err);
    try { await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]); } catch (e) {}
  }
}

async function processRescoreJob(job) {
  try {
    const payload = job.payload || {};
    const user_id = payload.user_id;
    if (!user_id) {
      console.warn('rescore job missing user_id', job.id);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }
    const r = await db.query('SELECT id,amount,country,merchant,timestamp FROM transactions WHERE user_id=$1', [user_id]);
    for (const t of r.rows) {
      const score = estimator.scoreTransaction(t);
      await db.query('UPDATE transactions SET risk_score=$1 WHERE id=$2', [score, t.id]);
    }
    await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', job.id]);
    console.log(`Rescore job ${job.id} done for user ${user_id}`);
  } catch (e) {
    console.error('processRescoreJob error', e && e.message ? e.message : e);
    try { await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]); } catch (er) {}
  }
}

async function workerLoop() {
  console.log('Worker started, polling for jobs...');
  while (true) {
    try {
      const r = await db.query("SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 5");
      for (const job of r.rows) {
        await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['processing', job.id]);
        if (job.type === 'parse_csv') {
          await processParseCsvJob(job);
        } else if (job.type === 'rescore_all') {
          await processRescoreJob(job);
        } else {
          console.warn('Unknown job type', job.type);
          await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
        }
      }
    } catch (e) {
      console.error('workerLoop error', e && e.message ? e.message : e);
    }
    await new Promise(r => setTimeout(r, 3000));
  }
}

// ----------------- Start server & worker -----------------
initTables()
  .then(() => {
    // run worker async (do not block server)
    workerLoop().catch(err => console.error('workerLoop failed', err && err.message ? err.message : err));
    app.listen(PORT, () => console.log(`Server listening on port ${PORT}`));
  })
  .catch(e => {
    console.error('Failed to initialize DB tables', e && e.message ? e.message : e);
    process.exit(1);
  });
