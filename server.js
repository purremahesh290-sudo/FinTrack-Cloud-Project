// server.js
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const cloudinary = require('cloudinary').v2;
const { v4: uuidv4 } = require('uuid');
const db = require('./db');
const RiskEstimator = require('./riskEstimator');

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME || '',
  api_key: process.env.CLOUDINARY_API_KEY || '',
  api_secret: process.env.CLOUDINARY_API_SECRET || ''
});

const app = express();
app.use(bodyParser.json());
const upload = multer();

const PORT = process.env.PORT || 8000;
const estimator = new RiskEstimator();

// init tables
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
  } catch (e) {
    console.error('initTables error', e.message);
  }
}

// ------------------ API Endpoints ------------------

// Basic register (demo / simple)
app.post('/api/auth/echo', async (req, res) => {
  const { email } = req.body || {};
  if (!email) return res.status(400).json({ error: 'email required' });
  try {
    // try find existing
    const r = await db.query('SELECT id,email FROM users WHERE email=$1 LIMIT 1', [email]);
    if (r.rowCount > 0) return res.json(r.rows[0]);
    const id = uuidv4();
    await db.query('INSERT INTO users(id,email) VALUES($1,$2)', [id, email]);
    res.json({ id, email });
  } catch (e) {
    console.error('auth error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// Create transaction (sanitized)
app.post('/api/transactions', async (req, res) => {
  try {
    const body = req.body || {};
    if (!body.user_id) return res.status(400).json({ error: 'user_id required' });

    const rawAmount = body.amount ?? body.amountString ?? "0";
    const amount = Number(String(rawAmount).replace(/[^0-9.-]+/g, '')) || 0;

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
      `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6)`,
      [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
    );
    res.json({ id, risk_score: score });
  } catch (e) {
    console.error('/api/transactions error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// List transactions
app.get('/api/transactions', async (req, res) => {
  try {
    const user_id = req.query.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id query required' });
    const r = await db.query('SELECT * FROM transactions WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1000', [user_id]);
    res.json(r.rows);
  } catch (e) {
    console.error('/api/transactions GET error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// Upload CSV -> store on Cloudinary, create job record to be processed by worker
app.post('/api/transactions/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'file required' });
    const user_id = req.body.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });

    // upload raw file to cloudinary
    const streamUpload = (buffer) => {
      return new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream({ resource_type: 'raw', folder: 'fintrack_uploads' }, (error, result) => {
          if (result) resolve(result);
          else reject(error);
        });
        stream.end(buffer);
      });
    };

    const result = await streamUpload(req.file.buffer);

    const jobId = uuidv4();
    const payload = { user_id, url: result.secure_url };
    await db.query('INSERT INTO jobs(id,type,payload,status) VALUES($1,$2,$3,$4)', [jobId, 'parse_csv', payload, 'pending']);
    res.json({ uploaded: true, url: result.secure_url, job_id: jobId });
  } catch (e) {
    console.error('/api/transactions/upload error', e);
    res.status(500).json({ error: 'upload failed', details: e.message });
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
    console.error('/api/rescore error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// Simple dashboard aggregate
app.get('/api/dashboard', async (req, res) => {
  try {
    const user_id = req.query.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    const r = await db.query('SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1000', [user_id]);
    const txs = r.rows;
    const total = txs.reduce((s,t)=> s + Number(t.amount || 0), 0);
    // aggregate by month
    const byMonth = {};
    for (const t of txs) {
      const month = (new Date(t.timestamp)).toISOString().slice(0,7);
      byMonth[month] = (byMonth[month] || 0) + Number(t.amount || 0);
    }
    res.json({ count: txs.length, total, byMonth });
  } catch (e) {
    console.error('/api/dashboard error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// Serve frontend static if present (optional)
const path = require('path');
app.use(express.static(path.join(__dirname, 'frontend')));

// ------------------ Worker Loop ------------------
async function workerLoop() {
  console.log('Worker started, polling for jobs...');
  while (true) {
    try {
      const r = await db.query("SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 5");
      for (const job of r.rows) {
        const id = job.id;
        await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['processing', id]);
        try {
          if (job.type === 'parse_csv') {
            const url = job.payload.url;
            const user_id = job.payload.user_id;
            console.log('Processing parse_csv', url);
            const resp = await fetch(url);
            const buf = await resp.arrayBuffer();
            const csvText = Buffer.from(buf).toString('utf8');
            const records = parse(csvText, { columns: true, skip_empty_lines: true });

            for (const row of records) {
              const rawAmount = row.amount ?? row.Amount ?? row.AMOUNT ?? "0";
              const cleanAmount = Number(String(rawAmount).replace(/[^0-9.-]+/g, '')) || 0;

              const tx = {
                user_id,
                amount: cleanAmount,
                country: (row.country ?? row.Country ?? 'Ireland').trim(),
                merchant: (row.merchant ?? row.Merchant ?? 'unknown').trim(),
                timestamp: row.timestamp ?? row.Timestamp ?? new Date().toISOString()
              };

              const score = estimator.scoreTransaction(tx);

              await db.query(
                `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6)`,
                [uuidv4(), tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
              );
            }
          } else if (job.type === 'rescore_all') {
            const user_id = job.payload.user_id;
            const rtx = await db.query('SELECT * FROM transactions WHERE user_id=$1', [user_id]);
            for (const rec of rtx.rows) {
              const tx = { amount: Number(rec.amount), country: rec.country, merchant: rec.merchant, timestamp: rec.timestamp };
              const newScore = estimator.scoreTransaction(tx);
              await db.query('UPDATE transactions SET risk_score=$1 WHERE id=$2', [newScore, rec.id]);
            }
          }
          await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', id]);
        } catch (e) {
          console.error('job failed', id, e);
          await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', id]);
        }
      }
    } catch (e) {
      console.error('Worker loop error', e);
    }
    await new Promise(r => setTimeout(r, 3000));
  }
}

// init then start server or worker
initTables().then(() => {
  if (process.argv[2] === 'worker') {
    workerLoop();
    return;
  }
  app.listen(PORT, () => console.log(`Server listening on ${PORT}`));
});
