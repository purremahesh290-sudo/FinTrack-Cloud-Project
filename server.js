// server.js
// Full backend for FinTrack — local uploads (no Cloudinary).
// Expects env: DATABASE_URL, PORT (optional), S3_BUCKET (optional but unused here)

require('dotenv').config();
const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const { parse } = require('csv-parse/sync'); // sync parser (small files). Install: npm i csv-parse
const { v4: uuidv4 } = require('uuid');

const db = require('./db'); // your existing DB helper (pg wrapper)
const { RiskEstimator } = require('./fintrack-risk-lib'); // your risk logic

const app = express();
app.use(express.json());

// Upload configuration: store uploaded files into /tmp/uploads
const UPLOAD_DIR = path.join('/tmp', 'uploads');
try { fs.mkdirSync(UPLOAD_DIR, { recursive: true }); } catch (e) { /* ignore */ }
const storage = multer.memoryStorage();
const upload = multer({ storage });

// helpers
function safeParseNumber(input) {
  if (input === null || input === undefined) return 0;
  // remove currency symbols, commas, whitespace
  const s = String(input).replace(/[^\d.\-]/g, '').trim();
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function normalizeHeader(h) {
  if (!h) return '';
  // strip BOM
  let s = String(h).replace(/^\uFEFF/, '');
  // remove parentheses and quotes, replace non-word (except - and _) with space
  s = s.replace(/[()\[\]"]/g, '').replace(/[^\w\s\-]/g, ' ');
  // collapse whitespace, lowercase
  s = s.replace(/\s+/g, ' ').trim().toLowerCase();
  return s;
}

function pickByTokens(tokens, row) {
  for (const [k, v] of Object.entries(row)) {
    if (!k) continue;
    for (const t of tokens) {
      if (k.includes(t) && String(v || '').trim() !== '') return String(v).trim();
    }
  }
  return null;
}

const estimator = new RiskEstimator();

// ------------------ DB init ------------------
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
    console.log('Tables checked/created');
  } catch (e) {
    console.error('initTables error', e && e.message ? e.message : e);
    throw e;
  }
}

// ------------------ API Endpoints ------------------

// auth echo (simple)
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
    console.error('auth error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// create single transaction via API
app.post('/api/transactions', async (req, res) => {
  try {
    const body = req.body || {};
    if (!body.user_id) return res.status(400).json({ error: 'user_id required' });

    const rawAmount = body.amount ?? body.amountString ?? "0";
    const amount = safeParseNumber(rawAmount);

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
    console.error('/api/transactions error', e);
    res.status(500).json({ error: 'server error' });
  }
});

// list transactions for a user
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

// upload CSV (local store) -> create parse_csv job
app.post('/api/transactions/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'file required' });
    const user_id = req.body.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });

    const fname = `${Date.now()}-${Math.random().toString(36).slice(2,9)}.csv`;
    const localPath = path.join(UPLOAD_DIR, fname);
    fs.writeFileSync(localPath, req.file.buffer);

    const jobId = uuidv4();
    const payload = { user_id, local_path: localPath };
    await db.query('INSERT INTO jobs(id,type,payload,status) VALUES($1,$2,$3,$4)', [jobId, 'parse_csv', payload, 'pending']);
    console.log('Uploaded CSV saved to', localPath, 'job=', jobId);
    res.json({ uploaded: true, path: localPath, job_id: jobId });
  } catch (e) {
    console.error('/api/transactions/upload error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'upload failed', details: e && e.message ? e.message : String(e) });
  }
});

// trigger rescore job
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

// dashboard aggregate
app.get('/api/dashboard', async (req, res) => {
  try {
    const user_id = req.query.user_id;
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    const r = await db.query('SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1000', [user_id]);
    const txs = r.rows;
    const total = txs.reduce((s,t)=> s + Number(t.amount || 0), 0);
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

// serve frontend static (if present)
app.use(express.static(path.join(__dirname, 'frontend')));

// ------------------ Worker: process jobs ------------------

async function processParseCsvJob(job) {
  try {
    const payload = job.payload || {};
    const user_id = payload.user_id;
    const local_path = payload.local_path;
    if (!user_id || !local_path) {
      console.warn('parse_csv job missing fields', job.id);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }

    console.log(`Processing CSV job=${job.id} user=${user_id} path=${local_path}`);

    if (!fs.existsSync(local_path)) {
      console.error('CSV file not found:', local_path);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }

    const buf = fs.readFileSync(local_path);
    const text = buf.toString('utf8');

    // parse into records with normalized headers
    const records = parse(text, {
      columns: (header) => header.map(normalizeHeader),
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true
    });

    console.log(`CSV job ${job.id}: parsed ${records.length} rows`);
    if (records.length > 0) {
      console.log('CSV job header keys (normalized):', Object.keys(records[0]).join(', '));
    }

    let inserted = 0;

    // token lists for fuzzy matching
    const amountTokens = ['amount','amt','value','price','debit','credit','transaction_amount','amount_eur','amount_euro'];
    const merchantTokens = ['merchant','shop','payee','vendor','store','description','narrative','merchant_name'];
    const countryTokens = ['country','location','country_code','countryname','country_name'];
    const dateTokens = ['timestamp','date','datetime','transaction_date','posted_date','created_at','time'];

    for (const row of records) {
      try {
        // pick fields by token matching
        const rawAmount = pickByTokens(amountTokens, row) ?? 0;
        const amount = safeParseNumber(rawAmount);

        const merchant = pickByTokens(merchantTokens, row) || 'unknown';
        const country = pickByTokens(countryTokens, row) || 'Ireland';

        let timestamp = new Date().toISOString();
        const rawTimestamp = pickByTokens(dateTokens, row);
        if (rawTimestamp) {
          const p = new Date(rawTimestamp);
          timestamp = isNaN(p.getTime()) ? rawTimestamp : p.toISOString();
        }

        const tx = { user_id, amount, country, merchant, timestamp };
        const id = uuidv4();
        const score = estimator.scoreTransaction(tx);

        await db.query(
          `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6,$7)`,
          [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
        );
        inserted++;
      } catch (innerErr) {
        console.error('Error inserting CSV row', innerErr && innerErr.message ? innerErr.message : innerErr);
      }
    }

    // cleanup local file (ignore errors)
    try { fs.unlinkSync(local_path); } catch (e) {}

    await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', job.id]);
    console.log(`CSV job ${job.id} done - inserted ${inserted} rows`);
  } catch (err) {
    console.error('processParseCsvJob error', err && err.message ? err.message : err);
    try { await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]); } catch (e) {}
  }
}

async function processRescoreJob(job) {
  try {
    const user_id = job.payload && job.payload.user_id;
    if (!user_id) {
      console.warn('rescore_all job missing user_id', job.id);
      await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
      return;
    }
    console.log(`Processing rescore job=${job.id} user=${user_id}`);
    const txs = await db.query('SELECT id,amount,country,merchant,timestamp FROM transactions WHERE user_id=$1', [user_id]);
    for (const t of txs.rows) {
      const score = estimator.scoreTransaction(t);
      await db.query('UPDATE transactions SET risk_score=$1 WHERE id=$2', [score, t.id]);
    }
    await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', job.id]);
    console.log(`Rescore job ${job.id} done - updated ${txs.rowCount} rows`);
  } catch (e) {
    console.error('processRescoreJob error', e && e.message ? e.message : e);
    try { await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]); } catch (e) {}
  }
}

async function workerLoop() {
  console.log('Worker started, polling for jobs...');
  while (true) {
    try {
      const r = await db.query("SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 10");
      for (const job of r.rows) {
        const id = job.id;
        await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['processing', id]);
        try {
          if (job.type === 'parse_csv') {
            await processParseCsvJob(job);
          } else if (job.type === 'rescore_all') {
            await processRescoreJob(job);
          } else {
            console.warn('Unknown job type', job.type);
            await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', id]);
          }
        } catch (jobErr) {
          console.error('job processing error', jobErr && jobErr.message ? jobErr.message : jobErr);
          await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', id]);
        }
      }
    } catch (e) {
      console.error('worker loop error', e && e.message ? e.message : e);
    }
    // sleep 3s (short)
    await new Promise(r => setTimeout(r, 3000));
  }
}

// ------------------ start ------------------
const PORT = process.env.PORT || 8000;
initTables().then(() => {
  // start worker
  workerLoop().catch(e => console.error('worker failed', e));
  app.listen(PORT, () => console.log(`Server listening on ${PORT}`));
}).catch(e => {
  console.error('initTables failed', e);
  process.exit(1);
});

// export for tests
module.exports = app;
