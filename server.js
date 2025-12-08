// server.js
// FinTrack backend (local-file CSV upload variant - flexible header mapping)
//
// Features:
// - Express API for auth, transactions, uploads
// - CSV uploads saved to local /tmp/uploads (ephemeral) and enqueued as jobs
// - Worker loop picks up parse_csv and rescore_all jobs
// - CSV parser normalises headers and accepts common synonyms for amount, merchant, country, timestamp
// - Multer memory storage used so req.file.buffer is available
// - Serves frontend from ./frontend if present
//
// WARNING: Uploaded files are stored on instance ephemeral storage (/tmp/uploads).
// If the EB instance is replaced, files are lost. Good for testing/demo.

require('dotenv').config();

const express = require('express');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const csv = require('csv-parser'); // npm install csv-parser
const { v4: uuidv4 } = require('uuid');

const db = require('./db'); // your DB helper (expects db.query)
const { RiskEstimator } = require('./fintrack-risk-lib'); // your risk scoring lib

// ---------- configuration ----------
const PORT = process.env.PORT || 8000;
const UPLOAD_DIR = process.env.UPLOAD_DIR || '/tmp/uploads';
fs.mkdirSync(UPLOAD_DIR, { recursive: true, mode: 0o700 });

// ---------- app & middleware ----------
const app = express();
app.use(express.json());

// multer memory storage (keeps file in req.file.buffer)
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

const estimator = new RiskEstimator();

// ---------- helpers ----------
function safeParseNumber(v) {
  if (v === null || v === undefined) return 0;
  // remove currency symbols and thousands separators, keep - and .
  const s = String(v).replace(/[^0-9\.\-]+/g, '');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function isLikelyUserId(id) {
  if (!id || typeof id !== 'string') return false;
  // simple UUID-ish check (loose)
  return /^[0-9a-fA-F\-]{8,}$/i.test(id);
}

// ---------- DB tables init ----------
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
    console.log('DB tables ensured');
  } catch (e) {
    console.error('initTables error', e && e.message ? e.message : e);
    throw e;
  }
}

// ---------- API routes ----------

// auth/register echo
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

// create single transaction
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

// list transactions for a user
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

// Upload CSV -> save to local disk and enqueue job
app.post('/api/transactions/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'file required' });
    const user_id = (req.body.user_id || '').trim();
    if (!user_id) return res.status(400).json({ error: 'user_id required' });
    if (!isLikelyUserId(user_id)) return res.status(400).json({ error: 'user_id appears invalid' });

    const filename = `${Date.now()}-${Math.random().toString(36).slice(2,8)}.csv`;
    const fullpath = path.join(UPLOAD_DIR, filename);

    fs.writeFileSync(fullpath, req.file.buffer, { mode: 0o600 });

    const jobId = uuidv4();
    const payload = { user_id, local_path: fullpath };
    await db.query('INSERT INTO jobs(id,type,payload,status) VALUES($1,$2,$3,$4)', [jobId, 'parse_csv', payload, 'pending']);

    return res.json({ uploaded: true, path: fullpath, job_id: jobId });
  } catch (e) {
    console.error('/api/transactions/upload error', e && e.message ? e.message : e);
    return res.status(500).json({ error: 'upload failed', details: (e && e.message) || String(e) });
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
    console.error('/api/rescore error', e && e.message ? e.message : e);
    res.status(500).json({ error: 'server error' });
  }
});

// simple dashboard
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

// serve frontend static if exists
app.use(express.static(path.join(__dirname, 'frontend')));

// ---------- Worker loop & job processors ----------

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

    const rs = fs.createReadStream(local_path).pipe(csv({ mapHeaders: ({ header }) => header }));
    let inserted = 0;

    // Flexible header mapping loop:
    for await (const rowRaw of rs) {
      try {
        // Normalize keys: trim + lowercase
        const row = {};
        for (const [k, v] of Object.entries(rowRaw || {})) {
          const nk = String(k || '').trim().toLowerCase();
          row[nk] = v;
        }

        // amount candidates
        const amountCandidates = [
          'amount', 'amt', 'value', 'amount (eur)', 'amount_eur',
          'transaction_amount', 'debit', 'credit', 'money', 'amount_euro', 'price'
        ];
        let rawAmount = 0;
        for (const c of amountCandidates) {
          if (row[c] !== undefined && String(row[c]).trim() !== '') {
            rawAmount = row[c];
            break;
          }
        }
        const amount = safeParseNumber(rawAmount);

        // merchant candidates
        const merchantCandidates = ['merchant', 'shop', 'payee', 'description', 'vendor', 'store', 'narrative'];
        let merchant = 'unknown';
        for (const c of merchantCandidates) {
          if (row[c] !== undefined && String(row[c]).trim() !== '') {
            merchant = String(row[c]).trim();
            break;
          }
        }

        // country candidates
        const countryCandidates = ['country', 'country_code', 'location', 'countryname'];
        let country = 'Ireland';
        for (const c of countryCandidates) {
          if (row[c] !== undefined && String(row[c]).trim() !== '') {
            country = String(row[c]).trim();
            break;
          }
        }

        // date / timestamp candidates
        const dateCandidates = ['timestamp', 'date', 'datetime', 'transaction_date', 'posted_date', 'created_at'];
        let timestamp = new Date().toISOString();
        for (const c of dateCandidates) {
          if (row[c] !== undefined && String(row[c]).trim() !== '') {
            const s = String(row[c]).trim();
            const parsed = new Date(s);
            timestamp = isNaN(parsed.getTime()) ? s : parsed.toISOString();
            break;
          }
        }

        const tx = {
          user_id,
          amount,
          country,
          merchant,
          timestamp
        };

        const id = uuidv4();
        const score = estimator.scoreTransaction(tx);

        await db.query(
          `INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score) VALUES($1,$2,$3,$4,$5,$6,$7)`,
          [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]
        );
        inserted++;
      } catch (innerErr) {
        console.error('Error inserting CSV row', innerErr && innerErr.message ? innerErr.message : innerErr);
        // continue processing other rows
      }
    }

    // optionally delete uploaded file after processing
    try { fs.unlinkSync(local_path); } catch (e) { /* ignore */ }

    await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['done', job.id]);
    console.log(`CSV job ${job.id} done - inserted ${inserted} rows`);
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
        try {
          if (job.type === 'parse_csv') {
            await processParseCsvJob(job);
          } else if (job.type === 'rescore_all') {
            await processRescoreJob(job);
          } else {
            console.warn('Unknown job type', job.type);
            await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
          }
        } catch (jobErr) {
          console.error('job processing error', jobErr && jobErr.message ? jobErr.message : jobErr);
          await db.query('UPDATE jobs SET status=$1 WHERE id=$2', ['failed', job.id]);
        }
      }
    } catch (e) {
      console.error('worker loop error', e && e.message ? e.message : e);
    }
    // small sleep
    await new Promise(r => setTimeout(r, 3000));
  }
}

// ---------- error handler ----------
app.use((err, req, res, next) => {
  console.error('Unhandled error', err && err.stack ? err.stack : err);
  res.status(err.status || 500).json({ error: err.message || 'Server error' });
});

// ---------- start server ----------
initTables()
  .then(() => {
    // start worker in background
    workerLoop().catch(e => console.error('worker failed', e && e.message ? e.message : e));
    app.listen(PORT, () => console.log(`Server listening on ${PORT}`));
  })
  .catch(e => {
    console.error('initTables failed', e && e.message ? e.message : e);
    process.exit(1);
  });
