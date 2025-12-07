// server_demo.js
require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const { parse } = require('csv-parse/sync');
const { v4: uuidv4 } = require('uuid');
const RiskEstimator = require('./riskEstimator');

const app = express();
app.use(bodyParser.json());
const upload = multer();

const PORT = process.env.PORT || 8000;
const estimator = new RiskEstimator();

const DB = { users: {}, transactions: {}, jobs: {} };

function listTransactionsForUser(user_id) {
  return Object.values(DB.transactions).filter(t => t.user_id === user_id).sort((a,b) => new Date(b.created_at) - new Date(a.created_at));
}

app.post('/api/auth/echo', async (req, res) => {
  const { email } = req.body || {};
  if (!email) return res.status(400).json({ error: 'email required' });
  const existing = Object.values(DB.users).find(u => u.email === email);
  if (existing) return res.json(existing);
  const id = uuidv4();
  const user = { id, email };
  DB.users[id] = user;
  res.json(user);
});

app.post('/api/transactions', async (req, res) => {
  const body = req.body || {};
  if (!body.user_id) return res.status(400).json({ error: 'user_id required' });

  const rawAmount = body.amount ?? body.amountString ?? "0";
  const amount = Number(String(rawAmount).replace(/[^0-9.-]+/g, '')) || 0;

  const id = uuidv4();
  const score = estimator.scoreTransaction({
    amount,
    country: body.country ?? 'Ireland',
    merchant: body.merchant ?? 'unknown',
    timestamp: body.timestamp ?? new Date().toISOString()
  });

  const record = {
    id,
    user_id: body.user_id,
    amount,
    country: body.country ?? 'Ireland',
    merchant: body.merchant ?? 'unknown',
    timestamp: body.timestamp ?? new Date().toISOString(),
    risk_score: score,
    created_at: new Date().toISOString()
  };
  DB.transactions[id] = record;
  res.json({ id, risk_score: score });
});

app.get('/api/transactions', async (req, res) => {
  const user_id = req.query.user_id;
  if (!user_id) return res.status(400).json({ error: 'user_id required' });
  res.json(listTransactionsForUser(user_id));
});

app.post('/api/transactions/upload', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'file required' });
  const user_id = req.body.user_id;
  if (!user_id) return res.status(400).json({ error: 'user_id required' });
  try {
    const csvText = req.file.buffer.toString('utf8');
    const records = parse(csvText, { columns: true, skip_empty_lines: true });
    const inserted = [];
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
      const id = uuidv4();
      const score = estimator.scoreTransaction(tx);
      const rec = {
        id,
        user_id,
        amount: tx.amount,
        country: tx.country,
        merchant: tx.merchant,
        timestamp: tx.timestamp,
        risk_score: score,
        created_at: new Date().toISOString()
      };
      DB.transactions[id] = rec;
      inserted.push(rec);
    }
    res.json({ inserted_count: inserted.length, inserted });
  } catch (e) {
    console.error('CSV parse error', e);
    res.status(500).json({ error: 'failed to parse CSV', detail: e.message });
  }
});

app.post('/api/rescore', async (req, res) => {
  const user_id = req.body.user_id;
  if (!user_id) return res.status(400).json({ error: 'user_id required' });
  const txs = listTransactionsForUser(user_id);
  for (const rec of txs) {
    const newScore = estimator.scoreTransaction({ amount: rec.amount, country: rec.country, merchant: rec.merchant, timestamp: rec.timestamp });
    rec.risk_score = newScore;
  }
  res.json({ rescore_count: txs.length });
});

app.get('/api/dashboard', async (req, res) => {
  const user_id = req.query.user_id;
  if (!user_id) return res.status(400).json({ error: 'user_id required' });
  const txs = listTransactionsForUser(user_id);
  const total = txs.reduce((s, x) => s + Number(x.amount || 0), 0);
  const byMonth = {};
  for (const t of txs) {
    const month = (new Date(t.timestamp)).toISOString().slice(0,7);
    byMonth[month] = (byMonth[month] || 0) + Number(t.amount || 0);
  }
  res.json({ count: txs.length, total, byMonth });
});

const path = require('path');
app.use(express.static(path.join(__dirname, 'frontend')));

app.listen(PORT, () => console.log(`Demo server listening on ${PORT}`));
