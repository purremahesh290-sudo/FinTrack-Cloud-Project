// server.js (FINAL CLEAN VERSION WITH CSV VALIDATION + LOCAL UPLOADS)

require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { v4: uuidv4 } = require('uuid');
const db = require('./db');
const { RiskEstimator } = require('./fintrack-risk-lib');

const app = express();
app.use(bodyParser.json());

const PORT = process.env.PORT || 8000;
const UPLOAD_DIR = "/tmp/uploads";
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive:true });

const upload = multer();
const estimator = new RiskEstimator();

// ---------- DB TABLES ----------
async function initTables() {
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS users(
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL
      )`);

    await db.query(`
      CREATE TABLE IF NOT EXISTS transactions(
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
      CREATE TABLE IF NOT EXISTS jobs(
        id TEXT PRIMARY KEY,
        type TEXT,
        payload JSONB,
        status TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`);
  } catch(err) {
    console.error("initTables error:", err);
  }
}

// ---------- AUTH ----------
app.post("/api/auth/echo", async (req,res)=>{
  const { email } = req.body || {};
  if (!email) return res.status(400).json({ error:"email required" });

  const r = await db.query("SELECT id,email FROM users WHERE email=$1", [email]);
  if (r.rowCount > 0) return res.json(r.rows[0]);

  const id = uuidv4();
  await db.query("INSERT INTO users(id,email) VALUES($1,$2)", [id,email]);
  res.json({ id,email });
});

// ---------- ADD TX ----------
app.post("/api/transactions", async (req,res)=>{
  try{
    const body = req.body || {};
    if (!body.user_id) return res.status(400).json({ error:"user_id required" });

    const rawAmount = body.amount || "0";
    const amount = Number(String(rawAmount).replace(/[^0-9.-]/g,"")) || 0;

    const tx = {
      user_id: body.user_id,
      amount,
      country: body.country || "Ireland",
      merchant: body.merchant || "unknown",
      timestamp: body.timestamp || new Date().toISOString()
    };

    const id = uuidv4();
    const score = estimator.scoreTransaction(tx);

    await db.query(`
      INSERT INTO transactions(id,user_id,amount,country,merchant,timestamp,risk_score)
      VALUES($1,$2,$3,$4,$5,$6,$7)
    `, [id, tx.user_id, tx.amount, tx.country, tx.merchant, tx.timestamp, score]);

    res.json({ id, risk_score:score });
  } catch(err){
    res.status(500).json({ error:"server error" });
  }
});

// ---------- GET TX ----------
app.get("/api/transactions", async (req,res)=>{
  const user_id = req.query.user_id;
  if (!user_id) return res.status(400).json({ error:"user_id required" });

  const r = await db.query(
    "SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1000",
    [user_id]
  );
  res.json(r.rows);
});

// ---------- CSV UPLOAD WITH HEADER VALIDATION ----------
app.post("/api/transactions/upload", upload.single("file"), async (req,res)=>{
  try{
    if (!req.file) return res.status(400).json({ error:"file required" });
    const user_id = req.body.user_id;
    if (!user_id) return res.status(400).json({ error:"user_id required" });

    const text = req.file.buffer.toString("utf8");
    const firstLine = text.split(/\r?\n/).find(l => l.trim().length>0);
    if (!firstLine) return res.status(400).json({ error:"CSV empty" });

    const normalize = h => String(h).replace(/^\uFEFF/,"")
      .replace(/[()\[\]"]/g,"")
      .replace(/[^\w\s\-]/g," ")
      .replace(/\s+/g," ")
      .trim()
      .toLowerCase();

    const headers = firstLine.split(",").map(h => normalize(h));

    const needAmount = ["amount","amt","value","transaction_amount","monthly_expense_total"];
    const needDate   = ["timestamp","date","datetime","transaction_date"];

    const has = (tokens) => headers.some(h => tokens.some(t => h.includes(t)));

    if (!has(needAmount))
      return res.status(400).json({ error:"CSV missing amount column", headers });

    if (!has(needDate))
      return res.status(400).json({ error:"CSV missing date/timestamp column", headers });

    const filename = `${Date.now()}-${Math.random().toString(36).slice(2,8)}.csv`;
    const localPath = path.join(UPLOAD_DIR, filename);
    fs.writeFileSync(localPath, req.file.buffer);

    const jobId = uuidv4();
    const payload = { user_id, local_path: localPath };

    await db.query(`
      INSERT INTO jobs(id,type,payload,status)
      VALUES($1,$2,$3,$4)
    `, [jobId, "parse_csv", payload, "pending"]);

    res.json({ uploaded:true, path:localPath, job_id:jobId });
  }catch(err){
    res.status(500).json({ error:"upload failed", details:err.message });
  }
});

// ---------- SIMPLE DASHBOARD ----------
app.get("/api/dashboard", async (req,res)=>{
  const user_id = req.query.user_id;
  if (!user_id) return res.status(400).json({ error:"user_id required" });

  const r = await db.query(
    "SELECT * FROM transactions WHERE user_id=$1 ORDER BY created_at DESC LIMIT 1000",
    [user_id]
  );
  const txs = r.rows;

  const total = txs.reduce((s,t)=> s + Number(t.amount||0), 0);
  const byMonth = {};
  for(const t of txs){
    const m = new Date(t.timestamp).toISOString().slice(0,7);
    byMonth[m] = (byMonth[m]||0) + Number(t.amount||0);
  }

  res.json({ count:txs.length, total, byMonth });
});

// ---------- STATIC ----------
app.use(express.static(path.join(__dirname, "client")));

// ---------- WORKER ----------
async function workerLoop(){
  console.log("Worker started...");
  while(true){
    try{
      const r = await db.query(`
        SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 5
      `);
      for(const job of r.rows){
        const id = job.id;
        await db.query("UPDATE jobs SET status='processing' WHERE id=$1", [id]);

        try{
          if (job.type === "parse_csv"){
            const p = job.payload;
            const buf = fs.readFileSync(p.local_path);
            const csv = parse(buf.toString("utf8"), { columns:true, skip_empty_lines:true });

            let inserted = 0;
            for(const row of csv){
              const amtField = Object.keys(row).find(k=> k.toLowerCase().includes("amount"));
              const dateField = Object.keys(row).find(k=> k.toLowerCase().includes("date") || k.toLowerCase()
