// upload-route.js
const express = require('express');
const fs = require('fs');
const path = require('path');
const multer = require('multer');
const csv = require('csv-parser');

const router = express.Router();
const UPLOAD_DIR = process.env.UPLOAD_DIR || '/tmp/uploads';
fs.mkdirSync(UPLOAD_DIR, { recursive: true, mode: 0o700 });

// multer storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname.replace(/\s+/g, '_')}`)
});

const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50 MB
  fileFilter: (req, file, cb) => {
    if (!file.originalname.match(/\.(csv|txt)$/i)) {
      return cb(new Error('Only CSV files allowed'));
    }
    cb(null, true);
  }
});

// POST /api/transactions/upload
router.post('/transactions/upload', upload.single('file'), async (req, res) => {
  try {
    // Validate file + user_id
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const userId = (req.body.user_id || '').trim();
    if (!userId) {
      fs.unlink(req.file.path, ()=>{});
      return res.status(400).json({ error: 'user_id required' });
    }

    // Basic user id format check (UUID-like). Adjust as necessary.
    if (!/^[A-Za-z0-9\-_]{8,}$/i.test(userId)) {
      fs.unlink(req.file.path, ()=>{});
      return res.status(400).json({ error: 'user_id appears invalid' });
    }

    // Stream-parse CSV (memory safe)
    const rows = [];
    const rs = fs.createReadStream(req.file.path)
      .pipe(csv({ mapHeaders: ({ header }) => header.trim() }));

    rs.on('data', row => {
      // normalize expected columns
      rows.push({
        user_id: userId,
        amount: Number(row.amount || row.Amount || 0),
        country: row.country || row.Country || '',
        merchant: row.merchant || row.Merchant || '',
        timestamp: row.timestamp || row.Timestamp || (new Date()).toISOString()
      });
    });

    rs.on('end', async () => {
      // Do processing: insert into DB or queue background job
      // Example: respond with uploaded count and a job id
      fs.unlink(req.file.path, ()=>{});
      return res.json({ uploaded: rows.length, job_id: `csv-${Date.now()}` });
    });

    rs.on('error', err => {
      console.error('CSV parse error', err);
      fs.unlink(req.file.path, ()=>{});
      return res.status(500).json({ error: 'CSV parse failed' });
    });

  } catch (err) {
    console.error('Upload error', err);
    if (req.file && req.file.path) fs.unlink(req.file.path, ()=>{});
    return res.status(500).json({ error: 'upload failed', details: err.message });
  }
});

module.exports = router;
