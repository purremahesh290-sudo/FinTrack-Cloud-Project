// db.js
// Postgres connection helper using pg Pool.
// Reads connection string from DATABASE_URL environment variable.

const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || null,
  // If DATABASE_URL is an RDS-style URL and you need SSL, adjust as needed.
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

async function query(text, params) {
  return pool.query(text, params);
}

module.exports = { query, pool };
