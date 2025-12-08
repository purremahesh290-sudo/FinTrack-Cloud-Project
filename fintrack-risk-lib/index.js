class RiskEstimator {
  constructor(opts = {}) {
    this.highAmountThreshold = opts.highAmountThreshold ?? 1000;
    this.foreignCountryPenalty = opts.foreignCountryPenalty ?? 0.3;
  }

  scoreTransaction(tx = {}) {
    const amount = Number(tx.amount || 0);
    let score = 0;

    score += Math.min(1, amount / this.highAmountThreshold);

    const country = (tx.country || '').toLowerCase();
    if (country && country !== 'ireland') score += this.foreignCountryPenalty;

    const m = (tx.merchant || '').toLowerCase();
    const suspicious = ['casino', 'bet', 'lottery', 'unknown', 'transfer'];
    for (const s of suspicious) if (m.includes(s)) score += 0.25;

    try {
      const h = new Date(tx.timestamp).getHours();
      if (h >= 0 && h <= 5) score += 0.15;
    } catch (e) {}

    if (score > 1) score = 1;
    if (score < 0) score = 0;

    return Math.round(score * 1000) / 1000;
  }
}

module.exports = { RiskEstimator };
