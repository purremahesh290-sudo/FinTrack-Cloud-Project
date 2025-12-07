// riskEstimator.js
class RiskEstimator {
  constructor(cfg = {}) {
    const d = {
      large_amount_threshold: 1000.0,
      large_amount_weight: 0.4,
      unusual_country_weight: 0.25,
      off_hours_weight: 0.2,
      merchant_blacklist_weight: 0.15
    };
    this.cfg = { ...d, ...cfg };
    this.merchant_blacklist = new Set(["scamshop ltd","suspicious merchant"]);
  }

  scoreTransaction(tx) {
    let score = 0.0;
    const amount = Number(tx.amount || 0);

    if (amount >= this.cfg.large_amount_threshold) {
      score += Math.min(1.0, Math.log(amount / this.cfg.large_amount_threshold + 1)) * this.cfg.large_amount_weight * 100;
    }

    const country = (tx.country || "").toLowerCase();
    if (country && !["ireland","uk","usa"].includes(country)) {
      score += this.cfg.unusual_country_weight * 100;
    }

    try {
      const t = new Date(tx.timestamp);
      if (!isNaN(t) && t.getHours() >= 0 && t.getHours() < 5) {
        score += this.cfg.off_hours_weight * 100;
      }
    } catch (e) {}

    const merchant = (tx.merchant || "").toLowerCase();
    if (this.merchant_blacklist.has(merchant)) {
      score += this.cfg.merchant_blacklist_weight * 100;
    }

    return Math.min(100.0, Math.round(score * 100) / 100);
  }

  batchScore(list) {
    return list.map(tx => this.scoreTransaction(tx));
  }
}

module.exports = RiskEstimator;
