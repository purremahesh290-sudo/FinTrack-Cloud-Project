// fintrack-risk-mahesh - small risk estimator
class RiskEstimator {
  constructor(opts={}) { this.highAmountThreshold = opts.highAmountThreshold ?? 1000; }
  score(tx={}) {
    const amount = Number(tx.amount||0);
    let score = 0;
    if (amount >= this.highAmountThreshold) score += 0.6;
    if ((tx.country||'').toLowerCase() !== 'ireland') score += 0.2;
    if (/unusual|suspicious|unknown/i.test(tx.merchant||'')) score += 0.2;
    return Math.min(1, Number(score.toFixed(2)));
  }
}
module.exports = { RiskEstimator };
