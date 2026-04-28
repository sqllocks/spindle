# Post 3 — "It Looked Wrong"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

A client looked at the demo data and said: "This doesn't look like our data."

They were right.

---

I had the structure correct. The foreign keys resolved. The cardinalities were plausible.

But the distributions were wrong. The correlations were wrong. The outlier rates were wrong.

Real customer data has correlated columns. Higher income correlates with certain zip codes. Longer tenure correlates with more purchases. rand() doesn't know any of that. The marginal distributions looked fine in isolation. The joint distribution gave it away immediately.

---

So I built a profiler.

Profile your real data — it produces a statistical fingerprint. Distribution shape per column. Correlation matrix. Null rates. Outlier rates. Temporal histograms. String patterns.

Then generate synthetic data that matches that fingerprint, not just the schema.

For numeric distributions, it tries parametric fits first — normal, lognormal, exponential, uniform, Poisson. If the KS test says the fit is poor, it falls back to quantile interpolation: empirical sampling that preserves the real shape.

For correlations: generate the columns independently, then reorder values to achieve the target Pearson correlations without changing any column's marginal distribution. The algorithm is Cholesky decomposition on the correlation matrix. Pure NumPy.

---

I ran it against AdventureWorks — the official Microsoft sample dataset, 18,484 real customer rows.

75.9% fidelity in 6.5 seconds.

The failing columns were all explainable: sequential surrogate keys, unique ID strings, free-text addresses, email format. Everything analytical — income bands, education, marital status, commute distance, occupation — passed.

And it kept going.

---

The next problem wasn't accuracy. It was volume.

That's Post 4.

[ pip install sqllocks-spindle[inference] — link in comments ]
