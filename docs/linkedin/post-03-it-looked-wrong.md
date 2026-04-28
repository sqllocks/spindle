# Post 3 — "It Looked Wrong"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

A client looked at the demo data and said: "This doesn't look like our data."

I knew immediately what they meant, and I didn't have a good answer for it. The schema was correct. The foreign keys resolved. The row counts were right. The revenue numbers followed a realistic distribution. By every structural measure the data was fine. But it didn't feel like their data, and that gap — between structurally correct and statistically believable — turned out to be a much harder problem than anything I'd solved up to that point.

The issue is that real datasets have a shape that goes well beyond the distributions of individual columns. Higher income tends to correlate with certain zip codes. Customers who have been around longer tend to have more purchases. Certain product categories sell more in certain seasons, and that pattern shows up consistently across years. These relationships aren't captured in a schema. They're not constraints you can express as foreign keys or business rules. They're emergent properties of real data that synthetic data needs to reproduce if it's going to pass scrutiny.

When you generate columns independently — which is the natural thing to do — you lose all of that. Each column looks right in isolation. The income column has a realistic distribution. The zip code column has realistic geographic spread. But the relationship between them is random, because you didn't model it, and that randomness is immediately visible to anyone who runs a correlation analysis or builds a segmentation model or just stares at the data long enough.

The solution, conceptually, is to profile the real data first — understand its statistical shape — and then generate synthetic data that reproduces that shape rather than just the schema. That's straightforward to say and considerably harder to implement well.

The profiling part involves fitting distributions to each column: is this column normally distributed, or lognormal, or exponential, or something else? How many nulls does it have? What fraction of its values are outliers by IQR? What are the most common values and how often do they appear? For string columns: what patterns are present? Are these email addresses, phone numbers, social security numbers, free-form text? For date columns: is there a weekly cycle? A yearly cycle? A trend over time?

For most numeric columns, you can try a set of candidate distributions and pick the best fit using a KS test. That works well for columns that actually follow a standard distribution. For columns that don't — and there are more of those than you'd expect — you need a fallback. I ended up implementing a quantile-based approach: store the empirical quantiles of the real column, then interpolate when generating synthetic values. It preserves the actual shape of the distribution without requiring you to name it.

The correlation problem required a different approach. The standard technique for generating correlated multivariate data is to use a copula — a function that captures the dependency structure between variables independently of their marginal distributions. I implemented a Gaussian copula, which works by decomposing the target correlation matrix via Cholesky factorization, drawing correlated standard normals, and then transforming them through the inverse CDF of each column's marginal distribution. The result is synthetic data where each column has the right marginal distribution and the columns have approximately the right pairwise correlations.

This sounds more exotic than it is. The implementation is maybe two hundred lines, most of which is bookkeeping. The core algorithm is a few matrix operations. The hard part isn't the math — it's making sure the profiling step produces accurate enough correlation estimates that the generation step has something useful to work with, which requires careful handling of nulls and outliers and columns with low variance.

I validated it against a well-known public dataset — around eighteen thousand rows of real customer data from a Microsoft sample database. The fidelity score, measured by comparing the statistical properties of the synthetic output against the real input, came in just under seventy-six percent. The failures were all in a category I'd expected: sequential surrogate keys, unique identifier strings, free-text fields, email addresses. Everything analytical — income bands, education levels, marital status, commute distance, occupation — matched. That's the meaningful part.

What this meant in practice was that someone could look at a segmentation analysis or a cohort chart built on synthetic data and not immediately know it was synthetic. The texture was right. The correlations were there. The distributions matched. It wouldn't fool a forensic statistician, but it would pass a presentation review, which was what the original problem called for.

The next thing I needed to solve had nothing to do with statistical realism. It was purely about volume.

That's next.