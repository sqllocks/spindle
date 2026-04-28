# Post 5 — "Phase 4"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

I started this project to generate one table of fake data for a client demo. I am now in Phase 4 of a formal development plan, which is not something I anticipated when I wrote those first forty lines of Python.

Phase 4 is what I'm calling a Discovery Sprint, and the name is more honest than it might sound. The project grew quickly — faster than my documentation kept up with — and before I could write anything coherent for public consumption, I needed to establish ground truth on what was actually in the codebase. Not what I thought was in it. What was actually there, working, tested, and ready to use.

The audit has turned up some things I expected and some things I did not.

On the expected side: the core generation engine is solid. The schema inference system — the part that profiles real data and reproduces its statistical shape — works well and has good test coverage. The billion-row pipeline is real and has been validated against an actual cloud environment. The relational constraint system handles the cases I've described in earlier posts and a number of cases I haven't written about yet.

On the unexpected side: there is a warehouse bulk writer that is 610 lines long and not connected to the publish command. No tests either. It exists, it apparently works, and it is not reachable from the CLI. I don't have a clear memory of building it. I have a clear memory of the problem it was solving — getting data into a cloud data warehouse efficiently requires a specific staging and bulk load pattern that the naive approach doesn't handle — but the fact that I built the solution and then didn't wire it up is the kind of thing that only happens when a project is moving faster than its own organization.

There is also a Capital Markets domain. Thirteen data domains total in the project — retail, healthcare, financial services, supply chain, IoT, HR, insurance, marketing, education, real estate, manufacturing, telecom, and capital markets. The capital markets domain has real S&P 500 tickers for around a hundred and ten companies, OHLCV daily pricing, dividends, stock splits, earnings with EPS surprise, insider transactions, and tick-level trade data suitable for streaming scenarios. I apparently built this. It works. I had genuinely not thought about it in approximately two months before the audit turned it up.

There is a chaos engine with four intensity levels — calm, moderate, stormy, and hurricane — that can inject realistic data quality problems into generated datasets for pipeline testing purposes. There is a masking engine that can replace real values with synthetic ones that preserve statistical properties, for cases where you have real data but need to anonymize it. There is an incremental load engine and an SCD2 strategy for slowly changing dimensions. There are thirty-four pre-executed Jupyter notebooks.

I say all of this not to make the project sound impressive — though I'll admit it's more than I realized I'd built — but because I think it illustrates something real about how technical projects actually develop when you're solving real problems as they appear. The features that exist are the features that someone needed. Some of them got finished and tested and documented. Some of them got finished but not wired up. Some of them got started and then interrupted when a more urgent problem appeared. The audit is about finding all of that, characterizing it honestly, and deciding what needs to happen before this is something I'd hand to a stranger and tell them to install.

Phase 5 will be the first fully documented, fully validated public release. Before that happens, the warehouse sink gets wired up and tested. The documentation gets written. The gaps get closed or explicitly acknowledged. The thing I describe publicly will match the thing that actually exists.

If you're working on synthetic data generation, testing environments, or data platform development and any of this sounds familiar — the scope creep, the features that exist but aren't quite connected, the audit that reveals more than you expected — I'd be glad to compare notes. The tool is on PyPI now if you want to take a look at where it stands. Links in the comments.
