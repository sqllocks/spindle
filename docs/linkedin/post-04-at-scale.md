# Post 4 — "At Scale"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

"Can we test this at scale?"

Four words that changed the architecture.

---

Local generation is fine for demos. It is not fine for 500 million rows.

The single-process generator would take hours. More importantly, it would run out of RAM long before it finished.

So I built a multi-process scale router.

The schema gets split into chunks. Each chunk runs in a subprocess worker. Workers are RAM-guarded — psutil checks available memory before spawning the next one, capped at 80% to leave headroom. Primary key continuity is maintained across chunk boundaries via sequence offsets, so you get one coherent dataset, not N independent shards.

---

Reference tables were the first complication.

A product category table with 50 rows should not be generated 1,000 times across 1,000 chunks. It should be generated once and broadcast into every worker as a pre-loaded pool. Static tables — those with cardinality less than the chunk size — are generated once. Dynamic tables are chunked. The worker never knows the difference.

---

Then it needed to land somewhere.

I added sinks — Lakehouse, Warehouse, KQL Eventhouse, SQL Database. A fan-out coordinator writes to all configured targets simultaneously via a thread pool. One generation run, multiple destinations.

Then someone needed Spark.

Above 500,000 rows with a cloud connection configured, the router automatically submits a Fabric Spark notebook run instead of running locally. The notebook is auto-created if it doesn't exist. The schema gets uploaded to cloud storage. The job returns a job ID immediately. You poll status. You don't wait.

And it kept going.

---

At some point I stopped counting what I'd built. So I ran an audit.

That's Post 5.

[ sqllocks-spindle — link in comments ]
