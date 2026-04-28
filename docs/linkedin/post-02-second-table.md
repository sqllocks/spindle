# Post 2 — "The Second Table"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

When I left off last week, I had a Python script that could generate a reasonably realistic single table of customer data. The distributions were right. The dates had seasonality. It felt like something a real business might produce. I was fairly pleased with it.

Then I needed a second table.

The specific requirement was straightforward: I needed an orders table that referenced the customers. Each order had to belong to a customer who actually existed in the customer table. Seemed simple enough. I generated a list of customer IDs, then when building each order row I sampled randomly from that list to assign a customer. Done in twenty minutes. I ran it, looked at the output, and immediately knew something was wrong.

The problem was the distribution. When you sample uniformly at random from a list of customer IDs, every customer ends up with roughly the same number of orders. With ten thousand customers and one hundred thousand orders, you get about ten orders per customer, give or take. Real data doesn't work that way. In real transaction data, a small number of customers account for a disproportionate share of the activity. Most customers place one or two orders and never return. A handful of customers place hundreds. The ratio between your most active customer and your median customer is enormous, and if you don't model it, any analyst who looks at your fake data will know something is off the moment they run a frequency distribution.

This is a well-documented phenomenon in customer data — it follows something close to a Pareto distribution, or in some cases a Zipf distribution depending on the domain. Once I understood the problem it wasn't hard to fix: I built an ID manager that could weight the sampling according to whatever distribution fit the domain. Pareto for customers and orders, Zipf for product popularity, uniform for cases where you actually want equal probability. That solved the immediate problem and I moved on.

The next requirement was line items. Orders with individual products, where each line item belonged to an order. Now I had a three-table schema: customers, orders, line items. The foreign keys went in one direction — line items referenced orders, orders referenced customers. The topological sort was straightforward. Generate customers first, then orders, then line items. I built a dependency resolver that would figure out the right generation sequence automatically from the schema definition, and that worked well.

Then I hit the self-referencing hierarchy problem.

The specific case was an employee table. Each employee row had a manager ID that referenced another employee in the same table — except for the top of the org chart, where the manager ID was null. This seems simple but it creates a constraint that is surprisingly awkward to satisfy during generation. You can't generate all the employees first and then assign managers, because the manager IDs have to reference rows that exist in the table, and if you're generating the table from scratch you have to decide which rows will be managers before you know what all the rows are. The solution is to generate in passes — executive layer first with null manager IDs, then each subsequent layer referencing the layer above. Not complicated once you see it, but it took me longer to get there than I'd like to admit.

What I was building by this point was essentially a constraint satisfaction system for tabular data. Every column had a generation strategy — a rule for how to produce values for that column. Some strategies were simple: generate a random number from this distribution, pick from this list. Others were relational: sample from this other table's primary key column, weighted by this distribution. And some were structural: generate this value such that it satisfies this constraint relative to other values in the same row, or other rows in the same table.

The more schemas I tried to support, the more constraint types I discovered. Composite foreign keys, where the relationship is across multiple columns rather than a single ID. Computed columns, where a value is derived from other columns rather than generated independently. Business rules, where certain combinations of values are invalid regardless of their individual distributions. Each one required a new strategy type, and each new strategy type required the generation engine to understand a little more about the structure of the schema.

At some point I stopped thinking of it as a script that generates CSV files and started thinking of it as a declarative system for describing what data should look like, with a generation engine that could satisfy those descriptions. That shift in framing changed how I built everything that came after it.

The problem that came next was harder. I had a system that could generate structurally correct data. But a client looked at the output and said "this doesn't look like our data." They were right. Structural correctness and statistical realism are different things, and I had solved only one of them.

That's next.
