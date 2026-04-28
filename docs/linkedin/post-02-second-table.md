# Post 2 — "The Second Table"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

Fake data for one table is easy.

Fake data for a schema with foreign keys is a different problem.

---

I needed a second table. Orders that reference customers.

My first attempt: generate a list of customer IDs, sample randomly when building orders.

It worked. It also looked like every customer ordered exactly the same number of times.

Real FK distributions don't work that way. A small number of customers generate most of the orders. The rest buy once and disappear. That's Pareto. If you don't model it, the data is obviously synthetic the moment anyone looks at the histogram.

So I built an ID manager with configurable distributions — Pareto, Zipf, uniform. You pick the shape that matches your domain.

---

Then I needed composite keys.

Orders with line items, where (order_id, line_number) is the primary key and both columns FK into the parent order. The naive approach breaks the moment you try to generate them across multiple tables simultaneously.

So I built a composite FK strategy, topological dependency resolution, and generation ordering that guarantees child tables always resolve after parents.

It handled that. Then someone needed a self-referencing hierarchy — an employee table where each row references a manager who is also in the same table.

That required a different approach entirely.

And it kept going.

---

Once the structure was right, the data still looked wrong.

That's Post 3.

[ sqllocks-spindle on PyPI — link in comments ]
