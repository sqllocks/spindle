# Post 1 — "One Table"
<!-- LinkedIn copy starts below — everything above this line is metadata -->

Every data project hits the same wall: you need data, and you can't use prod.

So you make something up. And it looks made up.

---

I had a client demo in two hours. I needed a customers table.

I wrote about 40 lines of Python. Random names. Random numbers. Random dates. It worked. The demo went fine.

Then I needed it again. Different project. I copied the script.

Then I needed the distributions to be realistic. I tweaked it.

Then I needed a second table. And the foreign keys had to actually match.

And it kept going.

---

That script is now sqllocks-spindle — a synthetic data engine sitting at v2.9.0 on PyPI.

1,994 passing tests. 13 data domains. A billion-row pipeline. A full inference engine.

I needed one table.

---

The foreign key problem is where it got interesting. That's next.

[ pip install sqllocks-spindle — link in comments ]
