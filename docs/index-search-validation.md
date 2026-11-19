# Index conflicts and Search evaluation

The base is A0–A3's accepted commit `9b7ac8834ad3a89785f2440d343fadfb08c597eb`.
The prior replay measurement and acceptance remain separate.

## B1 — Shared unique-index conflict comparison

Memory and SQLite use the same comparator next to the common multikey
extraction. The candidate lookup, index population and transactions stay
with each engine. The comparator preserves the existing first-conflict
selection and effective collation. SQLite's index-build callback also uses
the shared comparator.

PyMongo 4.20.0 passed 56 targeted engine tests, 992 index and find-and-modify
error tests, and 96 replays on both engines and all four surfaces. Four
native cases passed against each of 7.0.43, 8.0.32 and 9.0.2 with its matching
FCV. The selection covers missing/null inserts, compound multikey arrays,
partial indexes with collation (insert and update), and existing-data
build conflicts. The error probes preserve code, key pattern, key value
and the persisted effect for the witnessed single conflicts.
