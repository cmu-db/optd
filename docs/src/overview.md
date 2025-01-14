# Introduction

optd is a database query optimizer service。The project is in active development.

## Our Wishlist

- Correct and complete enumeration of the search space.
- An accurate cost model powered with advanced statistics that can differentiate plans under a mix of optimization objectives.
- An efficient search algorithm to navigate the vast search space.
- A persistent storage (cache) of query optimizer state that allows us to reuse past optimizations for future queries.
- An explainable, self-correcting, and human-assisted optimization process by producing and consuming a trail of breadcrumbs that could explain every decision that the optimizer makes.
- An intelligent scheduler that exploit parallelism in modern hardwares to boost search performance.
- An extensible operator and rule system.