Reference:


This module implements subquery decorrelation, based on the paper "Improving 
Unnesting of Complex Queries (BTW 2025)" by Thomas Neumann. 


Design Decisions:


While the paper contains the essential pseudocode for the main structure of the
algorithm, a few decisions had to be made about specifics: 
TODO - write this section


Gaps to the paper's implementation / Future TODOs:


1 - We currently only support full unnesting, without the "simple unnesting" 
pass before that. This is logically correct, and anything that can be simply 
unnested can be fully unnested. However, we can probably shave off a few 
milliseconds by running a simple unnesting pass first if latency is essential in
the future (i.e. OLTP applications).

2 - The 4 "advanced constructs" from the paper (CTEs, WITH RECURSIVE, FULL OUTER
joins, ORDER BY with LIMIT) are unsupported, since many of these constructs 
don't even have a meaningful IR in optd. When these constructs are added, the
implementation rules for those operators can be added.

3 - We compute outer-references and accessing sets on the fly, by checking
which columns of an operator are not bound by downstream columns being
produced. This is a much bigger latency concern than (1), since we do
potentially O(n) work per operator in the tree, i.e. potentially O(n^2)
total work. This should be done in a pass before decorrelation, where we
mark every dependent join with the operators that access outer columns, and
every operator with which of its columns are outer references or bound
references.