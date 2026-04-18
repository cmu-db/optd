# 1 Introduction
One of the key properties of SQL is that it is a declarative query language, where the user specifies only the query intent and the database system then finds the best execution strategy for this query. At least, that is the promise of it being declarative. In reality, this declarative nature sometimes breaks down when formulating complex queries, in particular when the queries contain complex subqueries.
FIG 1:
```
SELECT *
FROM customer
WHERE c_mktsegment='AUTOMOBILE'AND
(SELECT COUNT(*)
FROM orders
WHERE o_custkey=c_custkey AND
(SELECT SUM(l_extendedprice)
FROM lineitem
WHERE l_orderkey=o_orderkey)>300000)>
```
Consider the example query in Figure 1. While conceptually simple – it finds customers from the automobile market that have more than 5 orders with a revenue of more than 300K - it is very unpleasant for most database systems. The canonical translation of this query into relational algebra shown on the right uses a _dependent join_ ( ), where the right-hand side of the join is evaluated for every tuple of the left-hand side. This nested loop evaluation is necessary because the right-hand side accesses values provided from the left-hand side, which would not be possible if we were to use, e.g., a regular hash join. But algorithmically a dependent join is a very bad idea, the runtime complexity of a join 𝐿 𝑅 is inΘ(|𝐿||𝑅|), which grows quadratically with the input sizes. And for database engines execution times in 𝑂(𝑛^2)are often prohibitive, as our𝑛tends to be in the order of millions. On TPC-H scale factor 1, this query needs over 25 minutes on PostgreSQL 16, even though it processes just 1 GB of data. Note that it is possible to reformulate the query in a way such that PostgreSQL can answer it instantly, by decoupling the subqueries, but that makes the query longer and more difficult to write. And given that SQL aims to be declarative, that transformation should be done by the DBMS automatically.
Historically, database systems tried to cope with correlated subqueries by recognizing common patterns in SQL queries and rewriting them to eliminate correlations [Ki82,JK84, Ki85]. While this allows for an efficient execution of some correlated subquery patterns, this is not a very satisfying approach, as even small changes to the SQL text can break these patterns, again resulting in disastrous execution times. A fundamentally different approach was proposed in our previous work ten years ago [NK15]: It operates on the algebra level, making it robust against SQL variations, and it is based upon two observations: 1) we can always rewrite a dependent join into a dependent join where the right-hand side is joined with the unique correlation values from the left-hand side, and 2) a dependent join where the left-hand side is duplicate free can be pushed down an algebra tree until the right-hand side no longer depends on the left-hand side. We will discuss the approach in more detail in Section 2.
Conceptually, this algorithm allows for eliminating correlations in arbitrary queries, dramatically improving the performance of these queries and making it possible to execute them even for large inputs. It was such a significant improvement that several database systems now use it for unnesting queries. Both Hyper and DuckDB use it in commercial offerings, our research system Umbra uses it, and we are aware of several other systems that have prototype implementations. This more widespread usage exposed some limitations of the original publication [NK15]: First, there are some advanced SQL constructs that are not covered by the algorithm, particularly recursive SQL and complex order by and group by constructs. And second, the algorithm sometimes exhibits pathological behavior when dependent queries are nested inside each other, similar to what happens in our query in Figure 1: The unnesting algorithm has to be executed twice – once for each dependent join. The original algorithm does that bottom-up. But because they are stacked inside one another, the second unnesting pass increases the costs of the first join, leading to a performance degradation that becomes worse the deeper the nesting is. See Section 2.3 for a detailed example.
In this paper, we address both shortcomings. First, we change the algorithm to use a top-down strategy that takes subsequent dependent joins into account, leading to dramatically better performance in the case of deep nestings. And second, we cover the remaining SQL constructs, in particular recursive SQL and ORDER BY LIMIT constructs.
The rest of this paper is structured as follows: We give an overview of the problem statement and discuss the original algorithm from [NK15] in Section 2. We then develop a new algorithm that improves the previous approach in Section 3. Complex SQL constructs are handled in Section 4. An evaluation is included in Section 5. Related work is discussed in Section 6 and the results are summarized in Section 7.
## 2 Background
In the following section we will first discuss the problem statement and an algebraic representation of correlated subqueries. Then we will briefly repeat the unnesting algorithm from [NK15], and we will show the limitations of that approach.
**2.1 Problem Statement**
Internally, most database systems represent queries in relational algebra. Which means that we need an algebraic representation for correlated subqueries. For that purpose, we use the dependent join, a variant of a regular join. That is, the dependent join makes the left-hand side of the join visible on the right-hand side. Note that the usage of a dependent join is not optional in the presence of correlation. If we wrote, e.g.,𝐿 (𝜎𝐿.𝑥<𝑅.𝑦(𝑅))we would get a type error, as𝐿.𝑥is undefined in the expression𝜎𝐿.𝑥<𝑅.𝑦(𝑅). If we use a dependent join like this𝐿 (𝜎𝐿.𝑥<𝑅.𝑦(𝑅)),𝐿.𝑥 becomes bound and is therefore available. Note that dependent joins exist for all variants of joins, i.e. left semi join ( ), left outer join ( ) etc. have the corresponding dependent left semi join ( ), dependent left outer join ( ) etc. In all of these variants the right-hand side is evaluated for every tuple from the left-hand side.
To simplify this reasoning about available and required attributes we introduce a bit of notation. If𝑇is an expression in relational algebra,A(𝑇)is the set of attributes produced by that expression. In our example,A(𝑅)would include𝑅.𝑦. If an attribute is used that is not provided by its input (or a higher-up dependent join) it becomes a free variable, we denote that asF(𝑇). In our example,F(𝑅)=∅andF(𝜎𝐿.𝑥<𝑅.𝑦(𝑅))={𝐿.𝑥}. Note that it is not possible to evaluate an algebra expression ifF(𝑇)is not empty, all values have to be bound first. This can be done either by placing a dependent join on top that provides these attributes or by changing the expression to remove the free variables.
In the canonical translation of SQL into relational algebra, each subquery is added to the outer query via a dependent join (in the case of scalar subqueries via a dependent single join [NLK17]). This makes the attributes of the outer query available to the inner query. However, this comes at a high cost, the quadratic runtime of dependent joins. Thus, the problem of _query unnesting_ can be formulated as follows:
_Problem Statement_ : Given a query in relational algebra, including dependent joins. Rewrite the query into an equivalent algebra expression that does not use dependent joins.
Note that we can trivially replace a dependent join with a regular join if the right-hand side does not depend on the left-hand side. We usually assume that all dependent joins are non-trivial. Next, we will discuss a bottom-up strategy that eliminates all remaining dependent joins.
**2.2 Bottom-Up Unnesting**
The paper [NK15] introduces an algorithm that does a bottom-up traversal of the algebra tree and eliminates all dependent joins that it encounters. It uses two strategies for that: First, some dependent joins just exist because of syntax constraints of SQL. For example, this query here
```
SELECT *
FROM R
WHERE EXISTS(SELECT *
FROM S
WHERE R.x=S.y)
```
gets translated into a dependent semi join, but it is easy to see that we can pull up the filter condition into the semi join like this:𝑅 𝑅.𝑥=𝑆.𝑦𝑆, eliminating the dependency. The paper calls that _simple unnesting_ and always tries it first.
If that is not sufficient to eliminate the dependent join, the approach uses the second strategy, which is more complex but can handle arbitrary queries. It is based on two observations: First, for correlated subqueries it is not really necessary to evaluate the inner query for every tuple of the outer query; it is sufficient to evaluate it once for every possible binding of the free variables. We can compute the _domain_ of the free variables 𝐷 and use that to drive the dependent join. That join is performed with is semantics, i.e., NULL is considered a dedicated value, and two NULLs compare as equal.
Rewriting the original dependent join into a regular join and a dependent join with the binding domain𝐷has two advantages: First, we know that|𝐷| ≤ |𝐿|, which means that our query has potentially become cheaper because we execute the inner query less often. Even more importantly,𝐷is duplicate free, which allows for push down rewrites that do not hold for arbitrary tables. The paper then lists a number of rules to push down the dependent join.
This process is repeated until the dependent join becomes trivial and can be replaced with a regular join. This must always happen because the dependent join is pushed down further with each transformation and the leaves of the algebra tree have no free variables. As a final step, the query optimizer can decide if it wants to remove the dependent join with 𝐷: If all attributes of𝐷are bound in equality conditions, e.g., if𝐷consists of the column 𝑥and the query contains the filter condition𝑥=𝑦, the join with𝐷can be removed and replaced with𝜒𝑥:𝑦instead, such that the column𝑥is computed by the value𝑦. This works because𝐷was duplicate free, thus we know that the dependent join will find at most one join partner, and because our original dependent join was transformed into natural D, and thus the join condition will be enforced at that point of the plan. Conceptually this results in the most elegant plans, as now𝐷is completely gone and the resulting algebra expression looks like any regular query, but performance-wise it might be a bad idea to do so. If the join with𝐷was selective it might be better to keep the join with𝐷, as it removes tuples earlier.
But regardless of whether or not the query optimizer decides to perform the final trans- formation step, the result will be a completely unnested plan without any dependent joins. While that sounds perfect, we will discuss limitations of that approach next.
**2.3 Limitations of the Bottom-Up Approach**
While the bottom-up approach handles most queries just fine, it unfortunately degenerates in some corner cases. We were originally notified about this by Sam Arch, who translated complex UDFs into pure SQL [Fr24]. There, similar to our original example in Figure 1, it could happen that dependent subqueries are nested inside each other. 
This problem becomes worse and worse the deeper the nesting is. Sam Arch had sent us a query – tellingly calledcrash.sql– that contained six nested joins and where the unnesting strategy created large intermediate cross products which would all then vanish after filtering of subsequent operators, but which consumed memory and CPU time regardless. This motivated us to develop a holistic unnesting strategy that considered all dependent joins simultaneously, thus avoiding the unnecessary cross products. We will discuss that new algorithm next.
## 3 Holistic Query Unnesting
In this section, we introduce a holistic unnesting algorithm that processes nested dependent joins together in one pass. It can be seen as a top-down variant of [NK15], as it uses the same idea of pushing down the domain𝐷of the free variable bindings. But we formulate it differently: instead of explicitly pushing down𝐷 through the tree, the algorithm remembers which𝐷would have to be added, and rewrites all operators in a top-down pass until𝐷becomes unnecessary or can be added safely.
We split the algorithm into three parts: First, a preparatory phase that identifies all non-trivial dependent joins and annotates them with information that the main algorithm needs. Second, the logic to eliminate dependent joins, which will be called for all non-trivial dependent joins in top-to-bottom order and which is the main algorithm, and third, the unnesting rules for individual operators. Note that we do not include a formalization of this approach due to space constraints, formal definitions and a proof of correctness can be found in a technical report [Ne24].
**3.1 Identifying Non-Trivial Dependent Joins**
First, we have to identify all non-trivial dependent joins, i.e., dependent joins where the right-hand side accesses attributes provided by the left hand side. The easiest way to achieve this is to use an indexed algebra [FMN23], where we can query the structure of the algebra expressions. If this functionality is available, we consider every column access and compute the lowest common ancestor (LCA) of the operator◦^1 that accesses the column and the operator◦^2 that provides the column. If the LCA is not◦^1 , it must be a dependent join^3 and we annotate^3 with the fact that◦^1 is _accessing_ the left-hand side of^3. The annotations for the example query from the previous section are shown in Figure 2. Dependent joins that have no accessing operators are trivial and can simply be converted into regular joins.
Using an indexed algebra is ideal for this phase because it can do every LCA computation in𝑂(log𝑛)without any additional data structures. If the DBMS does not support that functionality, the same information can be computed with worse asymptotic complexity by keeping track of the column sets that are available in the different parts of the tree.
**3.2 Eliminating Dependent Joins**
After identifying all non-trivial dependent joins, we process them in a top-to-bottom order, eliminating each of them. If a dependent join is nested within another dependent join, it will usually be eliminated while the ancestor join is eliminated. That is not always the case because, as we will see below, eliminating the ancestor join might stop before reaching the descendant join. But we never push different𝐷sets across dependent joins, thus eliminating the problem discussed in Section 2.3.
Fig. 3: Simple Dependent Joins Elimination Join:
```
1 fun simpleDJoinElimination(join):
2     for op in accessing(join):
3         if path from op to join is linear:
4             if op is a selection and can merge op into join:
5                 merge op into join
6                 remove op from accessing(join)
7             else if op is a map and can move op above join:
8                 move op above join
9                 remove op from accessing(join)
10     if accessing(join) is empty:
11         convert join into regular join
12     return whether or not we converted the join
``````

In our running example, we would start with^1 then first try a _simple dependent join elimination_. This is a generalization of the simple unnesting from [NK15] and shown in Figure 3: We inspect all operators _op_ that are _accessing_ the left-hand side of the the join, and we check if the path from that operator to the join consists of only _linear_ operators (i.e., operators that allow for partitioning their input, see [G.20]). That can be checked in 𝑂(log𝑛)with an indexed algebra, or with a linear traversal from _op_ to _join_ otherwise. If yes, we can move _op_ up towards the join. If _op_ is a selection, we do that if permitted, merging the selection with the join and thus eliminating the accessing operator. For maps (sometimes also called projections), we cannot simply drop the map, but we can check if we could move the map above the join. If that is possible, we do that, again eliminating the operator from the list of _accessing_ operators. If afterwards no accessing operator is left, we can simply convert the dependent join into a regular join. That step would be sufficient for theEXISTS query from Section 2.2, but in our running example none of the dependent joins can be eliminated like that.
Fig. 4: State Maintained During Unnesting:
```
1 struct UnnestingInfo:
2     join : join operator of the form𝐿 𝑅
3     outerRefs :A(𝐿)∩F(𝑅)
4     D : domain computation,ΠouterRefs(𝐿)
5     parent : parent Unnesting struct (if any)
6 struct Unnesting:
7     info : the UnnestingInfo shared between Unnesting states
8     cclasses : union find data structure of equivalent columns
9     repr : mapping from outerRefs columns to new colunns (if any)
```

If simple unnesting is not sufficient, the full unnesting algorithm is started. It has to rewrite the right-hand side of the dependent join such that no references from the outer side occur anymore. To do that it maintains some state, as shown in Figure 4. The state is split into the part that remains the same the whole time (UnnestingInfo) and the part that changes in the different parts of the query (Unnesting). In the global part, it stores the join itself and the set of outer references. That is, the columns from the left that are accessed from the right, and the corresponding domain computationD. Theparententry allows for recognizing nested dependent joins within the algorithm. In the query fragment local part, it maintains a union-find data structurecclassesto keep track of equivalent columns. We use that for transitive reasoning. If the query has predicates of the form𝑎=𝑏and𝑏=𝑐, and we have an outer reference to𝑐, we could substitute that with𝑎if𝑎is available. If we have substituted an outer reference with something else we store that inrepr, which allows us to look up the new name of an outer reference.

The algorithm will substitute outer references with other columns, thus it is convenient to have a helper function that rewrites all column references that occur in an operator, as shown below, that will be called after the suitable replacements have been found:
Fig. 5: Helper Function To Rewrite References to Outer Columns:
```
1 fun rewriteColumns(op, unnesting):
2     for each column reference c in op:
3         if unnesting.repr contains c:
4             replace c with unnesting.repr[c]
```
With that infrastructure we can now describe the real elimination algorithm. It will be called for each dependent join, ordered from root to bottom, and can optionally be passed a parentUnnestingstructure if we encounter another dependent join during our unnesting (see Section 3.3). It will first check if simple unnesting is sufficient. If yes it will stop, otherwise, It will unnest the left-hand side in the case of nested dependent joins. Then, it creates a newUnnestingstructure, merges it with the parent unnesting if needed, and unnests the right-hand side. The pseudo code for that step is shown in Figure 6.
Fig. 6: The General Case of Dependent Join Elimination:
```
1 fun dJoinElimination(join, parentUnnesting, parentAccessing):
2     // Check if simple unnesting is sufficient
3     if simpleDJoinElimination(join):
4         // Handle the outer unnesting if needed
5         if parentUnnesting is set:
6             for each map m moved by simpleDJoinElimination:
7                 rewriteColumns(m, parentUnnesting)
8             unnest(join, parentUnnesting, parentAccessing)
9         return
11     // In the nested case we have to unnest the left-hand side first
12     if parentUnnesting is set:
13         accLeft =∅
14         for a in parentAccessing:
15             if a is contained in join.left:
16                 insert a into accLeft
17         unnest(join.left, parentUnnesting, accLeft)
18         // Update our condition as needed
19         rewriteColumns(join.condition.parentUnnestring)
20         for each map m moved by simpleDJoinElimination:
21             rewriteColumns(m, parentUnnesting)
23     // Create a new unnesting struct
24     info = new UnnestingInfo for join, set parent=parentUnnesting
25     unnest = new Unnesting for info
27     // Merge with parent unnesting if needed
28     acc = accessing(join)
29     if parentUnnesting is set:
30         for each a in parentAccessing:
31             if a is contained in join.right:
32                 insert a into acc
34     // Unnest right-hand side
35     add equivalences from join.condition to unnest.cclasses
36     unnest(join.right, info, acc)
38     for each c in info.outerRefs:
39         add "{c}␣is␣not␣distinct␣from␣{info.repr[c]}" to join.condition
```
**3.3 Unnesting Rules**
We now briefly discuss the unnesting rules for individual operators. These are analogous to the push down rules from [NK15], thus we only give an overview over the most important rules due to space constraints. All operators share the common logic that 1) if we call unnest(◦, info, accessing)andaccessingis empty, unnesting stops, and we either replace◦withinfo.D ◦or we substitute all outer references with equivalent columns from info.cclasses, and 2) when recursing to an input of an operator◦,◦is removed from the accessinglist if needed. We do not list that code explicitly.
For selections, the unnesting logic registers the filter conditions and rewrites all columns on the way back:
```
1 fun unnest(select, info, accessing):
2     add equivalences from select.predicate to info.cclasses
3     unnest(select.input, info, accessing)
4     rewriteColumns(select.condition, info)
```
Similar for maps, where we only have to rename columns after unnesting the input:
```
1 fun unnest(map, info, accessing):
2     unnest(map.input, info, accessing)
3     rewriteColumns(map.computations, info)
```
The group by operator unnests its input and adds the outer references to the group by attributes. Note that there is a special case where the group by is _static_ (i.e., a select clause with aggregation functions but withoutgroup by). In that case, we must produce a non-empty output for an empty input, and we provide that with an outer join (or a group join [MN11] if supported by the system):
```
1 fun unnest(groupby, info, accessing):
2     static = groupby.groups is empty or groups.groupingsets contains∅
3     unnest(groupby.input, info, accessing)
4     rewriteColumns(groupby, info)
5     for c in info.outerRefs:
6         add info.repr[c] to groupby.groups
7     if static:
8         replace groupby with info.Dgroupy, joining on mapped info.outerRefs
```
Window operators are conceptually handled similar to group by. We have to add the outer references to the partition by clause in order to compute the window functions separately for each binding:
```
1 fun unnest(window, info, accessing):
2     unnest(window.input, info, accessing)
3     rewriteColumns(window, info)
4     for c in info.outerRefs:
5         add info.repr[c] to window.partitionby
```
For joins, we check if we have encountered another dependent join, merging both removals if needed. Otherwise, we check if all dependent accesses occur only on one side. If yes, and if the join type does not have to keep track of the number of join partners, we can simply recurse, like a selection. If not, we unnest both sides and update the join condition as needed:
```
1 fun unnest(join, info, accessing):
2     split accessing into accessingLeft and accessingRight for input of join
4     // Check if we have encounter another dependent join
5     if accessing(join) is not empty:
6         dJoinElimination(join, info, accessing)
7         return
9     // Check if only one side accesses outer columns
10     if accessingRight is empty and info.join cannot output unmatched from the right:
11         unnest(join.left, info, accessingLeft)
12         rewriteColumns(join.condition, info)
13         return
14     if accessingLeft is empty and info.join cannot output unmatched from the left:
15         unnest(join.right, info, accessingRight)
16         rewriteColumns(join.condition, info)
17         return
19     // Unnest both sides
20     unnestingLeft = new Unnesting(info.info)
21     unnestingRight = new Unnesting(info.info)
22     unnest(join.left, unnestLeft, accessingLeft)
23     unnest(join.right, unnestRight, accessingRight)
24     rewriteColumnsForJoin(join.condition, unnestingLeft, unnestingRight)
25     for c in info.outerRefs:
26         add "{unnestLeft.repr[c]}␣is␣not␣distinct␣from␣{unnestRight.repr[c]}" to join.condition
27     merge cclasses and repr from unnestLeft and unnestRight into info
```
Note that a different logic for rewriting columns is needed for joins, as we have to take the join type into account to decide which side of the join to use for the replacement. A left outer join, which outputs unmatched rows from the left, would use the left side for the replacement, while a right outer join would use the right side. A full outer join must determine the used side per row, by replacing the columns with expressions such as COALESCE(unnestLeft.repr[c], unnestRight.repr[c]).