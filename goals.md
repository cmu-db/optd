GoalId: GroupId + (required) PhysicalProperties
--> Unique id for each (GroupId + PhysicalProperties)
--> If group id merges, goal id can merge

GoalId has equivalencies (contains):
Equivalencies are either:
1. PhysicalExpressionIds (TODO(yuchen): might need property info here).
2. Other GoalIds


Flow:
1. Ingest logical plan
2. Get group id of root
3. Get/create goal id from group id + []
4. Subscribe on the goal id
5. Run BFS / DFS to get all equivalent goals
6. Launch implementation tasks for all those goals
7. Run BFS / DFS to get all physical expression ids (leaves)
8. Launch costing tasks for all these expression ids
--
9. For implementation results: we get back partial physical plans (leaves are group id + props).
10. We ingest, resolve to physical expression id or goal id. Unlike logical merges, we do not 
create new groups for each expression that is alone. However we do assign it an expression id.
Along the way, we might create new goal ids, but that's fine.
11. We immediately launch the costing of that new expression (if new). The engine will in turn
expand the children goals.



LogicalSort(1) -> (1, sorted) => this is a goal! not an expression id
- An easy way to think about this is converting a logical expression into a physical propert + the child group.
Join(1) -> HashJoin(1) => this is an expression id

Example: for filter passthrough we would have
(2, [blaaa]) -> <LogicalFilter(1), [blaaa]> => PhysicalFilter(1, [blaaa])

Group 1: LogicalSort(2, on A.a)
Group 2: Get(A)

Goal 1: (1, [])

impl rule:
emit (2, [sorted on A.a]) -- created a new goal 2

Goal 1 -> Goal 2

launch implement of goal 2
emit IndexScan(A, indexed_on=A.a)
emit Sort(Scan(A), on=A.a)
Both expressions added to goal 2

Then they get costed.

Later we optimize
(Group 2, []): Goal 3
emit IndexScan(A, indexed_on=A.a)
emit Scan(A)

You never merge Goal 2 and 3! but you will benefit from costing the shared expressions only once.

TODO:
- When subscibing to a goal id, we should also get notified of any new goal / expression in that subgraph.
--> Having hierarchy of tasks

Goal 1:
- Goal 2




Optimize Goal task can launch
- Other optimize goal
- Cost expressions tasks

Add parents in tasks. And then use that to notify every time a new costed expression is produced and better.


- If a new equivalent goal pops up, we need to implement it (NOT DONE RIGHT NOW!).
- If it is a new expression, then we need to cost it (PARTIALLY DONE RIGHT NOW, but only for the root goal).

To do the last point, there are two types of notifications:
1. New partial physical. Notify (already done with the current job system) the direct goal we are implementing for. If it is an expression, cost it. Otherwise launch an implementation task for that goal.
2. When new costed expression for a goal, we check if it is locally the best. If yes, notify optimize goal tasks. 

Goal 5:
- Goal 1

Goal 1:
- Goal 2
    - Expr 7
- Expr 2
- Expr 4
- Goal 5




Note:
- Only groups merge. However they can trigger goal id merges. When group merge,
need to maintain all implement group tasks. Implement group task still keeps goal id
as an optimization.
- Goal id merge happens only when the groups related are merged. Then the equivalencies are inherited.
- Partial physical plans have materialized goals as children. This is convenient for the implementation
as we don't need to ingest them in the memo at that time. However it is a bit lame when we send them to the
cost model... Let's keep materialized goals for now. It might even be easier to extract the logical properties if we carry along the group id.
- We need to keep track of logical expression repr and physical expression repr to know what the duplictaes are when we have merged goals and groups. Nothing changes here. You may think we no longer need to track repr for physical expressions but we need it because of the dependency
Same logical expr belongs to two different group  -> group merges -> dependent goal merges -> might have new physical expr duplicate 