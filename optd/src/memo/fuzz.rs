use crate::cir::{Child, GroupId, LogicalExpression, LogicalProperties, OperatorData};
use crate::memo::Memo;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::StdRng;
use std::cmp::max;
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub(crate) struct FuzzExpression {
    pub op: usize,
    pub children: Vec<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct FuzzGroup {
    pub exprs: Vec<usize>,
    pub id: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct FuzzData {
    pub exprs: Vec<FuzzExpression>,
    pub groups: Vec<FuzzGroup>,
    pub entry: usize,
}

impl FuzzData {
    pub fn new(ngroups: usize, nexprs: usize, dag: bool, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);

        // FIXME: MAGIC NUMBERS
        let weights = [10, 30, 30]; // distribution operator arity
        let proximity = 4; // proximity factor (1 for no proximity preference)

        let mut memo = FuzzData {
            exprs: Vec::new(),
            groups: Vec::new(),
            entry: 0,
        };

        let mut tot = 0;
        let mut cnt = 0;
        for (i, v) in weights.iter().enumerate() {
            tot += i * v;
            cnt += v;
        }
        let dist = WeightedIndex::new(&weights).unwrap();

        // Generate groups
        let mut gqueue: Vec<usize> = vec![];
        while memo.groups.len() < ngroups || gqueue.len() > 1 {
            let mut exprs: Vec<usize> = vec![];

            let ngen = rng.gen_range(0..nexprs * 2);

            // Generate expressions even if no groups to reference (will be a scan!)
            while exprs.len() < ngen {
                let arity = dist.sample(&mut rng);
                let mut children: Vec<usize> = vec![];
                let mut cset: HashSet<usize> = HashSet::new();
                for i in 0..arity {
                    if gqueue.len() > 0 {
                        let idx = rng.gen_range(0..gqueue.len());
                        // avoid using the same group twice
                        // as an operand to the same expression
                        let c = gqueue[idx];
                        if !cset.contains(&c) {
                            cset.insert(c);
                            children.push(gqueue[idx]);
                            gqueue.remove(idx);
                        }
                    }
                    if children.len() <= i {
                        // failed to find a group, make one extra 'scan' node now
                        children.push(memo.groups.len());
                        memo.groups.push(FuzzGroup {
                            exprs: vec![memo.exprs.len()],
                            id: memo.groups.len(),
                        });
                        memo.exprs.push(FuzzExpression {
                            op: 0,
                            children: vec![],
                        });
                    }
                }
                let expr_id = memo.exprs.len();
                exprs.push(expr_id);

                let op = children.len(); // FIXME: more ops?
                memo.exprs.push(FuzzExpression { op, children });
            }

            if ngen > 0 {
                let group_id = memo.groups.len();
                memo.groups.push(FuzzGroup {
                    exprs,
                    id: group_id,
                });

                // While we don't have enough groups, collect operands for future expressions
                if group_id < ngroups && dag {
                    // replenish groups to be referenced
                    let ng = match nexprs * tot / cnt {
                        d if d > 0 => rng.gen_range(0..d * 2),
                        _ => 0,
                    };

                    let m = max(0, (group_id as i32) - (ngroups as i32) / proximity) as usize;
                    for _ in 0..ng {
                        gqueue.push(rng.gen_range(m..group_id + 1));
                    }
                }

                // Add at least this group, so that it is referenced or the last
                gqueue.push(group_id);
            }
        }

        memo.entry = gqueue[0];

        memo
    }

    pub fn shuffle(&self, chunk: usize, merge: bool) -> FuzzData {
        assert!(chunk > 1);

        let mut groups = self.groups.clone();
        let mut i = 0;

        while i < groups.len() {
            let g = &groups[i];
            let gid = g.id;
            if g.exprs.len() > chunk {
                let upd = g.exprs[..chunk].to_vec();
                let rest = if merge {
                    // trigger merge 1,2,3,4,5,6,7 --> 1,2,3 + 4,5,6 + 7,1,4
                    let mut v = g.exprs[chunk..].to_vec();
                    v.push(g.exprs[0]);
                    v
                } else {
                    // don't trigger merge 1,2,3,4,5,6,7 --> 1,2,3 - 3,4,5, - 5,6,7
                    g.exprs[chunk - 1..].to_vec()
                };
                groups[i].exprs = upd;
                groups.push(FuzzGroup {
                    exprs: rest,
                    id: gid,
                })
            }
            i = i + 1;
        }

        FuzzData {
            exprs: self.exprs.clone(),
            groups,
            entry: self.entry,
        }
    }
}

pub struct Fuzzer<M: Memo> {
    memo: M,
    group_ids: Vec<GroupId>,
    entry: GroupId,
}

impl<M: Memo> Fuzzer<M> {
    pub fn new(memo: M) -> Self {
        Self {
            memo,
            group_ids: Vec::new(),
            entry: GroupId(0),
        }
    }

    pub async fn add(&mut self, memo: &FuzzData) {
        for g in memo.groups.iter() {
            let mut group_id = None;

            for j in g.exprs.iter() {
                let e = &memo.exprs[*j];

                let expr = match e.op {
                    0 => LogicalExpression {
                        tag: "Scan".to_string(),
                        data: vec![OperatorData::Int64(*j as i64)],
                        children: vec![],
                    },
                    1 => LogicalExpression {
                        tag: "Filter".to_string(),
                        data: vec![OperatorData::Int64(*j as i64)],
                        children: vec![Child::Singleton(self.group_ids[e.children[0]])],
                    },
                    2 => LogicalExpression {
                        tag: "Filter".to_string(),
                        data: vec![OperatorData::Int64(*j as i64)],
                        children: vec![
                            Child::Singleton(self.group_ids[e.children[0]]),
                            Child::Singleton(self.group_ids[e.children[1]]),
                        ],
                    },
                    _ => unreachable!(),
                };

                let eid = self.memo.get_logical_expr_id(&expr).await.unwrap();
                let gid = match self.memo.find_logical_expr_group(eid).await.unwrap() {
                    None => {
                        // new expression, create a (temporary) group
                        self.memo
                            .create_group(eid, &LogicalProperties(None))
                            .await
                            .unwrap()
                    }
                    Some(id) => {
                        // expression already existed, just use its group
                        id
                    }
                };

                if let Some(id) = group_id {
                    // merge with known equivalent expressions
                    self.memo.merge_groups(gid, id).await.unwrap();
                }
                group_id = Some(gid);
            }
            if g.id >= self.group_ids.len() {
                self.group_ids.push(group_id.unwrap());
            } else {
                self.group_ids[g.id] = group_id.unwrap();
            }
        }

        self.entry = self.group_ids[memo.entry];
    }

    pub async fn check(&mut self, memo: &FuzzData) {
        let mut _tot = 0;
        for g in 0..memo.groups.len() {
            let group_expressions = self
                .memo
                .get_all_logical_exprs(self.group_ids[g])
                .await
                .unwrap();

            // do something with it
            let mut ids = vec![];
            for eid in group_expressions {
                let expr = self.memo.materialize_logical_expr(eid).await.unwrap();
                if let OperatorData::Int64(v) = expr.data[0] {
                    ids.push(v as usize);
                }
            }

            ids.sort();
            assert_eq!(ids, memo.groups[g].exprs, "incorrect memo")
        }
    }
}
