//! The main implementation of the in-memory memo table.

use super::{
    Infallible, Memo, MemoryMemo, MergeProducts, Representative, helpers::MemoryMemoHelper,
};
use crate::{cir::*, memo::memory::GroupInfo};
use hashbrown::{HashMap, HashSet};
use std::collections::VecDeque;

impl Memo for MemoryMemo {
    async fn debug_dump(&self) -> Result<(), Infallible> {
        println!("\n===== MEMO TABLE DUMP =====");
        println!("---- GROUPS ----");

        // Get all group IDs and sort them for consistent output.
        let mut group_ids: Vec<_> = self.group_info.keys().copied().collect();
        group_ids.sort_by_key(|id| id.0);

        for group_id in group_ids {
            println!("Group {:?}:", group_id);

            // Print logical properties.
            let group_info = self.group_info.get(&group_id).unwrap();
            println!("  Properties: {:?}", group_info.logical_properties);

            // Print expressions in group.
            println!("  Expressions:");
            for expr_id in &group_info.expressions {
                let expr = self.id_to_logical_expr.get(expr_id).unwrap();
                let repr_id = self.find_repr_logical_expr_id(*expr_id).await;
                println!("    {:?} [{:?}]: {:?}", expr_id, repr_id, expr);
            }

            // Print goals in this group (if any).
            if !group_info.goals.is_empty() {
                println!("  Goals:");
                for (props, goal_ids) in &group_info.goals {
                    for goal_id in goal_ids {
                        println!("    Goal {:?} with properties: {:?}", goal_id, props);
                    }
                }
            }
        }

        println!("---- GOALS ----");

        // Get all goal IDs and sort them.
        let mut goal_ids: Vec<_> = self.goal_info.keys().copied().collect();
        goal_ids.sort_by_key(|id| id.0);

        for goal_id in goal_ids {
            println!("Goal {:?}:", goal_id);

            // Print goal definition.
            let goal_info = self.goal_info.get(&goal_id).unwrap();
            println!("  Definition: {:?}", goal_info.goal);

            // Print members.
            println!("  Members:");
            for member in &goal_info.members {
                match member {
                    GoalMemberId::GoalId(sub_goal_id) => {
                        println!("    Sub-goal: {:?}", sub_goal_id);
                    }
                    GoalMemberId::PhysicalExpressionId(phys_id) => {
                        let expr = self.id_to_physical_expr.get(phys_id).unwrap();
                        println!("    Physical expr {:?}: {:?}", phys_id, expr);
                    }
                }
            }
        }

        println!("---- PHYSICAL EXPRESSIONS ----");

        // Get all physical expression IDs and sort them
        let mut phys_expr_ids: Vec<_> = self.id_to_physical_expr.keys().copied().collect();
        phys_expr_ids.sort_by_key(|id| id.0);

        for phys_id in phys_expr_ids {
            let expr = self.id_to_physical_expr.get(&phys_id).unwrap();
            println!("Physical expr {:?}: {:?}", phys_id, expr);
        }

        println!("===== END MEMO TABLE DUMP =====");

        Ok(())
    }

    async fn get_logical_properties(
        &self,
        group_id: GroupId,
    ) -> Result<LogicalProperties, Infallible> {
        let group_id = self.find_repr_group_id(group_id).await?;
        Ok(self
            .group_info
            .get(&group_id)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", group_id))
            .logical_properties
            .clone())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> Result<HashSet<LogicalExpressionId>, Infallible> {
        let group_id = self.find_repr_group_id(group_id).await?;
        Ok(self
            .group_info
            .get(&group_id)
            .unwrap_or_else(|| panic!("{:?} not found in memo table", group_id))
            .expressions
            .clone())
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> Result<Option<GroupId>, Infallible> {
        let repr_logical_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;
        Ok(self
            .logical_id_to_group_index
            .get(&repr_logical_expr_id)
            .copied())
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> Result<GroupId, Infallible> {
        let logical_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;

        if let Some(group_id) = self.logical_id_to_group_index.get(&logical_expr_id) {
            return Ok(*group_id);
        }

        let group_id = self.next_group_id();
        let group_info = GroupInfo {
            expressions: HashSet::from([logical_expr_id]),
            goals: HashMap::new(),
            logical_properties: props.clone(),
        };

        self.group_info.insert(group_id, group_info);
        self.logical_id_to_group_index
            .insert(logical_expr_id, group_id);
        Ok(group_id)
    }

    /// Merges equivalent groups in the memo structure.
    ///
    /// This function implements a critical operation in query optimization: merging equivalent
    /// expression groups within the memo structure. When two groups are found to be equivalent,
    /// this function creates a new representative group and handles all the cascading effects.
    ///
    /// # Algorithm
    ///
    /// Merging groups has complex cascading effects because expressions in the memo reference
    /// groups by their IDs:
    ///
    /// 1. When groups A and B are merged into a new group C, all expressions that reference
    ///    group A or B need to be updated to reference group C instead.
    ///
    /// 2. After this update, expressions that were previously distinct might become identical.
    ///    This happens when the only difference between expressions was that one referenced
    ///    group A and the other referenced group B.
    ///
    /// 3. When expressions become identical, their containing groups also need to be merged,
    ///    creating a cascading effect.
    ///
    /// 4. After all logical groups have been merged, we need to identify and merge all goals
    ///    that have become equivalent. Goals are considered equivalent when they reference the
    ///    same group and share identical logical properties.
    ///
    /// 5. When goals are merged, this triggers a cascading effect on physical expressions:
    /// - Physical expressions that reference merged goals need updating.
    /// - This may cause previously distinct physical expressions to become identical.
    /// - These newly identical expressions must then be merged.
    /// - Each merge may create more identical expressions, continuing the chain reaction
    ///   until the entire structure is consistent.
    ///
    /// To handle this complexity, we use an iterative approach:
    /// PHASE 1: LOGICAL
    /// - We maintain a queue of group pairs that need to be merged.
    /// - For each pair, we create a new representative group.
    /// - We update all expressions that reference the merged groups.
    /// - If this creates new equivalences, we add the affected groups to the merge queue.
    /// - We continue until no more merges are needed.
    ///
    /// PHASE 2: PHYSICAL
    /// - We inspect the result of logical merges, and identify the goals to merge in
    ///   the corresponding group_info of the new representative group.
    /// - For each group, we create a new representative goal.
    /// - We update all expressions that reference the merged goals.
    /// - If this creates new equivalences, we add the affected goals to the merge queue.
    /// - We continue until no more merges are needed.
    ///
    /// This approach ensures that all cascading effects are properly handled and the memo
    /// structure remains consistent after the merge.
    ///
    /// # Parameters
    /// * `group_id_1` - ID of the first group to merge.
    /// * `group_id_2` - ID of the second group to merge.
    ///
    /// # Returns
    /// Detailed results of all merges performed, including cascading merges.
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> Result<MergeProducts, Infallible> {
        let mut merge_operations = vec![];
        let mut pending_merges = VecDeque::from(vec![(group_id_1, group_id_2)]);

        while let Some((g1, g2)) = pending_merges.pop_front() {
            // Find current representatives - skip if already merged.
            let group_id_1 = self.find_repr_group_id(g1).await?;
            let group_id_2 = self.find_repr_group_id(g2).await?;
            if group_id_1 == group_id_2 {
                continue;
            }

            // Perform the group merge, creating a new representative.
            let (new_group_id, merge_product) =
                self.merge_group_pair(group_id_1, group_id_2).await?;
            merge_operations.push(merge_product);

            // Process expressions that reference the merged groups,
            // which may trigger additional group merges.
            let new_pending_merges = self
                .process_referencing_logical_exprs(group_id_1, group_id_2, new_group_id)
                .await?;

            pending_merges.extend(new_pending_merges);
        }

        // Consolidate the merge products by replacing the incremental merges
        // with consolidated results that show the full picture.
        let group_merges = self
            .consolidate_merge_group_products(merge_operations)
            .await?;

        // Now handle goal merges: we do not need to pass any extra parameters as
        // the goals to merge are gathered in the `goals` member of each new
        // representative group.
        let goal_merges = self.merge_dependent_goals(&group_merges).await?;

        // Finally, we need to recursively merge the physical expressions that are
        // dependent on the merged goals (and the recursively merged expressions themselves).
        let expr_merges = self.merge_dependent_physical_exprs(&goal_merges).await?;

        Ok(MergeProducts {
            group_merges,
            goal_merges,
            expr_merges,
        })
    }

    async fn get_all_goal_members(
        &self,
        goal_id: GoalId,
    ) -> Result<HashSet<GoalMemberId>, Infallible> {
        let repr_goal_id = self.find_repr_goal_id(goal_id).await?;

        let members = self
            .goal_info
            .get(&repr_goal_id)
            .expect("Goal not found in memo table")
            .members
            .iter()
            .map(|&member_id| self.find_repr_goal_member_id(member_id))
            .collect();

        Ok(members)
    }

    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member_id: GoalMemberId,
    ) -> Result<bool, Infallible> {
        // We call `get_all_goal_members` to ensure we only have the representative IDs
        // in the set. This is important because members may have been merged with another.
        let mut current_members = self.get_all_goal_members(goal_id).await?;

        let repr_member_id = self.find_repr_goal_member_id(member_id);
        let added = current_members.insert(repr_member_id);

        if added {
            let repr_goal_id = self.find_repr_goal_id(goal_id).await?;
            self.goal_info
                .get_mut(&repr_goal_id)
                .expect("Goal not found in memo table")
                .members
                .insert(repr_member_id);
        }

        Ok(added)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::cir::{Child, OperatorData};
    use crate::memo::Materialize;
    use crate::memo::fuzz::{FuzzData, Fuzzer};

    pub async fn lookup_or_insert(
        memo: &mut impl Memo,
        data: i64,
        children: Vec<GroupId>,
    ) -> GroupId {
        let expr = LogicalExpression {
            tag: data.to_string(),
            data: vec![OperatorData::Int64(data)],
            children: children.iter().map(|g| Child::Singleton(*g)).collect(),
        };

        let eid = memo.get_logical_expr_id(&expr).await.unwrap();
        match memo.find_logical_expr_group(eid).await.unwrap() {
            None => memo
                .create_group(eid, &LogicalProperties(None))
                .await
                .unwrap(),
            Some(g) => g,
        }
    }

    pub async fn insert_into_group(
        memo: &mut impl Memo,
        data: i64,
        children: Vec<GroupId>,
        gid: GroupId,
    ) -> GroupId {
        let g = lookup_or_insert(memo, data, children).await;
        memo.merge_groups(g, gid).await.unwrap();
        g
    }

    pub async fn retrieve(memo: &impl Memo, gid: GroupId) -> Vec<i64> {
        let eids = memo.get_all_logical_exprs(gid).await.unwrap();

        // Collect and sort data in expressions
        let mut data = vec![];
        for eid in eids {
            let expr = memo.materialize_logical_expr(eid).await.unwrap();
            if let OperatorData::Int64(v) = expr.data[0] {
                data.push(v);
            }
        }
        data.sort();
        data
    }

    #[tokio::test]
    async fn test_lookup_same() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 0, vec![]).await;

        assert_eq!(g0, g1);
    }

    #[tokio::test]
    async fn test_lookup_different() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;

        assert_ne!(g0, g1);
    }

    #[tokio::test]
    async fn test_add_to_group() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        insert_into_group(&mut memo, 1, vec![], g0).await;

        assert_eq!(retrieve(&memo, g0).await, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_merge() {
        let mut memo = MemoryMemo::default();

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        insert_into_group(&mut memo, 1, vec![], g0).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;
        insert_into_group(&mut memo, 3, vec![], g2).await;
        let g4 = insert_into_group(&mut memo, 0, vec![], g2).await;

        assert_eq!(retrieve(&memo, g4).await, vec![0, 1, 2, 3]);
    }

    async fn create_goal(
        memo: &mut MemoryMemo,
        group_id: GroupId,
        props: PhysicalProperties,
    ) -> GoalId {
        let goal = Goal(group_id, props);
        memo.get_goal_id(&goal).await.unwrap()
    }

    async fn create_physical_expr(
        memo: &mut MemoryMemo,
        tag: &str,
        children: Vec<GoalMemberId>,
    ) -> PhysicalExpressionId {
        let expr = PhysicalExpression {
            tag: tag.to_string(),
            data: vec![],
            children: children.into_iter().map(Child::Singleton).collect(),
        };
        memo.get_physical_expr_id(&expr).await.unwrap()
    }

    #[tokio::test]
    async fn test_recursive_merge() {
        let mut memo = MemoryMemo::default();

        // g0: 0(), 1()
        // g4: 4(g0), 5()
        // g2: 2(), 3(), 1()
        // g5: 4(g2), 6()

        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        insert_into_group(&mut memo, 1, vec![], g0).await;
        let g4 = lookup_or_insert(&mut memo, 4, vec![g0]).await;
        insert_into_group(&mut memo, 5, vec![], g4).await;

        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;
        insert_into_group(&mut memo, 3, vec![], g2).await;
        let g5 = lookup_or_insert(&mut memo, 4, vec![g2]).await;
        insert_into_group(&mut memo, 6, vec![], g5).await;

        let g0 = memo.find_repr_group_id(g0).await.unwrap();
        let g2 = memo.find_repr_group_id(g2).await.unwrap();
        let g4 = memo.find_repr_group_id(g4).await.unwrap();
        let g5 = memo.find_repr_group_id(g5).await.unwrap();

        // Try to merge g0 and g2
        let merge_result = memo.merge_groups(g0, g2).await.unwrap();

        // There should be exactly 2 merge records (one for base level, one for recursive)
        assert_eq!(merge_result.group_merges.len(), 2);

        // Find the base level merge (g0 + g2)
        let base_merge = merge_result
            .group_merges
            .iter()
            .find(|m| m.merged_groups.contains(&g0) && m.merged_groups.contains(&g2))
            .expect("Should have a merge record for g0 and g2");

        // Find the recursive merge (g4 + g5)
        let upper_merge = merge_result
            .group_merges
            .iter()
            .find(|m| m.merged_groups.contains(&g4) && m.merged_groups.contains(&g5))
            .expect("Should have a merge record for g4 and g5");

        // Get the new representative IDs
        let g6 = base_merge.new_group_id;
        let g7 = upper_merge.new_group_id;

        // Verify that each record has ONLY the current representatives that were merged
        let base_groups = &base_merge.merged_groups;
        assert!(base_groups.contains(&g0));
        assert!(base_groups.contains(&g2));
        assert_eq!(base_groups.len(), 2); // Only g0 and g2

        let upper_groups = &upper_merge.merged_groups;
        assert!(upper_groups.contains(&g4));
        assert!(upper_groups.contains(&g5));
        assert_eq!(upper_groups.len(), 2); // Only g4 and g5

        // Verify group content through direct queries
        assert_eq!(retrieve(&memo, g6).await, vec![0, 1, 2, 3]);
        assert_eq!(retrieve(&memo, g7).await, vec![4, 5, 6]);

        assert_eq!(retrieve(&memo, g6).await, vec![0, 1, 2, 3]);

        // Get merged upper group.
        let g7 = lookup_or_insert(&mut memo, 4, vec![g6]).await;

        assert_eq!(retrieve(&memo, g7).await, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_simple_merge_consolidation() {
        let mut memo = MemoryMemo::default();

        // Create three groups
        let g0 = lookup_or_insert(&mut memo, 0, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;

        // Merge g0 and g1
        let merge_result1 = memo.merge_groups(g0, g1).await.unwrap();
        assert_eq!(merge_result1.group_merges.len(), 1);

        let g3 = merge_result1.group_merges[0].new_group_id;

        // Verify the merged groups in the first result
        let merged_groups1 = &merge_result1.group_merges[0].merged_groups;
        assert!(merged_groups1.contains(&g0));
        assert!(merged_groups1.contains(&g1));
        assert_eq!(merged_groups1.len(), 2);

        // Merge g3 and g2
        let merge_result2 = memo.merge_groups(g3, g2).await.unwrap();
        assert_eq!(merge_result2.group_merges.len(), 1);

        // Get the new representative
        let g4 = merge_result2.group_merges[0].new_group_id;

        // Verify the merge result only includes the current representatives
        // that were merged (g3 and g2), not groups from previous merges
        let merged_groups2 = &merge_result2.group_merges[0].merged_groups;
        assert!(!merged_groups2.contains(&g0)); // Should not include already merged groups
        assert!(!merged_groups2.contains(&g1)); // Should not include already merged groups
        assert!(merged_groups2.contains(&g2)); // Current representative being merged
        assert!(merged_groups2.contains(&g3)); // Current representative being merged
        assert_eq!(merged_groups2.len(), 2); // Only g2 and g3

        // Verify the final group structure through direct queries
        assert_eq!(retrieve(&memo, g4).await, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_complex_merge_chain_consolidation() {
        let mut memo = MemoryMemo::default();

        // Create a complex chain of merges
        // First create 4 base groups
        let g0 = lookup_or_insert(&mut memo, 10, vec![]).await;
        let g1 = lookup_or_insert(&mut memo, 20, vec![]).await;
        let g2 = lookup_or_insert(&mut memo, 30, vec![]).await;
        let g3 = lookup_or_insert(&mut memo, 40, vec![]).await;

        // Merge g0+g1 -> g4
        let result1 = memo.merge_groups(g0, g1).await.unwrap();
        assert_eq!(result1.group_merges.len(), 1);
        assert_eq!(result1.group_merges[0].merged_groups.len(), 2);
        assert!(result1.group_merges[0].merged_groups.contains(&g0));
        assert!(result1.group_merges[0].merged_groups.contains(&g1));
        let g4 = result1.group_merges[0].new_group_id;

        // Merge g2+g3 -> g5
        let result2 = memo.merge_groups(g2, g3).await.unwrap();
        assert_eq!(result2.group_merges.len(), 1);
        assert_eq!(result2.group_merges[0].merged_groups.len(), 2);
        assert!(result2.group_merges[0].merged_groups.contains(&g2));
        assert!(result2.group_merges[0].merged_groups.contains(&g3));
        let g5 = result2.group_merges[0].new_group_id;

        // Now merge g4+g5
        let final_result = memo.merge_groups(g4, g5).await.unwrap();

        // Should have 1 merge record for only the current representatives
        assert_eq!(final_result.group_merges.len(), 1);

        // Get the final representative
        let g6 = final_result.group_merges[0].new_group_id;

        // Verify the merge result only includes the current representatives
        // that were merged (g4 and g5), not groups from previous merges
        let merged_groups = &final_result.group_merges[0].merged_groups;
        assert!(!merged_groups.contains(&g0)); // Should not include already merged groups
        assert!(!merged_groups.contains(&g1)); // Should not include already merged groups
        assert!(!merged_groups.contains(&g2)); // Should not include already merged groups
        assert!(!merged_groups.contains(&g3)); // Should not include already merged groups
        assert!(merged_groups.contains(&g4)); // Current representative being merged
        assert!(merged_groups.contains(&g5)); // Current representative being merged
        assert_eq!(merged_groups.len(), 2); // Only g4 and g5

        // Verify the final group structure contains all expressions through direct queries
        assert_eq!(retrieve(&memo, g6).await, vec![10, 20, 30, 40]);
    }

    #[tokio::test]
    async fn test_simple_cascade() {
        let mut memo = MemoryMemo::default();

        // Create leaf nodes
        let a = lookup_or_insert(&mut memo, 1, vec![]).await;
        let b = lookup_or_insert(&mut memo, 2, vec![]).await;

        // Create identical expressions that differ only in which leaf they reference
        let c = lookup_or_insert(&mut memo, 10, vec![a]).await;
        let d = lookup_or_insert(&mut memo, 10, vec![b]).await;

        // Create expressions that reference c and d
        let e = lookup_or_insert(&mut memo, 20, vec![c]).await;
        let f = lookup_or_insert(&mut memo, 20, vec![d]).await;

        // Verify initial state
        assert_ne!(c, d, "c and d should be different before merge");
        assert_ne!(e, f, "e and f should be different before merge");

        // Merge a and b
        let merge_result = memo.merge_groups(a, b).await.unwrap();

        // Should have exactly 3 merge records (a+b, c+d, e+f)
        assert_eq!(merge_result.group_merges.len(), 3, "Expected 3 merges");

        // Find the merge records
        let ab_merge = merge_result
            .group_merges
            .iter()
            .find(|m| m.merged_groups.contains(&a) && m.merged_groups.contains(&b))
            .expect("Should have a merge record for a and b");

        let cd_merge = merge_result
            .group_merges
            .iter()
            .find(|m| m.merged_groups.contains(&c) && m.merged_groups.contains(&d))
            .expect("Should have a merge record for c and d");

        let ef_merge = merge_result
            .group_merges
            .iter()
            .find(|m| m.merged_groups.contains(&e) && m.merged_groups.contains(&f))
            .expect("Should have a merge record for e and f");

        // Verify each merge has exactly 2 groups
        assert_eq!(ab_merge.merged_groups.len(), 2);
        assert_eq!(cd_merge.merged_groups.len(), 2);
        assert_eq!(ef_merge.merged_groups.len(), 2);

        // Verify final state
        let final_c = memo.find_repr_group_id(c).await.unwrap();
        let final_d = memo.find_repr_group_id(d).await.unwrap();
        let final_e = memo.find_repr_group_id(e).await.unwrap();
        let final_f = memo.find_repr_group_id(f).await.unwrap();

        assert_eq!(
            final_c, final_d,
            "c and d should have same representative after merge"
        );
        assert_eq!(
            final_e, final_f,
            "e and f should have same representative after merge"
        );
    }

    #[tokio::test]
    async fn test_duplicate_expr_after_merge() {
        let mut memo = MemoryMemo::default();

        // 1. Create first group with expressions 174, 175
        let g1 = lookup_or_insert(&mut memo, 174, vec![]).await;
        insert_into_group(&mut memo, 175, vec![], g1).await;

        // 2. Create second group with expressions 176, 177
        let g2 = lookup_or_insert(&mut memo, 176, vec![]).await;
        insert_into_group(&mut memo, 177, vec![], g2).await;

        // 3. Create third group with expressions 178, 179
        let g3 = lookup_or_insert(&mut memo, 178, vec![]).await;
        insert_into_group(&mut memo, 179, vec![], g3).await;

        // Verify initial state
        assert_eq!(retrieve(&memo, g1).await, vec![174, 175]);
        assert_eq!(retrieve(&memo, g2).await, vec![176, 177]);
        assert_eq!(retrieve(&memo, g3).await, vec![178, 179]);

        // 4. Merge first and second groups (174, 176)
        memo.merge_groups(g1, g2).await.unwrap();

        // Get the representative of the merged group
        let merged_g1_g2 = memo.find_repr_group_id(g1).await.unwrap();

        // Verify merged state
        let merged_exprs = retrieve(&memo, merged_g1_g2).await;
        assert_eq!(merged_exprs, vec![174, 175, 176, 177]);

        // 5. Merge third group with merged group (178, 174)
        memo.merge_groups(g3, merged_g1_g2).await.unwrap();

        // Find the final representative group
        let final_group = memo.find_repr_group_id(g3).await.unwrap();

        // Retrieve expressions in the final group
        let final_exprs = retrieve(&memo, final_group).await;

        // This is where the bug manifests - expression 178 appears twice
        // The assert will fail if the bug is present
        assert_eq!(
            final_exprs,
            vec![174, 175, 176, 177, 178, 179],
            "Merged group should contain exactly one copy of each expression"
        );
    }

    #[tokio::test]
    async fn test_logical_fuzzing() {
        let data = FuzzData::new(100, 10, true, 12345);
        let shuffled = data.shuffle(2, true);

        let memo = MemoryMemo::default();
        let mut fuzzer = Fuzzer::new(memo);
        fuzzer.add(&shuffled).await;
        fuzzer.check(&data).await;
    }

    #[tokio::test]
    async fn test_goal_merge() {
        let mut memo = MemoryMemo::default();

        // Create two groups.
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;

        // Create goals with the same properties.
        let props = PhysicalProperties(None);
        let goal1 = create_goal(&mut memo, g1, props.clone()).await;
        let goal2 = create_goal(&mut memo, g2, props).await;

        // Add a member to each goal.
        let p1 = create_physical_expr(&mut memo, "a", vec![]).await;
        let p2 = create_physical_expr(&mut memo, "b", vec![]).await;

        memo.add_goal_member(goal1, GoalMemberId::PhysicalExpressionId(p1))
            .await
            .unwrap();
        memo.add_goal_member(goal2, GoalMemberId::PhysicalExpressionId(p2))
            .await
            .unwrap();

        // Merge the groups.
        let merge_result = memo.merge_groups(g1, g2).await.unwrap();

        // Verify goal merges in the result.
        assert!(
            !merge_result.goal_merges.is_empty(),
            "Goal merges should not be empty"
        );

        // Get the new goal and verify members.
        let new_goal_id = merge_result.goal_merges[0].new_goal_id;
        let members = memo.get_all_goal_members(new_goal_id).await.unwrap();

        assert_eq!(members.len(), 2, "Merged goal should have two members");
        assert!(members.contains(&GoalMemberId::PhysicalExpressionId(p1)));
        assert!(members.contains(&GoalMemberId::PhysicalExpressionId(p2)));

        // Verify representatives.
        assert_eq!(memo.find_repr_goal_id(goal1).await.unwrap(), new_goal_id);
        assert_eq!(memo.find_repr_goal_id(goal2).await.unwrap(), new_goal_id);
    }

    #[tokio::test]
    async fn test_physical_expr_merge() {
        let mut memo = MemoryMemo::default();

        // Create two groups and goals.
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;

        let props = PhysicalProperties(None);
        let goal1 = create_goal(&mut memo, g1, props.clone()).await;
        let goal2 = create_goal(&mut memo, g2, props).await;

        // Create physical expressions referencing these goals.
        let p1 = create_physical_expr(&mut memo, "op", vec![GoalMemberId::GoalId(goal1)]).await;
        let p2 = create_physical_expr(&mut memo, "op", vec![GoalMemberId::GoalId(goal2)]).await;

        // Verify they're different before merge.
        assert_ne!(p1, p2);

        // Merge the groups.
        let merge_result = memo.merge_groups(g1, g2).await.unwrap();

        // Verify physical expression merges in the result.
        assert!(
            !merge_result.expr_merges.is_empty(),
            "Physical expr merges should not be empty"
        );

        // Get the new physical expression id.
        let new_expr_id = merge_result.expr_merges[0].new_physical_expr_id;

        // Verify representatives.
        assert_eq!(
            memo.find_repr_physical_expr_id(p1).await.unwrap(),
            new_expr_id
        );
        assert_eq!(
            memo.find_repr_physical_expr_id(p2).await.unwrap(),
            new_expr_id
        );
    }

    #[tokio::test]
    async fn test_recursive_physical_expr_merge() {
        let mut memo = MemoryMemo::default();

        // Create groups.
        let g1 = lookup_or_insert(&mut memo, 1, vec![]).await;
        let g2 = lookup_or_insert(&mut memo, 2, vec![]).await;

        // Create goals.
        let props = PhysicalProperties(None);
        let goal1 = create_goal(&mut memo, g1, props.clone()).await;
        let goal2 = create_goal(&mut memo, g2, props.clone()).await;

        // Create first level physical expressions.
        let p1 = create_physical_expr(&mut memo, "op1", vec![GoalMemberId::GoalId(goal1)]).await;
        let p2 = create_physical_expr(&mut memo, "op1", vec![GoalMemberId::GoalId(goal2)]).await;

        // Create second level physical expressions.
        let p3 = create_physical_expr(
            &mut memo,
            "op2",
            vec![GoalMemberId::PhysicalExpressionId(p1)],
        )
        .await;
        let p4 = create_physical_expr(
            &mut memo,
            "op2",
            vec![GoalMemberId::PhysicalExpressionId(p2)],
        )
        .await;

        // Merge the groups.
        let merge_result = memo.merge_groups(g1, g2).await.unwrap();

        // There should be multiple levels of physical expr merges.
        assert!(
            merge_result.expr_merges.len() >= 2,
            "Should have at least 2 levels of physical expr merges"
        );

        // Verify representatives for both levels.
        let p1_repr = memo.find_repr_physical_expr_id(p1).await.unwrap();
        let p2_repr = memo.find_repr_physical_expr_id(p2).await.unwrap();
        let p3_repr = memo.find_repr_physical_expr_id(p3).await.unwrap();
        let p4_repr = memo.find_repr_physical_expr_id(p4).await.unwrap();

        assert_eq!(
            p1_repr, p2_repr,
            "First level expressions should share representative"
        );
        assert_eq!(
            p3_repr, p4_repr,
            "Second level expressions should share representative"
        );
    }

    #[tokio::test]
    async fn test_merge_products_completeness() {
        let mut memo = MemoryMemo::default();

        // Create a three-level structure.
        let g1 = super::tests::lookup_or_insert(&mut memo, 1, vec![]).await;
        let g2 = super::tests::lookup_or_insert(&mut memo, 2, vec![]).await;

        let props = PhysicalProperties(None);
        let goal1 = create_goal(&mut memo, g1, props.clone()).await;
        let goal2 = create_goal(&mut memo, g2, props.clone()).await;

        // Level 1
        let p1 = create_physical_expr(&mut memo, "leaf1", vec![GoalMemberId::GoalId(goal1)]).await;
        let p2 = create_physical_expr(&mut memo, "leaf1", vec![GoalMemberId::GoalId(goal2)]).await;

        // Level 2
        let p3 = create_physical_expr(
            &mut memo,
            "mid1",
            vec![GoalMemberId::PhysicalExpressionId(p1)],
        )
        .await;
        let p4 = create_physical_expr(
            &mut memo,
            "mid1",
            vec![GoalMemberId::PhysicalExpressionId(p2)],
        )
        .await;

        // Level 3
        let p5 = create_physical_expr(
            &mut memo,
            "top1",
            vec![GoalMemberId::PhysicalExpressionId(p3)],
        )
        .await;
        let p6 = create_physical_expr(
            &mut memo,
            "top1",
            vec![GoalMemberId::PhysicalExpressionId(p4)],
        )
        .await;

        // Merge the groups.
        let merge_result = memo.merge_groups(g1, g2).await.unwrap();

        // Verify structure of the merge products.
        assert_eq!(
            merge_result.group_merges.len(),
            1,
            "Should have 1 group merge"
        );
        assert_eq!(
            merge_result.goal_merges.len(),
            1,
            "Should have 1 goal merge"
        );
        assert_eq!(
            merge_result.expr_merges.len(),
            3,
            "Should have 3 expression merges for the three levels"
        );

        // Verify all expressions share the same representatives at their respective levels.
        assert_eq!(
            memo.find_repr_physical_expr_id(p1).await.unwrap(),
            memo.find_repr_physical_expr_id(p2).await.unwrap(),
        );
        assert_eq!(
            memo.find_repr_physical_expr_id(p3).await.unwrap(),
            memo.find_repr_physical_expr_id(p4).await.unwrap(),
        );
        assert_eq!(
            memo.find_repr_physical_expr_id(p5).await.unwrap(),
            memo.find_repr_physical_expr_id(p6).await.unwrap(),
        );
    }
}
