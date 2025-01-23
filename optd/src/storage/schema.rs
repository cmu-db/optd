// @generated automatically by Diesel CLI.

diesel::table! {
    logical_exprs (id) {
        id -> BigInt,
        logical_op_desc_id -> Integer,
        group_id -> BigInt,
        created_at -> Timestamp,
    }
}

diesel::table! {
    logical_joins (logical_expr_id) {
        logical_expr_id -> BigInt,
        left -> Integer,
        right -> Integer,
    }
}

diesel::table! {
    logical_op_descs (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::table! {
    logical_scans (logical_expr_id) {
        logical_expr_id -> BigInt,
        table_name -> Text,
    }
}

diesel::table! {
    physical_exprs (id) {
        id -> BigInt,
        physical_op_desc_id -> Integer,
        group_id -> BigInt,
        total_cost -> Double,
        created_at -> Timestamp,
    }
}

diesel::table! {
    physical_nested_loop_joins (physical_expr_id) {
        physical_expr_id -> BigInt,
        left -> Integer,
        right -> Integer,
    }
}

diesel::table! {
    physical_op_descs (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::table! {
    physical_scans (physical_expr_id) {
        physical_expr_id -> BigInt,
        table_name -> Text,
    }
}

diesel::table! {
    rel_groups (id) {
        id -> BigInt,
        created_at -> Timestamp,
    }
}

diesel::joinable!(logical_exprs -> logical_op_descs (logical_op_desc_id));
diesel::joinable!(logical_exprs -> rel_groups (group_id));
diesel::joinable!(logical_joins -> logical_exprs (logical_expr_id));
diesel::joinable!(logical_scans -> logical_exprs (logical_expr_id));
diesel::joinable!(physical_exprs -> physical_op_descs (physical_op_desc_id));
diesel::joinable!(physical_exprs -> rel_groups (group_id));

diesel::allow_tables_to_appear_in_same_query!(
    logical_exprs,
    logical_joins,
    logical_op_descs,
    logical_scans,
    physical_exprs,
    physical_nested_loop_joins,
    physical_op_descs,
    physical_scans,
    rel_groups,
);
