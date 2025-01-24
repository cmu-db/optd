// @generated automatically by Diesel CLI.

diesel::table! {
    logical_exprs (id) {
        id -> BigInt,
        logical_op_kind_id -> BigInt,
        group_id -> BigInt,
        created_at -> Timestamp,
    }
}

diesel::table! {
    logical_filters (logical_expr_id) {
        logical_expr_id -> BigInt,
        child -> BigInt,
        predicate -> Text,
    }
}

diesel::table! {
    logical_joins (logical_expr_id) {
        logical_expr_id -> BigInt,
        join_type -> Integer,
        left -> BigInt,
        right -> BigInt,
        join_cond -> Text,
    }
}

diesel::table! {
    logical_op_kinds (id) {
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
        physical_op_kind_id -> BigInt,
        group_id -> BigInt,
        total_cost -> Double,
        created_at -> Timestamp,
    }
}

diesel::table! {
    physical_filters (physical_expr_id) {
        physical_expr_id -> BigInt,
        child -> BigInt,
        predicate -> Text,
    }
}

diesel::table! {
    physical_nljoins (physical_expr_id) {
        physical_expr_id -> BigInt,
        join_type -> Integer,
        left -> BigInt,
        right -> BigInt,
        join_cond -> Text,
    }
}

diesel::table! {
    physical_op_kinds (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::table! {
    physical_table_scans (physical_expr_id) {
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

diesel::joinable!(logical_exprs -> logical_op_kinds (logical_op_kind_id));
diesel::joinable!(logical_exprs -> rel_groups (group_id));
diesel::joinable!(logical_filters -> logical_exprs (logical_expr_id));
diesel::joinable!(logical_filters -> rel_groups (child));
diesel::joinable!(logical_joins -> logical_exprs (logical_expr_id));
diesel::joinable!(logical_scans -> logical_exprs (logical_expr_id));
diesel::joinable!(physical_exprs -> physical_op_kinds (physical_op_kind_id));
diesel::joinable!(physical_exprs -> rel_groups (group_id));
diesel::joinable!(physical_filters -> physical_exprs (physical_expr_id));
diesel::joinable!(physical_filters -> rel_groups (child));

diesel::allow_tables_to_appear_in_same_query!(
    logical_exprs,
    logical_filters,
    logical_joins,
    logical_op_kinds,
    logical_scans,
    physical_exprs,
    physical_filters,
    physical_nljoins,
    physical_op_kinds,
    physical_table_scans,
    rel_groups,
);
