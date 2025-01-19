// @generated automatically by Diesel CLI.

diesel::table! {
    logical_exprs (id) {
        id -> BigInt,
        typ_desc -> BigInt,
        group_id -> BigInt,
        created_at -> Timestamp,
    }
}

diesel::table! {
    logical_props (id) {
        id -> BigInt,
        group_id -> BigInt,
        card_est -> BigInt,
    }
}

diesel::table! {
    logical_typ_descs (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::table! {
    physical_exprs (id) {
        id -> BigInt,
        typ_desc -> BigInt,
        group_id -> BigInt,
        derived_phys_prop_id -> BigInt,
        cost -> Double,
        created_at -> Timestamp,
    }
}

diesel::table! {
    physical_props (id) {
        id -> BigInt,
        payload -> Binary,
    }
}

diesel::table! {
    physical_typ_descs (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::table! {
    rel_group_winners (group_id, required_phys_prop_id) {
        group_id -> BigInt,
        required_phys_prop_id -> BigInt,
        physical_expr_id -> BigInt,
    }
}

diesel::table! {
    rel_groups (id) {
        id -> BigInt,
        status -> Integer,
        created_at -> Timestamp,
        rep_id -> Nullable<BigInt>,
    }
}

diesel::table! {
    scalar_exprs (id) {
        id -> BigInt,
        typ_desc -> BigInt,
        group_id -> BigInt,
        created_at -> Timestamp,
        cost -> Nullable<Double>,
    }
}

diesel::table! {
    scalar_group_winners (group_id) {
        group_id -> BigInt,
        scalar_expr_id -> BigInt,
    }
}

diesel::table! {
    scalar_groups (id) {
        id -> BigInt,
        status -> Integer,
        created_at -> Timestamp,
        rep_id -> Nullable<BigInt>,
    }
}

diesel::table! {
    scalar_props (id) {
        id -> BigInt,
        payload -> Binary,
    }
}

diesel::table! {
    scalar_typ_descs (id) {
        id -> BigInt,
        name -> Text,
    }
}

diesel::joinable!(logical_exprs -> logical_typ_descs (typ_desc));
diesel::joinable!(logical_exprs -> rel_groups (group_id));
diesel::joinable!(logical_props -> rel_groups (group_id));
diesel::joinable!(physical_exprs -> physical_props (derived_phys_prop_id));
diesel::joinable!(physical_exprs -> physical_typ_descs (typ_desc));
diesel::joinable!(physical_exprs -> rel_groups (group_id));
diesel::joinable!(rel_group_winners -> physical_exprs (physical_expr_id));
diesel::joinable!(rel_group_winners -> physical_props (required_phys_prop_id));
diesel::joinable!(rel_group_winners -> rel_groups (group_id));
diesel::joinable!(scalar_exprs -> scalar_groups (group_id));
diesel::joinable!(scalar_exprs -> scalar_typ_descs (typ_desc));
diesel::joinable!(scalar_group_winners -> scalar_exprs (scalar_expr_id));
diesel::joinable!(scalar_group_winners -> scalar_groups (group_id));
diesel::joinable!(scalar_groups -> rel_groups (rep_id));

diesel::allow_tables_to_appear_in_same_query!(
    logical_exprs,
    logical_props,
    logical_typ_descs,
    physical_exprs,
    physical_props,
    physical_typ_descs,
    rel_group_winners,
    rel_groups,
    scalar_exprs,
    scalar_group_winners,
    scalar_groups,
    scalar_props,
    scalar_typ_descs,
);
