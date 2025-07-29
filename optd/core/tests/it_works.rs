use std::sync::Arc;

use optd_core::ir::{
    Column,
    catalog::DataSourceId,
    convert::{IntoOperator, IntoScalar},
    operator::LogicalGet,
    scalar::*,
};

#[test]
fn it_works() {
    let op = LogicalGet::new(
        DataSourceId(1),
        ProjectionList::new(Arc::new([
            Assign::new(Column(0), ColumnRef::new(Column(0)).into_scalar()).into_scalar(),
            Assign::new(Column(1), ColumnRef::new(Column(1)).into_scalar()).into_scalar(),
        ]))
        .into_scalar(),
    )
    .into_operator();
    assert_eq!(op.input_operators().len(), 0);
    assert_eq!(op.input_scalars().len(), 1);
}
