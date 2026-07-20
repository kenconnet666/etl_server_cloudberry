//! Runtime adapters connecting the replication loop to PostgreSQL and Cloudberry.

mod cloudberry;
mod pgoutput;

pub use cloudberry::{
    AdapterConfigError, CloudberryTransactionSink, DdlScope, TableBinding, TableBindingRegistry,
    build_apply_request, build_apply_request_scoped,
};
pub use pgoutput::PgOutputTransactionSource;
