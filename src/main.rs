use arrow::{error::ArrowError, record_batch::RecordBatch};
use attributes::{Attribute, AttributeSchema, AttributeType, AttributeValues};
use buffer::Buffer;
use datafusion::{error::DataFusionError, prelude::SessionContext};

mod attributes;
mod buffer;
mod data;
mod web;

fn gen_attributes(tag: &str) -> AttributeValues {
    let mut values = AttributeValues::new();
    values.insert(0, Attribute::String(tag.to_string()));
    values
}

fn example_batch() -> Result<RecordBatch, ArrowError> {
    let schema = AttributeSchema::new(vec![("tag", AttributeType::String)]);
    let mut buffer = Buffer::new(schema);

    let trace_id = Buffer::generate_trace_id();
    let span_id = buffer.start_span(trace_id, gen_attributes("s"));
    buffer.log(
        trace_id,
        span_id,
        "example: 1".to_string(),
        gen_attributes("m"),
    );
    buffer.log(
        trace_id,
        span_id,
        "example: 2".to_string(),
        gen_attributes("m"),
    );
    buffer.end_span(trace_id, span_id);

    buffer.to_record_batch()
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();

    let batch = example_batch()?;
    ctx.register_batch("example", batch)?;

    let df = ctx.sql("SELECT * FROM example").await?;
    df.show().await?;

    Ok(())
}
