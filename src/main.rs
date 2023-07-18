use arrow::{error::ArrowError, record_batch::RecordBatch};
use attributes::{Attribute, AttributeSchema, AttributeType, AttributeValues};
use buffer::Buffer;
use datafusion::{error::DataFusionError, prelude::SessionContext};
use reedline::{DefaultPrompt, DefaultPromptSegment, FileBackedHistory, Reedline, Signal};

mod attributes;
mod buffer;
mod data;
mod web;

fn gen_attributes(tag: &str, req: u64) -> AttributeValues {
    let mut values = AttributeValues::new();
    values.insert(0, Attribute::String(tag.to_string()));
    values.insert(1, Attribute::UInt64(req));
    values
}

fn gen_trace(buffer: &mut Buffer, req: u64, prefix: &str, span_count: usize) {
    let trace_id = Buffer::generate_trace_id();
    for _ in 0..span_count {
        let span_id = buffer.start_span(trace_id, gen_attributes("s", req));
        for message_idx in 0..3 {
            buffer.log(
                trace_id,
                span_id,
                format!("{}: {}", prefix, message_idx),
                gen_attributes("m", req),
            );
        }
        buffer.end_span(trace_id, span_id);
    }
}

fn example_batch() -> Result<(AttributeSchema, RecordBatch), ArrowError> {
    let schema = AttributeSchema::new(vec![
        ("tag", AttributeType::String),
        ("request", AttributeType::UInt64),
    ]);
    let mut buffer = Buffer::new(schema.clone());

    gen_trace(&mut buffer, 1, "first", 5);
    gen_trace(&mut buffer, 2, "second", 2);

    Ok((schema, buffer.to_record_batch()?))
}

async fn add_views(ctx: &SessionContext, schema: &AttributeSchema) -> Result<(), DataFusionError> {
    let attr_columns = schema
        .attrs
        .iter()
        .map(|attr| format!("e1.{}", attr.name))
        .collect::<Vec<String>>()
        .join(", ");

    ctx.sql(&format!(
        "
        CREATE VIEW events AS
            SELECT
                time,
                type,
                trace_id,
                span_id,
                message,
                duration,
                {attr_columns}
            FROM
                _events e1
            ORDER BY
                time
        "
    ))
    .await?;

    ctx.sql(&format!(
        "
        CREATE VIEW spans AS
            SELECT
                e1.time AS start_time,
                e2.time AS end_time,
                e1.trace_id,
                e1.span_id,
                e2.duration,
                {attr_columns}
            FROM
                _events e1
                JOIN _events e2
                ON e1.span_id = e2.span_id
            WHERE
                e1.type = 0
                AND e2.type = 1
            ORDER BY
                e1.time
        "
    ))
    .await?;

    ctx.sql(&format!(
        "
        CREATE VIEW traces AS
            SELECT
                min(e1.time) AS start_time,
                max(e2.time) AS end_time,
                e1.trace_id,
                count(1) AS spans,
                arrow_cast(sum(arrow_cast(e2.duration, 'Int64')), 'Duration(Nanosecond)') AS duration
            FROM
                _events e1
                JOIN _events e2
                ON e1.span_id = e2.span_id
            WHERE
                e1.type = 0
                AND e2.type = 1
            GROUP BY
                e1.trace_id
            ORDER BY
                e1.trace_id
        "
    ))
    .await?;

    ctx.sql(&format!(
        "
        CREATE VIEW logs AS
            SELECT
                time,
                trace_id,
                span_id,
                message,
                {attr_columns}
            FROM
                _events e1
            WHERE
                type = 2
            ORDER BY
                time
        "
    ))
    .await?;

    Ok(())
}

async fn exec_query(ctx: &SessionContext, query: &str) -> Result<(), DataFusionError> {
    match ctx.sql(&query).await {
        Ok(df) => {
            df.show().await?;
            println!("");
        }
        Err(err) => {
            println!("Error: {:?}", err);
            println!("");
        }
    }
    Ok(())
}

async fn start_cli(ctx: &SessionContext) -> Result<(), DataFusionError> {
    let history = Box::new(
        FileBackedHistory::with_file(100, "history.txt".into())
            .expect("Error configuring history with file"),
    );

    let mut line_editor = Reedline::create().with_history(history);
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("".to_string()),
        DefaultPromptSegment::Empty,
    );

    loop {
        let sig = line_editor.read_line(&prompt);
        match sig {
            Ok(Signal::Success(query)) => {
                exec_query(ctx, &query).await?;
            }
            Ok(Signal::CtrlD) | Ok(Signal::CtrlC) => {
                println!("Exit.");
                break;
            }
            unknown => {
                println!("Event: {:?}", unknown);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();

    let (schema, batch) = example_batch()?;
    ctx.register_batch("_events", batch)?;

    add_views(&ctx, &schema).await?;
    start_cli(&ctx).await?;

    Ok(())
}
