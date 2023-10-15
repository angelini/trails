use std::{io, net::AddrParseError, path::Path, sync::Arc};

use arrow::{datatypes::DataType, error::ArrowError};
use attributes::{Attribute, AttributeSchema, AttributeType, AttributeValues};
use buffer::Buffer;
use clap::{command, CommandFactory, Parser, Subcommand};
use client::{Client, ClientError};
use data::arrow_schema;
use datafusion::{
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    error::DataFusionError,
    prelude::SessionContext,
};
use reedline::{DefaultPrompt, DefaultPromptSegment, FileBackedHistory, Reedline, Signal};
use tonic::transport;
use web::start_server;
use writer::write_batch;

mod attributes;
mod buffer;
mod client;
mod data;
mod web;
mod writer;

#[derive(Debug)]
enum Error {
    AddrParse(AddrParseError),
    Arrow(ArrowError),
    Client(ClientError),
    DataFusion(DataFusionError),
    Io(io::Error),
    Tonic(transport::Error),
}

impl From<AddrParseError> for Error {
    fn from(value: AddrParseError) -> Self {
        Error::AddrParse(value)
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Error::Arrow(value)
    }
}

impl From<ClientError> for Error {
    fn from(value: ClientError) -> Self {
        Error::Client(value)
    }
}

impl From<DataFusionError> for Error {
    fn from(value: DataFusionError) -> Self {
        Error::DataFusion(value)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<transport::Error> for Error {
    fn from(value: transport::Error) -> Self {
        Error::Tonic(value)
    }
}

type Result<T> = std::result::Result<T, Error>;

fn gen_attributes(tag: &str, req: u64) -> AttributeValues {
    let mut values = AttributeValues::new();
    values.insert(0, Attribute::String(tag.to_string()));
    values.insert(1, Attribute::UInt64(req));
    values.insert(2, Attribute::Boolean(false));
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

fn example_schema() -> AttributeSchema {
    AttributeSchema::new(vec![
        ("tag", AttributeType::String),
        ("request", AttributeType::UInt64),
        ("error", AttributeType::Boolean),
    ])
}

fn example_buffer(schema: &AttributeSchema) -> Buffer {
    let mut buffer = Buffer::new(schema.clone());
    gen_trace(&mut buffer, 1, "first", 5);
    gen_trace(&mut buffer, 2, "second", 2);
    buffer
}

async fn add_views(ctx: &SessionContext, schema: &AttributeSchema) -> Result<()> {
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
                sum(e2.duration) AS duration
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

async fn exec_query(ctx: &SessionContext, query: &str) -> Result<()> {
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

async fn start_repl(path: &str, schema: AttributeSchema) -> Result<()> {
    let ctx = SessionContext::new();

    let listing_opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(vec![("hour".to_string(), DataType::Int64)])
        .with_collect_stat(true);

    ctx.register_listing_table(
        "_events",
        path,
        listing_opts,
        Some(Arc::new(arrow_schema(&schema))),
        None,
    )
    .await?;

    add_views(&ctx, &schema).await?;

    let history = Box::new(FileBackedHistory::with_file(100, "history.txt".into())?);

    let mut line_editor = Reedline::create().with_history(history);
    let prompt = DefaultPrompt::new(
        DefaultPromptSegment::Basic("".to_string()),
        DefaultPromptSegment::Empty,
    );

    loop {
        let sig = line_editor.read_line(&prompt);
        match sig {
            Ok(Signal::Success(query)) => {
                exec_query(&ctx, &query).await?;
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

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    //, arg_required_else_help(true)
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// SQL REPL connected to an example dataset
    Repl,

    /// Data collection server
    Server {
        #[arg(short, long)]
        path: String,
    },

    /// Test writes
    Write {
        #[arg(short, long)]
        idx: u16,
    },

    /// Write from a flight client
    Client {
        #[arg(short, long)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let schema = example_schema();

    if let Some(command) = cli.command {
        match command {
            Commands::Repl => {
                start_repl("/tmp/trails", schema).await?;
            }
            Commands::Server { path } => {
                start_server(&path, arrow_schema(&schema)).await?;
            }
            Commands::Write { idx } => {
                let batch = example_buffer(&schema).to_record_batch()?;
                write_batch(Path::new("/tmp/trails"), idx, batch).await?;
            }
            Commands::Client { port } => {
                let mut client = Client::new(port).await?;
                let buffer = example_buffer(&schema);
                client.put_buffer("example", buffer).await?;
            }
        }
    } else {
        Cli::command().print_help().unwrap();
    }

    Ok(())
}
