use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time,
};

use arrow::{datatypes, record_batch};
use uuid::Uuid;

pub type FieldKey = usize;

pub enum FieldType {
    Boolean,
    Int64,
    String,
}

impl FieldType {
    fn to_arrow(&self) -> datatypes::DataType {
        match self {
            FieldType::Boolean => datatypes::DataType::Boolean,
            FieldType::Int64 => datatypes::DataType::Int64,
            FieldType::String => datatypes::DataType::Utf8,
        }
    }
}

pub struct FieldDefinition {
    name: String,
    type_: FieldType,
}

pub type FieldDefinitions = HashMap<FieldKey, FieldDefinition>;

pub type FieldValues = HashMap<FieldKey, bytes::Bytes>;

pub struct Context {
    fields: FieldDefinitions,
}

impl Context {
    fn new(fields: FieldDefinitions) -> Self {
        Self { fields }
    }
}

pub type TraceId = Uuid;

pub struct Trace {
    id: TraceId,
}

pub type SpanId = u64;

pub struct Span {
    trace_id: TraceId,
    id: SpanId,
    start_time: time::SystemTime,
    end_time: Option<time::SystemTime>,

    attributes: FieldValues,
}

pub struct Event {
    trace_id: TraceId,
    span_id: SpanId,
    time: time::SystemTime,

    attributes: FieldValues,
}

pub struct Buffer {
    keys: HashSet<FieldKey>,
    spans: Vec<Span>,
    events: Vec<Event>,
}

impl Buffer {
    fn new() -> Self {
        Self {
            keys: HashSet::new(),
            spans: Vec::new(),
            events: Vec::new(),
        }
    }

    fn start_span(&mut self, trace_id: TraceId, attributes: FieldValues) -> SpanId {
        for key in attributes.keys() {
            self.keys.insert(*key);
        }

        let id = rand::random::<u64>();
        self.spans.push(Span {
            trace_id,
            id,
            start_time: time::SystemTime::now(),
            end_time: None,
            attributes,
        });
        id
    }

    fn end_span(&mut self, trace_id: TraceId, span_id: SpanId) {
        let mut span_idx = 0;
        for (idx, span) in self.spans.iter().enumerate().rev() {
            if span.trace_id == trace_id && span.id == span_id {
                span_idx = idx;
                break;
            }
        }
        self.spans[span_idx].end_time = Some(time::SystemTime::now())
    }

    fn append_event(&mut self, trace_id: TraceId, span_id: SpanId, attributes: FieldValues) {
        for key in attributes.keys() {
            self.keys.insert(*key);
        }

        self.events.push(Event {
            trace_id,
            span_id,
            time: time::SystemTime::now(),
            attributes,
        })
    }
}

fn define_fields<S: Into<String>>(
    definitions: Vec<(S, FieldType)>,
    offset: usize,
) -> FieldDefinitions {
    let mut key = offset;
    let mut map = HashMap::with_capacity(definitions.len());
    for (name, type_) in definitions {
        map.insert(
            key,
            FieldDefinition {
                name: name.into(),
                type_,
            },
        );
        key += 1;
    }
    map
}

pub fn init() -> (Context, Buffer) {
    (
        Context::new(define_fields(
            vec![("name", FieldType::String), ("value", FieldType::Int64)],
            0,
        )),
        Buffer::new(),
    )
}

fn build_arrays(ctx: &Context, buffer: &Buffer) -> record_batch::RecordBatch {
    let mut keys = buffer.keys.iter().copied().collect::<Vec<FieldKey>>();
    keys.sort();

    let fields = keys
        .iter()
        .map(|key| {
            let field = &ctx.fields[key];
            datatypes::Field::new(field.name.clone(), field.type_.to_arrow(), false)
        })
        .collect::<datatypes::Fields>();

    let schema = datatypes::Schema::new(fields);

    record_batch::RecordBatch::try_new(Arc::new(schema), vec![]).unwrap()
}
