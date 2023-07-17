use arrow::{error::ArrowError, record_batch::RecordBatch};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    attributes::{AttributeSchema, AttributeValues},
    data::{to_record_batch, Event, SpanId, TraceId},
};

pub struct Buffer {
    schema: AttributeSchema,
    events: Vec<Event>,
}

impl Buffer {
    pub fn new(schema: AttributeSchema) -> Self {
        Buffer {
            schema,
            events: Vec::new(),
        }
    }

    pub fn generate_trace_id() -> TraceId {
        Uuid::now_v7()
    }

    pub fn start_span(&mut self, trace_id: TraceId, attributes: AttributeValues) -> SpanId {
        let event = Event::span_start(trace_id, attributes);
        let span_id = event.span_id;
        self.events.push(event);
        span_id
    }

    pub fn end_span(&mut self, trace_id: TraceId, span_id: SpanId) {
        self.events.push(Event::span_end(
            trace_id,
            span_id,
            self.span_start_time(span_id),
        ));
    }

    pub fn log(
        &mut self,
        trace_id: TraceId,
        span_id: SpanId,
        message: String,
        attributes: AttributeValues,
    ) {
        self.events
            .push(Event::log(trace_id, span_id, message, attributes));
    }

    pub fn to_record_batch(self) -> Result<RecordBatch, ArrowError> {
        to_record_batch(&self.schema, self.events)
    }

    fn span_start_time(&self, span_id: SpanId) -> Option<DateTime<Utc>> {
        for event in self.events.iter() {
            if event.span_id == span_id {
                return Some(event.time);
            }
        }
        None
    }
}
