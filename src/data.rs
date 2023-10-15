use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Int64Builder,
        StringBuilder, TimestampNanosecondBuilder, UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::attributes::{Attribute, AttributeSchema, AttributeType, AttributeValues};

pub type TraceId = Uuid;

pub type SpanId = u64;

pub struct SpanStartInfo {}

pub struct SpanEndInfo {
    duration: Duration,
}

pub struct LogInfo {
    message: String,
}

pub enum EventType {
    SpanStart(SpanStartInfo),
    SpanEnd(SpanEndInfo),
    Log(LogInfo),
}

pub struct Event {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub time: DateTime<Utc>,
    pub event_type: EventType,
    pub attributes: AttributeValues,
}

impl Event {
    pub fn span_start(trace_id: TraceId, attributes: AttributeValues) -> Self {
        Event {
            trace_id,
            span_id: rand::random::<u64>(),
            time: Utc::now(),
            event_type: EventType::SpanStart(SpanStartInfo {}),
            attributes,
        }
    }

    pub fn span_end(trace_id: TraceId, span_id: SpanId, start_time: Option<DateTime<Utc>>) -> Self {
        let end_time = Utc::now();
        let duration = match start_time {
            Some(start_time) => end_time - start_time,
            None => Duration::nanoseconds(0),
        };
        Event {
            trace_id,
            span_id,
            time: end_time,
            event_type: EventType::SpanEnd(SpanEndInfo { duration }),
            attributes: AttributeValues::new(),
        }
    }

    pub fn log(
        trace_id: TraceId,
        span_id: SpanId,
        message: String,
        attributes: AttributeValues,
    ) -> Self {
        Event {
            trace_id,
            span_id,
            time: Utc::now(),
            event_type: EventType::Log(LogInfo { message }),
            attributes,
        }
    }

    fn type_byte(&self) -> u8 {
        match self.event_type {
            EventType::SpanStart(_) => 0,
            EventType::SpanEnd(_) => 1,
            EventType::Log(_) => 2,
        }
    }
}

pub fn arrow_schema(schema: &AttributeSchema) -> Schema {
    let mut fields = vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::UInt64, false),
        // Datafusion's Timestamp type is time zone naive
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("type", DataType::UInt8, false),
        Field::new("duration", DataType::Int64, true),
        Field::new("message", DataType::Utf8, true),
    ];

    fields.extend(schema.arrow_fields());

    Schema::new(fields)
}

pub fn to_record_batch(
    schema: &AttributeSchema,
    events: Vec<Event>,
) -> Result<RecordBatch, ArrowError> {
    let size = events.len();
    let mut attribute_arrays: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(schema.attrs.len());

    for attribute in schema.attrs.iter() {
        match attribute.data_type {
            AttributeType::Boolean => {
                attribute_arrays.push(Box::new(BooleanBuilder::with_capacity(size)))
            }
            AttributeType::UInt64 => {
                attribute_arrays.push(Box::new(UInt64Builder::with_capacity(size)))
            }
            AttributeType::String => {
                attribute_arrays.push(Box::new(StringBuilder::with_capacity(size, size)))
            }
        }
    }

    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(size, 16);
    let mut span_id_builder = UInt64Builder::with_capacity(size);
    let mut time_builder = TimestampNanosecondBuilder::with_capacity(size);
    let mut type_builder = UInt8Builder::with_capacity(size);

    let mut duration_builder = Int64Builder::with_capacity(size);
    let mut message_builder = StringBuilder::with_capacity(size, size);

    for mut event in events {
        trace_id_builder
            .append_value(event.trace_id.as_bytes())
            .unwrap();
        span_id_builder.append_value(event.span_id);
        time_builder.append_value(event.time.timestamp_nanos());
        type_builder.append_value(event.type_byte());

        match event.event_type {
            EventType::SpanStart(_) => {
                duration_builder.append_null();
                message_builder.append_null();
            }
            EventType::SpanEnd(info) => {
                duration_builder.append_value(info.duration.num_nanoseconds().unwrap());
                message_builder.append_null();
            }
            EventType::Log(info) => {
                duration_builder.append_null();
                message_builder.append_value(info.message);
            }
        }

        for (idx, definition) in schema.attrs.iter().enumerate() {
            let generic_builder = attribute_arrays[idx].as_any_mut();

            let possible_attr = event.attributes.remove(&idx);
            match possible_attr {
                Some(attribute) => append_value(generic_builder, attribute),
                None => append_null(generic_builder, &definition.data_type),
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(time_builder.finish()),
        Arc::new(type_builder.finish()),
        Arc::new(duration_builder.finish()),
        Arc::new(message_builder.finish()),
    ];

    for mut attribute_array in attribute_arrays.into_iter() {
        arrays.push(Arc::new(attribute_array.finish()))
    }

    RecordBatch::try_new(Arc::new(arrow_schema(schema)), arrays)
}

fn append_value(builder: &mut dyn Any, attribute: Attribute) {
    match attribute {
        Attribute::Boolean(value) => {
            let builder = builder.downcast_mut::<BooleanBuilder>().unwrap();
            builder.append_value(value)
        }
        Attribute::UInt64(value) => {
            let builder = builder.downcast_mut::<UInt64Builder>().unwrap();
            builder.append_value(value)
        }
        Attribute::String(value) => {
            let builder = builder.downcast_mut::<StringBuilder>().unwrap();
            builder.append_value(value)
        }
    }
}

fn append_null(builder: &mut dyn Any, attr_type: &AttributeType) {
    match attr_type {
        AttributeType::Boolean => {
            let builder = builder.downcast_mut::<BooleanBuilder>().unwrap();
            builder.append_null()
        }
        AttributeType::UInt64 => {
            let builder = builder.downcast_mut::<UInt64Builder>().unwrap();
            builder.append_null()
        }
        AttributeType::String => {
            let builder = builder.downcast_mut::<StringBuilder>().unwrap();
            builder.append_null()
        }
    }
}
