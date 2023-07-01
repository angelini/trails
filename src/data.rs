use std::{sync::Arc, time::Duration};

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, DurationNanosecondBuilder, FixedSizeBinaryBuilder,
        StringBuilder, TimestampNanosecondBuilder, UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    record_batch::RecordBatch,
};
use byteorder::{ByteOrder, LittleEndian};
use chrono::NaiveDateTime;

use crate::core::{FieldValues, SpanId, TraceId};

pub struct SpandStartInfo {}

pub struct SpanEndInfo {
    end_time: NaiveDateTime,
    duration: Duration,
}

pub struct LogInfo {
    message: String,
}

pub enum EventType {
    SpanStart(SpandStartInfo),
    SpanEnd(SpanEndInfo),
    Log(LogInfo),
}

pub struct Event {
    trace_id: TraceId,
    span_id: SpanId,
    time: NaiveDateTime,
    event_type: EventType,
    attributes: FieldValues,
}

fn arrow_schema(attributes_schema: Schema) -> Schema {
    let mut fields = vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::Int64, false),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration", DataType::Duration(TimeUnit::Nanosecond), false),
        Field::new("message", DataType::Utf8, false),
    ];

    for field in attributes_schema.fields.iter() {
        fields.push(field.as_ref().clone())
    }

    Schema::new(fields)
}

pub fn to_record_batch(
    attributes_schema: Schema,
    events: Vec<Event>,
) -> Result<RecordBatch, ArrowError> {
    let size = events.len();
    let mut attribute_arrays: Vec<Box<dyn ArrayBuilder>> =
        Vec::with_capacity(attributes_schema.fields.len());

    for field in attributes_schema.fields.iter() {
        match field.data_type() {
            DataType::Boolean => {
                attribute_arrays.push(Box::new(BooleanBuilder::with_capacity(size)))
            }
            DataType::UInt64 => attribute_arrays.push(Box::new(UInt8Builder::with_capacity(size))),
            DataType::Utf8 => {
                attribute_arrays.push(Box::new(StringBuilder::with_capacity(size, size)))
            }
            _ => unimplemented!(),
        }
    }

    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(size, 16);
    let mut span_id_builder = UInt64Builder::with_capacity(size);
    let mut time_builder = TimestampNanosecondBuilder::with_capacity(size);

    let mut end_time_builder = TimestampNanosecondBuilder::with_capacity(size);
    let mut duration_builder = DurationNanosecondBuilder::with_capacity(size);
    let mut message_builder = StringBuilder::with_capacity(size, size);

    for event in events {
        trace_id_builder
            .append_value(event.trace_id.as_bytes())
            .unwrap();
        span_id_builder.append_value(event.span_id);
        time_builder.append_value(event.time.timestamp_nanos());

        match event.event_type {
            EventType::SpanStart(_) => {
                end_time_builder.append_null();
                duration_builder.append_null();
                message_builder.append_null();
            }
            EventType::SpanEnd(info) => {
                end_time_builder.append_value(info.end_time.timestamp_nanos());
                duration_builder
                    .append_value((info.end_time - event.time).num_nanoseconds().unwrap());
                message_builder.append_null();
            }
            EventType::Log(info) => {
                end_time_builder.append_null();
                duration_builder.append_null();
                message_builder.append_value(info.message);
            }
        }

        for (idx, field) in attributes_schema.fields.iter().enumerate() {
            let generic_builder = attribute_arrays[idx].as_any_mut();
            let bytes = event.attributes.get(&idx).unwrap();

            match field.data_type() {
                DataType::Boolean => {
                    let builder = generic_builder.downcast_mut::<BooleanBuilder>().unwrap();
                    if bytes.is_empty() {
                        builder.append_null()
                    } else {
                        builder.append_value(bytes.as_ref()[0] != 0)
                    }
                }
                DataType::UInt64 => {
                    let builder = generic_builder.downcast_mut::<UInt64Builder>().unwrap();
                    if bytes.is_empty() {
                        builder.append_null()
                    } else {
                        builder.append_value(LittleEndian::read_u64(bytes.as_ref()))
                    }
                }
                DataType::Utf8 => {
                    let builder = generic_builder.downcast_mut::<StringBuilder>().unwrap();
                    if bytes.is_empty() {
                        builder.append_null()
                    } else {
                        unsafe {
                            builder.append_value(std::str::from_utf8_unchecked(bytes.as_ref()))
                        }
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(trace_id_builder.finish()),
        Arc::new(span_id_builder.finish()),
        Arc::new(time_builder.finish()),
        Arc::new(end_time_builder.finish()),
        Arc::new(duration_builder.finish()),
        Arc::new(message_builder.finish()),
    ];

    for mut attribute_array in attribute_arrays.into_iter() {
        arrays.push(Arc::new(attribute_array.finish()))
    }

    RecordBatch::try_new(Arc::new(arrow_schema(attributes_schema)), arrays)
}
