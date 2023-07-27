use std::path::Path;

use arrow::array::TimestampNanosecondArray;
use arrow::compute::{max, min};
use arrow::{error::ArrowError, record_batch::RecordBatch};
use chrono::prelude::{Datelike, Timelike};
use chrono::{DateTime, Duration, TimeZone, Utc};
use datafusion::parquet::arrow::AsyncArrowWriter;
use tokio::fs::{self, File};

pub async fn write_batch(path: &Path, idx: u16, batch: RecordBatch) -> Result<(), ArrowError> {
    let (min, max) = min_max_time(&batch)?;

    for chunk in chunk_time_range(min, max) {
        let path = path.join(format!("hour={}", chunk));
        fs::create_dir_all(&path).await?;

        let file = File::create(&path.join(format!("{}.parquet", idx))).await?;
        let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), 1024, None)?;

        writer.write(&batch).await?;
        writer.close().await?;
    }

    Ok(())
}

fn min_max_time(batch: &RecordBatch) -> Result<(DateTime<Utc>, DateTime<Utc>), ArrowError> {
    if let Some(time_array) = batch
        .column_by_name("time")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
    {
        let min = min(time_array).unwrap();
        let max = max(time_array).unwrap();

        Ok((Utc.timestamp_nanos(min), Utc.timestamp_nanos(max)))
    } else {
        Err(ArrowError::CastError(
            "Cannot cast time field to nanosecond array".to_string(),
        ))
    }
}

fn chunk_time_range(start: DateTime<Utc>, stop: DateTime<Utc>) -> Vec<i64> {
    let mut chunks = Vec::new();

    let mut dt = Utc
        .with_ymd_and_hms(start.year(), start.month(), start.day(), start.hour(), 0, 0)
        .single()
        .unwrap();

    while dt < stop {
        chunks.push(dt.timestamp_nanos());
        dt = dt.checked_add_signed(Duration::hours(1)).unwrap();
    }

    chunks
}
