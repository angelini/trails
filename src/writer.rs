use std::{fs::File, path::Path};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use datafusion::parquet::arrow::ArrowWriter;

pub fn write_batch(path: &Path, idx: u16, batch: RecordBatch) -> Result<(), ArrowError> {
    let file = File::create(path.join(format!("{}.parquet", idx)))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
