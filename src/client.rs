use arrow::error::ArrowError;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError, flight_descriptor::DescriptorType,
    FlightClient, FlightDescriptor, PutResult,
};
use bytes::Bytes;
use futures::TryStreamExt;
use tonic::{
    codegen::http::uri::InvalidUri,
    transport::{self, Channel, Uri},
};

use crate::buffer::Buffer;

#[derive(Debug)]
pub enum ClientError {
    Arrow(ArrowError),
    Flight(FlightError),
    InvalidUri(InvalidUri),
    Tonic(transport::Error),
}

type Result<T> = std::result::Result<T, ClientError>;

impl From<ArrowError> for ClientError {
    fn from(value: ArrowError) -> Self {
        ClientError::Arrow(value)
    }
}

impl From<FlightError> for ClientError {
    fn from(value: FlightError) -> Self {
        ClientError::Flight(value)
    }
}

impl From<InvalidUri> for ClientError {
    fn from(value: InvalidUri) -> Self {
        ClientError::InvalidUri(value)
    }
}

impl From<transport::Error> for ClientError {
    fn from(value: transport::Error) -> Self {
        ClientError::Tonic(value)
    }
}

pub struct Client {
    fc: FlightClient,
}

impl Client {
    pub async fn new(port: u16) -> Result<Self> {
        let uri = format!("http://127.0.0.1:{}", port).parse::<Uri>()?;
        let channel = Channel::builder(uri).connect().await?;

        Ok(Self {
            fc: FlightClient::new(channel),
        })
    }

    pub async fn put_buffer<S: Into<String>>(&mut self, path: S, buffer: Buffer) -> Result<()> {
        let batch = buffer.to_record_batch()?;
        let request = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor {
                r#type: DescriptorType::Path.into(),
                path: vec![path.into()],
                cmd: Bytes::new(),
            }))
            .build(futures::stream::iter(vec![Ok(batch)]));

        self.fc
            .do_put(request)
            .await?
            .try_collect::<Vec<PutResult>>()
            .await?;
        Ok(())
    }
}
