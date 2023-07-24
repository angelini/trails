use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::{root_as_message, MessageHeader};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::SchemaAsIpc;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::convert::TryInto;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, Sender};
use tonic::transport::{self, Server};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};

pub struct TrailsService {
    tx: Sender<RecordBatch>,
    schema: Arc<Schema>,
}

#[tonic::async_trait]
impl FlightService for TrailsService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.get_ref();
        match descriptor.r#type() {
            DescriptorType::Path => {
                let path = &descriptor.path;
                if path.len() == 1 && path[0] == "example" {
                    let options = IpcWriteOptions::default();
                    let result: SchemaResult = SchemaAsIpc::new(&self.schema, &options)
                        .try_into()
                        .expect("Error encoding schema");
                    Ok(Response::new(result))
                } else {
                    Err(Status::unimplemented(
                        "Implement get_schema: PATH != example",
                    ))
                }
            }
            _ => Err(Status::unimplemented("Implement get_schema: CMD")),
        }
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let tx = self.tx.clone();
        let mut stream = request.into_inner();
        let mut descriptor_opt: Option<FlightDescriptor> = None;

        while let Some(message) = stream.next().await {
            let message = message?;

            if descriptor_opt.is_none() {
                descriptor_opt = message.flight_descriptor.clone();
            }

            match &descriptor_opt {
                Some(descriptor) => match descriptor.r#type() {
                    DescriptorType::Path => {
                        let path = &descriptor.path;
                        if path.len() == 1 && path[0] == "example" {
                            let batch = record_batch_from_flight_data(message, &self.schema)
                                .map_err(arrow_err_to_status)?;
                            tx.send(batch).await.map_err(send_err_to_status)?;
                        } else {
                            return Err(Status::unimplemented("Implement do_put: PATH != example"));
                        }
                    }
                    _ => return Err(Status::unimplemented("Implement do_put: CMD")),
                },
                None => unreachable!(),
            }
        }

        let output = futures::stream::iter(vec![Ok(PutResult {
            app_metadata: Bytes::from(""),
        })]);

        Ok(Response::new(Box::pin(output) as Self::DoPutStream))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

fn send_err_to_status<T>(err: SendError<T>) -> Status {
    Status::internal(err.to_string())
}

fn arrow_err_to_status(err: ArrowError) -> Status {
    Status::internal(err.to_string())
}

fn record_batch_from_flight_data(
    data: FlightData,
    schema: &SchemaRef,
) -> Result<RecordBatch, ArrowError> {
    let ipc_message = root_as_message(&data.data_header[..])
        .map_err(|err| ArrowError::ParseError(format!("Unable to get root as message: {err:?}")))?;

    match ipc_message.header_type() {
        MessageHeader::RecordBatch => {
            let ipc_record_batch = ipc_message.header_as_record_batch().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a record batch".to_string(),
                )
            })?;

            let dictionaries_by_field = HashMap::new();
            let record_batch = read_record_batch(
                &arrow::buffer::Buffer::from(data.data_body),
                ipc_record_batch,
                schema.clone(),
                &dictionaries_by_field,
                None,
                &ipc_message.version(),
            )?;
            Ok(record_batch)
        }
        _ => unimplemented!(),
    }
}

pub async fn start_server(address: SocketAddr, schema: Schema) -> Result<(), transport::Error> {
    let (tx, mut rx) = mpsc::channel::<RecordBatch>(128);
    let service = TrailsService {
        schema: Arc::new(schema),
        tx,
    };

    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            // todo
        }
    });

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(address).await?;

    Ok(())
}
