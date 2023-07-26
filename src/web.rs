use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::utils::flight_data_to_arrow_batch;
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

use crate::writer::write_batch;

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
        let path = path_from_descriptor(request.get_ref())?;
        if path != "example" {
            return Err(Status::unimplemented(format!("Unsupported path: {}", path)));
        }

        let options = IpcWriteOptions::default();
        let result: SchemaResult = SchemaAsIpc::new(&self.schema, &options)
            .try_into()
            .expect("Error encoding schema");
        Ok(Response::new(result))
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

        let mut first_message = true;
        let dictionaries = HashMap::new();

        while let Some(data) = stream.next().await {
            let data = data?;

            if first_message {
                // TODO: Check incoming schema

                // let message = root_as_message(&data.data_header[..])
                //     .map_err(|_| Status::internal("Cannot get root as message".to_string()))?;

                // let ipc_schema: arrow_ipc::Schema = message
                //     .header_as_schema()
                //     .ok_or_else(|| Status::internal("Cannot get header as Schema".to_string()))?;
                // let schema = fb_to_schema(ipc_schema);

                let path = path_from_descriptor(&data.flight_descriptor.unwrap())?;
                if path != "example" {
                    return Err(Status::unimplemented(format!("Unsupported path: {}", path)));
                }

                first_message = false;
                continue;
            }

            let batch = flight_data_to_arrow_batch(&data, self.schema.clone(), &dictionaries)
                .map_err(arrow_err_to_status)?;
            tx.send(batch).await.map_err(send_err_to_status)?;
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

fn path_from_descriptor(descriptor: &FlightDescriptor) -> Result<String, Status> {
    match &descriptor.r#type() {
        DescriptorType::Path => {
            let path = &descriptor.path;
            if path.len() == 1 {
                Ok(path[0].to_string())
            } else {
                Err(Status::unimplemented(format!(
                    "Path descriptor with {} length",
                    path.len()
                )))
            }
        }
        _ => Err(Status::unimplemented("Path descriptor CMD")),
    }
}

pub async fn start_server(address: SocketAddr, schema: Schema) -> Result<(), transport::Error> {
    let (tx, mut rx) = mpsc::channel::<RecordBatch>(128);
    let service = TrailsService {
        schema: Arc::new(schema),
        tx,
    };

    tokio::spawn(async move {
        while let Some(batch) = rx.recv().await {
            if let Err(err) = write_batch(&Path::new("/tmp/trails"), rand::random(), batch).await {
                println!("failed to write batch: {:?}", err)
            }
        }
    });

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(address).await?;

    Ok(())
}
