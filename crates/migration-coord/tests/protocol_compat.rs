//! Compile-time compatibility checks for coordinator protocol re-exports.

use std::any::TypeId;

#[test]
fn schema_path_reexports_protocol_types() {
    assert_eq!(
        TypeId::of::<migration_coord::schema::EventEnvelope>(),
        TypeId::of::<migration_control_protocol::schema::EventEnvelope>(),
    );
    assert_eq!(
        TypeId::of::<migration_coord::schema::Snapshot>(),
        TypeId::of::<migration_control_protocol::schema::Snapshot>(),
    );
    assert_eq!(
        TypeId::of::<migration_coord::schema::JobId>(),
        TypeId::of::<migration_control_protocol::schema::JobId>(),
    );
}

#[test]
fn legacy_server_dto_paths_reexport_protocol_types() {
    assert_eq!(
        TypeId::of::<migration_coord::server::worker::HeartbeatBody>(),
        TypeId::of::<migration_control_protocol::schema::HeartbeatBody>(),
    );
    assert_eq!(
        TypeId::of::<migration_coord::server::read::ListJobsResponse>(),
        TypeId::of::<migration_control_protocol::schema::ListJobsResponse>(),
    );
    assert_eq!(
        TypeId::of::<migration_coord::server::command::ReasonBody>(),
        TypeId::of::<migration_control_protocol::schema::ReasonBody>(),
    );
    assert_eq!(
        TypeId::of::<migration_coord::server::stream::StreamParams>(),
        TypeId::of::<migration_control_protocol::schema::StreamParams>(),
    );
}
