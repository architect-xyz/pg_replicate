use api::db::sources::SourceConfig;
use reqwest::StatusCode;

use crate::{
    tenants::create_tenant,
    test_app::{
        spawn_app, CreateSourceRequest, CreateSourceResponse, SourceResponse, TestApp,
        UpdateSourceRequest,
    },
};

fn new_source_config() -> SourceConfig {
    SourceConfig::Postgres {
        host: "localhost".to_string(),
        port: 5432,
        name: "postgres".to_string(),
        username: "postgres".to_string(),
        password: Some("postgres".to_string()),
        slot_name: "slot".to_string(),
        publication: "publication".to_string(),
    }
}

fn updated_source_config() -> SourceConfig {
    SourceConfig::Postgres {
        host: "example.com".to_string(),
        port: 2345,
        name: "sergtsop".to_string(),
        username: "sergtsop".to_string(),
        password: Some("sergtsop".to_string()),
        slot_name: "tols".to_string(),
        publication: "noitacilbup".to_string(),
    }
}

pub async fn create_source(app: &TestApp, tenant_id: i64) -> i64 {
    let source = CreateSourceRequest {
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test]
async fn source_can_be_created() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let source = CreateSourceRequest {
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test]
async fn an_existing_source_can_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let source = CreateSourceRequest {
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let response = app.read_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: SourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.config, source.config);
}

#[tokio::test]
async fn an_non_existing_source_cant_be_read() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.read_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_source_can_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let source = CreateSourceRequest {
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let updated_config = UpdateSourceRequest {
        config: updated_source_config(),
    };
    let response = app
        .update_source(tenant_id, source_id, &updated_config)
        .await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    let response: SourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, source_id);
    assert_eq!(response.tenant_id, tenant_id);
    assert_eq!(response.config, updated_config.config);
}

#[tokio::test]
async fn an_non_existing_source_cant_be_updated() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let updated_config = UpdateSourceRequest {
        config: updated_source_config(),
    };
    let response = app.update_source(tenant_id, 42, &updated_config).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_existing_source_can_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    let source = CreateSourceRequest {
        config: new_source_config(),
    };
    let response = app.create_source(tenant_id, &source).await;
    let response: CreateSourceResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let source_id = response.id;

    // Act
    let response = app.delete_source(tenant_id, source_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_source(tenant_id, source_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_non_existing_source_cant_be_deleted() {
    // Arrange
    let app = spawn_app().await;
    let tenant_id = create_tenant(&app).await;

    // Act
    let response = app.delete_source(tenant_id, 42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}