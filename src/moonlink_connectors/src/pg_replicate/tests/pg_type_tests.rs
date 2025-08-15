#![cfg(feature = "connector-pg")]

use crate::pg_replicate::clients::postgres::ReplicationClient;
use crate::pg_replicate::conversions::text::TextFormatConverter;
use crate::pg_replicate::table::{ColumnSchema, TableName};
use serial_test::serial;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{connect, NoTls};

async fn setup_connection() -> tokio_postgres::Client {
    // Use DATABASE_URL env var if set (for CI), otherwise use devcontainer default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@postgres:5432/postgres".to_string());

    let (client, connection) = connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {e}");
        }
    });
    client
}

async fn get_table_id(client: &tokio_postgres::Client, table_name: &str) -> u32 {
    let query = format!(
        "SELECT oid FROM pg_class WHERE relname = '{}' 
         AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
        table_name
    );

    for message in client.simple_query(&query).await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            return row.get("oid").unwrap().parse().unwrap();
        }
    }
    panic!("Table not found: {}", table_name);
}

#[tokio::test]
#[serial]
async fn test_basic_composite_type() {
    let client = setup_connection().await;

    // Create basic composite type
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_address CASCADE;
             CREATE TYPE test_address AS (street TEXT, city TEXT, zip INTEGER);
             
             DROP TABLE IF EXISTS test_basic_composite CASCADE;
             CREATE TABLE test_basic_composite (
                 id INTEGER PRIMARY KEY,
                 addr test_address
             );
             
             INSERT INTO test_basic_composite VALUES 
             (1, ROW('123 Main St', 'NYC', 10001)::test_address);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_basic_composite").await;
    let table_name = TableName {
        schema: "public".to_string(),
        name: "test_basic_composite".to_string(),
    };

    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(table_id, table_name, /*publication=*/ None)
        .await
        .unwrap();

    let addr_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "addr")
        .unwrap();

    if let Kind::Composite(fields) = addr_col.typ.kind() {
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name(), "street");
        assert_eq!(*fields[0].type_(), Type::TEXT);
        assert_eq!(fields[1].name(), "city");
        assert_eq!(*fields[1].type_(), Type::TEXT);
        assert_eq!(fields[2].name(), "zip");
        assert_eq!(*fields[2].type_(), Type::INT4);
    } else {
        panic!("Expected composite type");
    }

    // Test parsing
    let test_value = r#"("123 Main St",NYC,10001)"#;
    assert!(TextFormatConverter::try_from_str(&addr_col.typ, test_value).is_ok());
}

#[tokio::test]
#[serial]
async fn test_nested_composite_types() {
    let client = setup_connection().await;

    // Create nested composite types
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_point CASCADE;
             DROP TYPE IF EXISTS test_location CASCADE;
             CREATE TYPE test_point AS (x FLOAT8, y FLOAT8);
             CREATE TYPE test_location AS (name TEXT, point test_point);
             
             DROP TABLE IF EXISTS test_nested CASCADE;
             CREATE TABLE test_nested (
                 id INTEGER PRIMARY KEY,
                 loc test_location
             );
             
             INSERT INTO test_nested VALUES 
             (1, ROW('Home', ROW(1.5, 2.5)::test_point)::test_location);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_nested").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_nested".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let loc_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "loc")
        .unwrap();

    if let Kind::Composite(loc_fields) = loc_col.typ.kind() {
        let point_field = loc_fields.iter().find(|f| f.name() == "point").unwrap();

        if let Kind::Composite(point_fields) = point_field.type_().kind() {
            assert_eq!(point_fields.len(), 2);
            assert_eq!(*point_fields[0].type_(), Type::FLOAT8);
            assert_eq!(*point_fields[1].type_(), Type::FLOAT8);
        } else {
            panic!("Expected nested composite");
        }
    } else {
        panic!("Expected composite type");
    }

    let test_value = r#"(Home,"(1.5,2.5)")"#;
    assert!(TextFormatConverter::try_from_str(&loc_col.typ, test_value).is_ok());
}

#[tokio::test]
#[serial]
async fn test_array_of_composite_types() {
    let client = setup_connection().await;

    // Create composite type for array testing
    client
        .simple_query(
            "DROP TYPE IF EXISTS test_item CASCADE;
             CREATE TYPE test_item AS (name TEXT, value INTEGER);
             
             DROP TABLE IF EXISTS test_array_composite CASCADE;
             CREATE TABLE test_array_composite (
                 id INTEGER PRIMARY KEY,
                 items test_item[]
             );
             
             INSERT INTO test_array_composite VALUES 
             (1, ARRAY[ROW('Item1', 100)::test_item, ROW('Item2', 200)::test_item]);",
        )
        .await
        .unwrap();

    let table_id = get_table_id(&client, "test_array_composite").await;
    let replication_client = ReplicationClient::from_client(client);
    let schema = replication_client
        .get_table_schema(
            table_id,
            TableName {
                schema: "public".to_string(),
                name: "test_array_composite".to_string(),
            },
            /*publication=*/ None,
        )
        .await
        .unwrap();

    let items_col = schema
        .column_schemas
        .iter()
        .find(|c| c.name == "items")
        .unwrap();

    if let Kind::Array(element_type) = items_col.typ.kind() {
        if let Kind::Composite(fields) = element_type.kind() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(*fields[0].type_(), Type::TEXT);
            assert_eq!(fields[1].name(), "value");
            assert_eq!(*fields[1].type_(), Type::INT4);
        } else {
            panic!("Expected array element to be composite");
        }
    } else {
        panic!("Expected array type");
    }

    // Test parsing - arrays use curly braces and quotes for composite elements
    let test_value = r#"{"(Item1,100)","(Item2,200)"}"#;
    let result = TextFormatConverter::try_from_str(&items_col.typ, test_value);
    assert!(
        result.is_ok(),
        "Failed to parse array of composites: {:?}",
        result
    );
}
