use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;

use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::catalog_test_utils::*;
use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use crate::storage::iceberg::schema_utils::assert_is_same_schema;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut guard = RestCatalogTestGuard::new(namespace.clone(), /*table=*/ None)
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let namespace = NamespaceIdent::new(namespace);
    let table_creation = default_table_creation(table.clone());
    let table_name = table_creation.name.clone();
    catalog
        .create_table(&namespace, table_creation)
        .await
        .unwrap();
    let table_ident = TableIdent::new(namespace, table_name);
    guard.table = Some(table_ident.clone());
    assert!(catalog.table_exists(&table_ident).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_drop_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let table_ident = guard.table.clone().unwrap();
    guard.table = None;
    assert!(catalog.table_exists(&table_ident).await.unwrap());
    catalog.drop_table(&table_ident).await.unwrap();
    assert!(!catalog.table_exists(&table_ident).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_exists() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let iceberg_schema = create_test_table_schema().unwrap();
    let catalog = RestCatalog::new(rest_catalog_config, accessor_config, iceberg_schema.clone())
        .await
        .unwrap();

    // Check table existence.
    let table_ident = guard.table.clone().unwrap();
    assert!(catalog.table_exists(&table_ident).await.unwrap());

    // List tables and validate.
    let tables = catalog.list_tables(table_ident.namespace()).await.unwrap();
    assert_eq!(tables, vec![table_ident.clone()]);

    // Load table and check schema.
    let table = catalog.load_table(&table_ident).await.unwrap();
    let actual_schema = table.metadata().current_schema();
    assert_is_same_schema(actual_schema.as_ref().clone(), iceberg_schema);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_load_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), Some(table.clone()))
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let table_ident = guard.table.clone().unwrap();
    let result = catalog.load_table(&table_ident).await.unwrap();
    let result_table_ident = result.identifier().clone();
    assert_eq!(table_ident, result_table_ident);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_namespace() {
    let namespace = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), /*table=*/ None)
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let ns_ident = guard.namespace.clone().unwrap();
    assert!(catalog.namespace_exists(&ns_ident).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_drop_namespace() {
    let namespace = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), /*table=*/ None)
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let ns_parent_ident = guard.namespace.clone().unwrap();
    assert!(catalog.namespace_exists(&ns_parent_ident).await.unwrap());
    let ns_name = get_random_string();
    let ns_ident = NamespaceIdent::from_strs(vec![namespace, ns_name]).unwrap();
    catalog
        .create_namespace(&ns_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    assert_eq!(
        catalog
            .list_namespaces(Some(&ns_parent_ident))
            .await
            .unwrap(),
        vec![ns_ident.clone()]
    );
    catalog.drop_namespace(&ns_ident).await.unwrap();
    assert_eq!(
        catalog
            .list_namespaces(Some(&ns_parent_ident))
            .await
            .unwrap(),
        vec![]
    );
    assert!(!catalog.namespace_exists(&ns_ident).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_namespace() {
    let namespace = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), /*table=*/ None)
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let ns_ident = guard.namespace.clone().unwrap();
    assert!(catalog.namespace_exists(&ns_ident).await.unwrap());
    let ns_name = get_random_string();
    let ns_ident = NamespaceIdent::from_strs(vec![namespace, ns_name]).unwrap();
    let ns = catalog
        .create_namespace(&ns_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    assert_eq!(catalog.get_namespace(&ns_ident).await.unwrap(), ns);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_namespace() {
    fn to_set<T: Eq + Hash>(vec: Vec<T>) -> HashSet<T> {
        HashSet::from_iter(vec)
    }
    let namespace = get_random_string();
    let guard = RestCatalogTestGuard::new(namespace.clone(), /*table=*/ None)
        .await
        .unwrap();
    let rest_catalog_config = default_rest_catalog_config();
    let accessor_config = default_accessor_config();
    let catalog = RestCatalog::new(
        rest_catalog_config,
        accessor_config,
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    let namespace_1 = get_random_string();
    let namespace_2 = get_random_string();
    let ns_ident_1 = NamespaceIdent::from_strs(vec![namespace.clone(), namespace_1]).unwrap();
    let ns_ident_2 = NamespaceIdent::from_strs(vec![namespace.clone(), namespace_2]).unwrap();
    catalog
        .create_namespace(&ns_ident_1, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    catalog
        .create_namespace(&ns_ident_2, /*properties=*/ HashMap::new())
        .await
        .unwrap();
    let ns_parent_ident = guard.namespace.clone().unwrap();
    assert_eq!(
        to_set(
            catalog
                .list_namespaces(Some(&ns_parent_ident))
                .await
                .unwrap()
        ),
        to_set(vec![ns_ident_1, ns_ident_2])
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table_with_requirement_check_failed() {
    let namespace = get_random_string();
    let table = get_random_string();
    let catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_table_with_requirement_check_failed_impl(
        &catalog,
        namespace.clone(),
        table.clone(),
    )
    .await;
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_table_impl(&mut catalog, namespace.clone(), table.clone()).await;
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_update_schema() {
    let namespace = get_random_string();
    let table = get_random_string();
    let mut catalog = RestCatalog::new(
        default_rest_catalog_config(),
        default_accessor_config(),
        create_test_table_schema().unwrap(),
    )
    .await
    .unwrap();
    test_update_schema_impl(&mut catalog, namespace.to_string(), table.to_string()).await;

    // Clean up test.
    catalog
        .drop_table(&TableIdent::new(
            NamespaceIdent::new(namespace.clone()),
            table.clone(),
        ))
        .await
        .unwrap();
    catalog
        .drop_namespace(&NamespaceIdent::new(namespace.clone()))
        .await
        .unwrap();
}
