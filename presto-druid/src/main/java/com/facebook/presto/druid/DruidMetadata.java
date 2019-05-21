package com.facebook.presto.druid;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DruidMetadata implements ConnectorMetadata {
  private final String connectorId;
  private final DruidClient druidClient;

  @Inject
  public DruidMetadata(DruidConnectorId connectorId, DruidClient druidClient)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.druidClient = requireNonNull(druidClient, "client is null");
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return listSchemaNames();
  }

  public List<String> listSchemaNames()
  {
    return ImmutableList.copyOf(druidClient.getSchemaNames());
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
    if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
      return null;
    }

    DruidTable table = druidClient.getTable(tableName.getSchemaName(), tableName.getTableName());
    if (table == null) {
      return null;
    }

    return new DruidTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
    DruidTableHandle tableHandle = (DruidTableHandle) table;
    ConnectorTableLayout layout = new ConnectorTableLayout(new DruidTableLayoutHandle(tableHandle));
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
    return new ConnectorTableLayout(handle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
    DruidTableHandle druidTableHandle = (DruidTableHandle) table;
    checkArgument(druidTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
    SchemaTableName tableName = new SchemaTableName(druidTableHandle.getSchemaName(), druidTableHandle.getTableName());

    return getTableMetadata(tableName);
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
    DruidTableHandle druidTableHandle = (DruidTableHandle) tableHandle;
    checkArgument(druidTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

    DruidTable table = druidClient.getTable(druidTableHandle.getSchemaName(), druidTableHandle.getTableName());
    if (table == null){
      throw new TableNotFoundException(druidTableHandle.toSchemaTableName());
    }

    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    int index = 0;
    for (ColumnMetadata column : table.getColumnMetadata()) {
      columnHandles.put(column.getName(), new DruidColumnHandle(connectorId, column.getName(), column.getType(), index));
      index++;
    }
    return columnHandles.build();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    return ((DruidColumnHandle) columnHandle).getColumnMetadata();
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
      if (tableMetadata != null) {
        columns.put(tableName, tableMetadata.getColumns());
      }
    }
    return columns.build();
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
    Set<String> schemaNames;
    if (schemaNameOrNull != null) {
      schemaNames = ImmutableSet.of(schemaNameOrNull);
    }
    else {
      schemaNames = druidClient.getSchemaNames();
    }
    ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
    for (String schemaName : schemaNames) {
      for (String tableName : druidClient.getTableNames(schemaName)) {
        builder.add(new SchemaTableName(schemaName, tableName));
      }
    }
    return builder.build();
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
  {
    if (prefix.getSchemaName() == null) {
      return listTables(session, prefix.getSchemaName());
    }
    return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
  {
    if (!listSchemaNames().contains(tableName.getSchemaName())) {
      return null;
    }

    DruidTable table = druidClient.getTable(tableName.getSchemaName(), tableName.getTableName());
    if (table == null) {
      return null;
    }

    return new ConnectorTableMetadata(tableName, table.getColumnMetadata());
  }
}
