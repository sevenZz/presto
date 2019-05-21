package com.facebook.presto.druid;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DruidSplitManager implements ConnectorSplitManager{
  private final String connectorId;
  private final DruidClient druidClient;

  @Inject
  public DruidSplitManager(DruidConnectorId connectorId, DruidClient druidClient)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.druidClient = requireNonNull(druidClient, "druidClient is null");
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
  {
    DruidTableLayoutHandle layoutHandle = (DruidTableLayoutHandle) layout;
    DruidTableHandle tableHandle = layoutHandle.getTable();
    DruidTable table = druidClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

    checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

    List<ConnectorSplit> splits = new ArrayList<>();
//    for (URI uri : table.getSources()) {
//      splits.add(new DruidSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri));
//    }
    Collections.shuffle(splits);

    return new FixedSplitSource(splits);
  }
}
