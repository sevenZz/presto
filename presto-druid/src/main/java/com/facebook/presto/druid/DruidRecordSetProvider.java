package com.facebook.presto.druid;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DruidRecordSetProvider implements ConnectorRecordSetProvider {
  private final String connectorId;

  @Inject
  public DruidRecordSetProvider(DruidConnectorId connectorId)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
  }

  @Override
  public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
    requireNonNull(split, "partitionChunk is null");
    DruidSplit druidSplit = (DruidSplit) split;
    checkArgument(druidSplit.getConnectorId().equals(connectorId), "split is not for this connector");

    ImmutableList.Builder<DruidColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add((DruidColumnHandle) handle);
    }
    return new DruidRecordSet(druidSplit, handles.build());
  }
}
