package com.facebook.presto.druid;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class DruidHandleResolver implements ConnectorHandleResolver{
  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return DruidTableHandle.class;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return DruidTableLayoutHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return DruidColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return DruidSplit.class;
  }

  @Override
  public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
  {
    return DruidTransactionHandle.class;
  }
}
