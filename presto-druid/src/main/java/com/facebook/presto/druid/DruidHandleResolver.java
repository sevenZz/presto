package com.facebook.presto.druid;

import com.facebook.presto.spi.*;

public class DruidHandleResolver implements ConnectorHandleResolver{
  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return null;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return null;
  }
}
