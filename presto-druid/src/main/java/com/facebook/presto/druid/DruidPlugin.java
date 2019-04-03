package com.facebook.presto.druid;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * created by zhang.zhao 04/03/2019
 */
public class DruidPlugin implements Plugin
{
  @Override
  public Iterable<ConnectorFactory> getConnectorFactories()
  {
    return ImmutableList.of(new DruidConnectorFactory());
  }
}
