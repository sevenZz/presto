package com.facebook.presto.druid;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class DruidConnectorFactory implements ConnectorFactory{
  @Override
  public String getName() {
    return "druid";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new DruidHandleResolver();
  }

  @Override
  public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context) {
    requireNonNull(requiredConfig, "requiredConfig is null");
    try {
      Bootstrap app = new Bootstrap(new JsonModule(), new DruidModule(connectorId, context.getTypeManager()));
      Injector injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(requiredConfig).initialize();
      return injector.getInstance(DruidConnector.class);
    }
    catch (Exception e){
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
