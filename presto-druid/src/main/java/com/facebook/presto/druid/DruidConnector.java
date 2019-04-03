package com.facebook.presto.druid;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static com.facebook.presto.druid.DruidTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class DruidConnector implements Connector{
  private static final Logger log = Logger.get(DruidConnector.class);

  private final LifeCycleManager lifeCycleManager;
  private final DruidMetadata metadata;
  private final DruidSplitManager splitManager;

  @Inject
  public DruidConnector(LifeCycleManager lifeCycleManager, DruidMetadata metadata, DruidSplitManager splitManager)
  {
    this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
  {
    return INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
  {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager()
  {
    return splitManager;
  }

  @Override
  public final void shutdown()
  {
    try {
      lifeCycleManager.stop();
    } catch (Exception e) {
      log.error(e, "Error shutting down connector");
    }
  }
}