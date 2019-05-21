package com.facebook.presto.druid;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DruidSplit implements ConnectorSplit {
  private final String connectorId;
  private final String schemaName;
  private final String tableName;
  private final URI uri;
  private final boolean remotelyAccessible;
  private final List<HostAddress> addresses;

  @JsonCreator
  public DruidSplit(
          @JsonProperty("connectorId") String connectorId,
          @JsonProperty("schemaName") String schemaName,
          @JsonProperty("tableName") String tableName,
          @JsonProperty("uri") URI uri)
  {
    this.schemaName = requireNonNull(schemaName, "schema name is null");
    this.tableName = requireNonNull(tableName, "table name is null");
    this.connectorId = requireNonNull(connectorId, "connector id is null");
    this.uri = requireNonNull(uri, "uri is null");

    remotelyAccessible = true;
    addresses = ImmutableList.of(HostAddress.fromUri(uri));
  }

  @JsonProperty
  public String getConnectorId() { return connectorId; }

  @JsonProperty
  public String getSchemaName() { return schemaName; }

  @JsonProperty
  public String getTableName() { return tableName; }

  @JsonProperty
  public URI getUri() { return uri; }

  @Override
  public boolean isRemotelyAccessible() {
    return remotelyAccessible;
  }

  @Override
  public List<HostAddress> getAddresses() {
    return addresses;
  }

  @Override
  public Object getInfo() {
    return this;
  }
}
