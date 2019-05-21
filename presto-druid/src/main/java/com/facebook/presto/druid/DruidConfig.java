package com.facebook.presto.druid;



import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.net.URI;

public class DruidConfig {
  private URI datasourceMetadata;
  private URI columnMetadata;

  @NotNull
  public URI getDatasourceMetadata()
  {
    return datasourceMetadata;
  }

  @NotNull
  public URI getColumnMetadata()
  {
    return columnMetadata;
  }

  @Config("metadata-datasource-uri")
  public DruidConfig setDatasourceMetadata(URI datasourceMetadata)
  {
    this.datasourceMetadata = datasourceMetadata;
    return this;
  }

  @Config("metadata-column-uri")
  public DruidConfig setColumnMetadata(URI columnMetadata)
  {
    this.columnMetadata = columnMetadata;
    return this;
  }
}
