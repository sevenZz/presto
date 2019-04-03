package com.facebook.presto.druid;



import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.net.URI;

public class DruidConfig {
  private URI metadata;

  @NotNull
  public URI getMetadata()
  {
    return metadata;
  }

  @Config("metadata-uri")
  public DruidConfig setMetadata(URI metadata)
  {
    this.metadata = metadata;
    return this;
  }
}
