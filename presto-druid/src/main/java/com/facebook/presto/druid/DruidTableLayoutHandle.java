package com.facebook.presto.druid;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class DruidTableLayoutHandle implements ConnectorTableLayoutHandle {
  private final DruidTableHandle table;

  @JsonCreator
  public DruidTableLayoutHandle(@JsonProperty("table") DruidTableHandle table) { this.table = table; }

  @JsonProperty
  public DruidTableHandle getTable() { return table; }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DruidTableLayoutHandle that = (DruidTableLayoutHandle) o;
    return Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() { return Objects.hash(table); }

  @Override
  public String toString() { return table.toString(); }
}
