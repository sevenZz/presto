package com.facebook.presto.druid;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DruidTable {
  private final String name;
  private final List<DruidColumn> columns;
  private final List<ColumnMetadata> columnMetadata;
  private final List<URI> sources;

  @JsonCreator
  public DruidTable(
          @JsonProperty("name") String name,
          @JsonProperty("columns") List<DruidColumn> columns,
          @JsonProperty("sources") List<URI> sources)
  {
    checkArgument(!isNullOrEmpty(name), "name is null or is empty");
    this.name = requireNonNull(name, "name is null");
    this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
    this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));

    ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
    for (DruidColumn column : this.columns) {
      columnMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
    }
    this.columnMetadata = columnMetadata.build();
  }

  @JsonProperty
  public String getName() { return name; }

  @JsonProperty
  public List<DruidColumn> getColumns() { return columns; }

  @JsonProperty
  public List<URI> getSources() { return sources; }

  public List<ColumnMetadata> getColumnMetadata() { return columnMetadata; }
}
