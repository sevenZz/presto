package com.facebook.presto.druid;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DruidColumn {
  private final String name;
  private final Type type;

  @JsonCreator
  public DruidColumn(
          @JsonProperty("name") String name,
          @JsonProperty("type") Type type)
  {
    checkArgument(!isNullOrEmpty(name), "name is null or empty");
    this.name = name;
    this.type = requireNonNull(type, "type is null");
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Type getType() { return type; }

  @Override
  public int hashCode() { return Objects.hash(name, type); }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    DruidColumn other = (DruidColumn) obj;
    return Objects.equals(this.name, other.name) &&
             Objects.equals(this.type, other.type);
  }

  @Override
  public String toString() { return name + ":" + type;}
}
