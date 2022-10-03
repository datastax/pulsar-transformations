package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "schema-type"})
@Generated("jsonschema2pojo")
public class Properties__3 {

  @JsonProperty("type")
  private Type__2 type;

  @JsonProperty("schema-type")
  private SchemaType schemaType;

  @JsonProperty("type")
  public Type__2 getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type__2 type) {
    this.type = type;
  }

  @JsonProperty("schema-type")
  public SchemaType getSchemaType() {
    return schemaType;
  }

  @JsonProperty("schema-type")
  public void setSchemaType(SchemaType schemaType) {
    this.schemaType = schemaType;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__3.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("schemaType");
    sb.append('=');
    sb.append(((this.schemaType == null) ? "<null>" : this.schemaType));
    sb.append(',');
    if (sb.charAt((sb.length() - 1)) == ',') {
      sb.setCharAt((sb.length() - 1), ']');
    } else {
      sb.append(']');
    }
    return sb.toString();
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this.schemaType == null) ? 0 : this.schemaType.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__3) == false) {
      return false;
    }
    Properties__3 rhs = ((Properties__3) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this.schemaType == rhs.schemaType)
            || ((this.schemaType != null) && this.schemaType.equals(rhs.schemaType))));
  }
}
