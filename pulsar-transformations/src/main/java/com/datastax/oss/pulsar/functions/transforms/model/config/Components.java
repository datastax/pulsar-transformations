package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"schemas"})
@Generated("jsonschema2pojo")
public class Components {

  @JsonProperty("schemas")
  private Schemas schemas;

  @JsonProperty("schemas")
  public Schemas getSchemas() {
    return schemas;
  }

  @JsonProperty("schemas")
  public void setSchemas(Schemas schemas) {
    this.schemas = schemas;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Components.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("schemas");
    sb.append('=');
    sb.append(((this.schemas == null) ? "<null>" : this.schemas));
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
    result = ((result * 31) + ((this.schemas == null) ? 0 : this.schemas.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Components) == false) {
      return false;
    }
    Components rhs = ((Components) other);
    return ((this.schemas == rhs.schemas)
        || ((this.schemas != null) && this.schemas.equals(rhs.schemas)));
  }
}
