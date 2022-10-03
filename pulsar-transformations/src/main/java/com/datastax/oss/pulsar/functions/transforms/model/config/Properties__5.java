package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type"})
@Generated("jsonschema2pojo")
public class Properties__5 {

  @JsonProperty("type")
  private Type__4 type;

  @JsonProperty("type")
  public Type__4 getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type__4 type) {
    this.type = type;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__5.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
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
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__5) == false) {
      return false;
    }
    Properties__5 rhs = ((Properties__5) other);
    return ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)));
  }
}
