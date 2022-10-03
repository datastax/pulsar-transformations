package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "when"})
@Generated("jsonschema2pojo")
public class Properties__1 {

  @JsonProperty("type")
  private Type type;

  @JsonProperty("when")
  private When when;

  @JsonProperty("type")
  public Type getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type type) {
    this.type = type;
  }

  @JsonProperty("when")
  public When getWhen() {
    return when;
  }

  @JsonProperty("when")
  public void setWhen(When when) {
    this.when = when;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__1.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("when");
    sb.append('=');
    sb.append(((this.when == null) ? "<null>" : this.when));
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
    result = ((result * 31) + ((this.when == null) ? 0 : this.when.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__1) == false) {
      return false;
    }
    Properties__1 rhs = ((Properties__1) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this.when == rhs.when) || ((this.when != null) && this.when.equals(rhs.when))));
  }
}
