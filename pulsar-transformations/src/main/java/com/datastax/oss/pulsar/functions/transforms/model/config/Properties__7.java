package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "delimiter"})
@Generated("jsonschema2pojo")
public class Properties__7 {

  @JsonProperty("type")
  private Type__6 type;

  @JsonProperty("delimiter")
  private Delimiter delimiter;

  @JsonProperty("type")
  public Type__6 getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type__6 type) {
    this.type = type;
  }

  @JsonProperty("delimiter")
  public Delimiter getDelimiter() {
    return delimiter;
  }

  @JsonProperty("delimiter")
  public void setDelimiter(Delimiter delimiter) {
    this.delimiter = delimiter;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__7.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("delimiter");
    sb.append('=');
    sb.append(((this.delimiter == null) ? "<null>" : this.delimiter));
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
    result = ((result * 31) + ((this.delimiter == null) ? 0 : this.delimiter.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__7) == false) {
      return false;
    }
    Properties__7 rhs = ((Properties__7) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this.delimiter == rhs.delimiter)
            || ((this.delimiter != null) && this.delimiter.equals(rhs.delimiter))));
  }
}
