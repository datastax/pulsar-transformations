package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"part"})
@Generated("jsonschema2pojo")
public class Properties {

  @JsonProperty("part")
  private Part__1 part;

  @JsonProperty("part")
  public Part__1 getPart() {
    return part;
  }

  @JsonProperty("part")
  public void setPart(Part__1 part) {
    this.part = part;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("part");
    sb.append('=');
    sb.append(((this.part == null) ? "<null>" : this.part));
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
    result = ((result * 31) + ((this.part == null) ? 0 : this.part.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties) == false) {
      return false;
    }
    Properties rhs = ((Properties) other);
    return ((this.part == rhs.part) || ((this.part != null) && this.part.equals(rhs.part)));
  }
}
