package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"allOf"})
@Generated("jsonschema2pojo")
public class ComputeFields {

  @JsonProperty("allOf")
  private List<AllOf__6> allOf = new ArrayList<AllOf__6>();

  @JsonProperty("allOf")
  public List<AllOf__6> getAllOf() {
    return allOf;
  }

  @JsonProperty("allOf")
  public void setAllOf(List<AllOf__6> allOf) {
    this.allOf = allOf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(ComputeFields.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("allOf");
    sb.append('=');
    sb.append(((this.allOf == null) ? "<null>" : this.allOf));
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
    result = ((result * 31) + ((this.allOf == null) ? 0 : this.allOf.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof ComputeFields) == false) {
      return false;
    }
    ComputeFields rhs = ((ComputeFields) other);
    return ((this.allOf == rhs.allOf) || ((this.allOf != null) && this.allOf.equals(rhs.allOf)));
  }
}
