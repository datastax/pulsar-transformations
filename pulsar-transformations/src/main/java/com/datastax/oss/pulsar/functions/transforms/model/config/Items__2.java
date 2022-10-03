package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"oneOf"})
@Generated("jsonschema2pojo")
public class Items__2 {

  @JsonProperty("oneOf")
  private List<OneOf> oneOf = new ArrayList<OneOf>();

  @JsonProperty("oneOf")
  public List<OneOf> getOneOf() {
    return oneOf;
  }

  @JsonProperty("oneOf")
  public void setOneOf(List<OneOf> oneOf) {
    this.oneOf = oneOf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Items__2.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("oneOf");
    sb.append('=');
    sb.append(((this.oneOf == null) ? "<null>" : this.oneOf));
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
    result = ((result * 31) + ((this.oneOf == null) ? 0 : this.oneOf.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Items__2) == false) {
      return false;
    }
    Items__2 rhs = ((Items__2) other);
    return ((this.oneOf == rhs.oneOf) || ((this.oneOf != null) && this.oneOf.equals(rhs.oneOf)));
  }
}
