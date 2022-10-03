package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"steps"})
@Generated("jsonschema2pojo")
public class Properties__10 {

  @JsonProperty("steps")
  private Steps steps;

  @JsonProperty("steps")
  public Steps getSteps() {
    return steps;
  }

  @JsonProperty("steps")
  public void setSteps(Steps steps) {
    this.steps = steps;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__10.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("steps");
    sb.append('=');
    sb.append(((this.steps == null) ? "<null>" : this.steps));
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
    result = ((result * 31) + ((this.steps == null) ? 0 : this.steps.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__10) == false) {
      return false;
    }
    Properties__10 rhs = ((Properties__10) other);
    return ((this.steps == rhs.steps) || ((this.steps != null) && this.steps.equals(rhs.steps)));
  }
}
