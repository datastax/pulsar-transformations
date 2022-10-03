package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"propertyName", "mapping"})
@Generated("jsonschema2pojo")
public class Discriminator {

  @JsonProperty("propertyName")
  private String propertyName;

  @JsonProperty("mapping")
  private Mapping mapping;

  @JsonProperty("propertyName")
  public String getPropertyName() {
    return propertyName;
  }

  @JsonProperty("propertyName")
  public void setPropertyName(String propertyName) {
    this.propertyName = propertyName;
  }

  @JsonProperty("mapping")
  public Mapping getMapping() {
    return mapping;
  }

  @JsonProperty("mapping")
  public void setMapping(Mapping mapping) {
    this.mapping = mapping;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Discriminator.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("propertyName");
    sb.append('=');
    sb.append(((this.propertyName == null) ? "<null>" : this.propertyName));
    sb.append(',');
    sb.append("mapping");
    sb.append('=');
    sb.append(((this.mapping == null) ? "<null>" : this.mapping));
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
    result = ((result * 31) + ((this.propertyName == null) ? 0 : this.propertyName.hashCode()));
    result = ((result * 31) + ((this.mapping == null) ? 0 : this.mapping.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Discriminator) == false) {
      return false;
    }
    Discriminator rhs = ((Discriminator) other);
    return (((this.propertyName == rhs.propertyName)
            || ((this.propertyName != null) && this.propertyName.equals(rhs.propertyName)))
        && ((this.mapping == rhs.mapping)
            || ((this.mapping != null) && this.mapping.equals(rhs.mapping))));
  }
}
