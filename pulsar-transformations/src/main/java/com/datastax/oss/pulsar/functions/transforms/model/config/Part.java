package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "properties"})
@Generated("jsonschema2pojo")
public class Part {

  @JsonProperty("type")
  private String type;

  @JsonProperty("properties")
  private Properties properties;

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("properties")
  public Properties getProperties() {
    return properties;
  }

  @JsonProperty("properties")
  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Part.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("properties");
    sb.append('=');
    sb.append(((this.properties == null) ? "<null>" : this.properties));
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
    result = ((result * 31) + ((this.properties == null) ? 0 : this.properties.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Part) == false) {
      return false;
    }
    Part rhs = ((Part) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this.properties == rhs.properties)
            || ((this.properties != null) && this.properties.equals(rhs.properties))));
  }
}
