package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "properties", "description", "required"})
@Generated("jsonschema2pojo")
public class Items__1 {

  @JsonProperty("type")
  private List<String> type = new ArrayList<String>();

  @JsonProperty("properties")
  private Properties__9 properties;

  @JsonProperty("description")
  private String description;

  @JsonProperty("required")
  private List<String> required = new ArrayList<String>();

  @JsonProperty("type")
  public List<String> getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(List<String> type) {
    this.type = type;
  }

  @JsonProperty("properties")
  public Properties__9 getProperties() {
    return properties;
  }

  @JsonProperty("properties")
  public void setProperties(Properties__9 properties) {
    this.properties = properties;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  @JsonProperty("description")
  public void setDescription(String description) {
    this.description = description;
  }

  @JsonProperty("required")
  public List<String> getRequired() {
    return required;
  }

  @JsonProperty("required")
  public void setRequired(List<String> required) {
    this.required = required;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Items__1.class.getName())
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
    sb.append("description");
    sb.append('=');
    sb.append(((this.description == null) ? "<null>" : this.description));
    sb.append(',');
    sb.append("required");
    sb.append('=');
    sb.append(((this.required == null) ? "<null>" : this.required));
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
    result = ((result * 31) + ((this.description == null) ? 0 : this.description.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this.properties == null) ? 0 : this.properties.hashCode()));
    result = ((result * 31) + ((this.required == null) ? 0 : this.required.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Items__1) == false) {
      return false;
    }
    Items__1 rhs = ((Items__1) other);
    return (((((this.description == rhs.description)
                    || ((this.description != null) && this.description.equals(rhs.description)))
                && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
            && ((this.properties == rhs.properties)
                || ((this.properties != null) && this.properties.equals(rhs.properties))))
        && ((this.required == rhs.required)
            || ((this.required != null) && this.required.equals(rhs.required))));
  }
}
