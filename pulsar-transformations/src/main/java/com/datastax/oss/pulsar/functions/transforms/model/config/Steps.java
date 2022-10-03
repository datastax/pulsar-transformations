package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "description", "items"})
@Generated("jsonschema2pojo")
public class Steps {

  @JsonProperty("type")
  private String type;

  @JsonProperty("description")
  private String description;

  @JsonProperty("items")
  private Items__2 items;

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  @JsonProperty("description")
  public void setDescription(String description) {
    this.description = description;
  }

  @JsonProperty("items")
  public Items__2 getItems() {
    return items;
  }

  @JsonProperty("items")
  public void setItems(Items__2 items) {
    this.items = items;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Steps.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("description");
    sb.append('=');
    sb.append(((this.description == null) ? "<null>" : this.description));
    sb.append(',');
    sb.append("items");
    sb.append('=');
    sb.append(((this.items == null) ? "<null>" : this.items));
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
    result = ((result * 31) + ((this.items == null) ? 0 : this.items.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Steps) == false) {
      return false;
    }
    Steps rhs = ((Steps) other);
    return ((((this.description == rhs.description)
                || ((this.description != null) && this.description.equals(rhs.description)))
            && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
        && ((this.items == rhs.items) || ((this.items != null) && this.items.equals(rhs.items))));
  }
}
