package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "minItems", "items"})
@Generated("jsonschema2pojo")
public class Fields__1 {

  @JsonProperty("type")
  private String type;

  @JsonProperty("minItems")
  private Integer minItems;

  @JsonProperty("items")
  private Items__1 items;

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("minItems")
  public Integer getMinItems() {
    return minItems;
  }

  @JsonProperty("minItems")
  public void setMinItems(Integer minItems) {
    this.minItems = minItems;
  }

  @JsonProperty("items")
  public Items__1 getItems() {
    return items;
  }

  @JsonProperty("items")
  public void setItems(Items__1 items) {
    this.items = items;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Fields__1.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("minItems");
    sb.append('=');
    sb.append(((this.minItems == null) ? "<null>" : this.minItems));
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
    result = ((result * 31) + ((this.minItems == null) ? 0 : this.minItems.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this.items == null) ? 0 : this.items.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Fields__1) == false) {
      return false;
    }
    Fields__1 rhs = ((Fields__1) other);
    return ((((this.minItems == rhs.minItems)
                || ((this.minItems != null) && this.minItems.equals(rhs.minItems)))
            && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
        && ((this.items == rhs.items) || ((this.items != null) && this.items.equals(rhs.items))));
  }
}
