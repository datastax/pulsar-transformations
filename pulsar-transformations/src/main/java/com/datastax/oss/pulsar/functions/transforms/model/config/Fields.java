package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "items", "description", "message"})
@Generated("jsonschema2pojo")
public class Fields {

  @JsonProperty("type")
  private List<String> type = new ArrayList<String>();

  @JsonProperty("items")
  private Items items;

  @JsonProperty("description")
  private String description;

  @JsonProperty("message")
  private Message__1 message;

  @JsonProperty("type")
  public List<String> getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(List<String> type) {
    this.type = type;
  }

  @JsonProperty("items")
  public Items getItems() {
    return items;
  }

  @JsonProperty("items")
  public void setItems(Items items) {
    this.items = items;
  }

  @JsonProperty("description")
  public String getDescription() {
    return description;
  }

  @JsonProperty("description")
  public void setDescription(String description) {
    this.description = description;
  }

  @JsonProperty("message")
  public Message__1 getMessage() {
    return message;
  }

  @JsonProperty("message")
  public void setMessage(Message__1 message) {
    this.message = message;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Fields.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("items");
    sb.append('=');
    sb.append(((this.items == null) ? "<null>" : this.items));
    sb.append(',');
    sb.append("description");
    sb.append('=');
    sb.append(((this.description == null) ? "<null>" : this.description));
    sb.append(',');
    sb.append("message");
    sb.append('=');
    sb.append(((this.message == null) ? "<null>" : this.message));
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
    result = ((result * 31) + ((this.message == null) ? 0 : this.message.hashCode()));
    result = ((result * 31) + ((this.items == null) ? 0 : this.items.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Fields) == false) {
      return false;
    }
    Fields rhs = ((Fields) other);
    return (((((this.description == rhs.description)
                    || ((this.description != null) && this.description.equals(rhs.description)))
                && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
            && ((this.message == rhs.message)
                || ((this.message != null) && this.message.equals(rhs.message))))
        && ((this.items == rhs.items) || ((this.items != null) && this.items.equals(rhs.items))));
  }
}
