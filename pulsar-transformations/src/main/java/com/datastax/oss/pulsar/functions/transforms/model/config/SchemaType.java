package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "description", "enum"})
@Generated("jsonschema2pojo")
public class SchemaType {

  @JsonProperty("type")
  private List<String> type = new ArrayList<String>();

  @JsonProperty("description")
  private String description;

  @JsonProperty("enum")
  private List<String> _enum = new ArrayList<String>();

  @JsonProperty("type")
  public List<String> getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(List<String> type) {
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

  @JsonProperty("enum")
  public List<String> getEnum() {
    return _enum;
  }

  @JsonProperty("enum")
  public void setEnum(List<String> _enum) {
    this._enum = _enum;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(SchemaType.class.getName())
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
    sb.append("_enum");
    sb.append('=');
    sb.append(((this._enum == null) ? "<null>" : this._enum));
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
    result = ((result * 31) + ((this._enum == null) ? 0 : this._enum.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof SchemaType) == false) {
      return false;
    }
    SchemaType rhs = ((SchemaType) other);
    return ((((this.description == rhs.description)
                || ((this.description != null) && this.description.equals(rhs.description)))
            && ((this._enum == rhs._enum)
                || ((this._enum != null) && this._enum.equals(rhs._enum))))
        && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))));
  }
}
