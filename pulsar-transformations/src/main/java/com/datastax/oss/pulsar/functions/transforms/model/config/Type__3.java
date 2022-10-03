package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "enum"})
@Generated("jsonschema2pojo")
public class Type__3 {

  @JsonProperty("type")
  private String type;

  @JsonProperty("enum")
  private List<String> _enum = new ArrayList<String>();

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
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
    sb.append(Type__3.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
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
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this._enum == null) ? 0 : this._enum.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Type__3) == false) {
      return false;
    }
    Type__3 rhs = ((Type__3) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this._enum == rhs._enum) || ((this._enum != null) && this._enum.equals(rhs._enum))));
  }
}
