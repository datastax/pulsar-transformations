package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "extends", "allOf"})
@Generated("jsonschema2pojo")
public class DropFields {

  @JsonProperty("type")
  private String type;

  @JsonProperty("extends")
  private Extends _extends;

  @JsonProperty("allOf")
  private List<AllOf> allOf = new ArrayList<AllOf>();

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("extends")
  public Extends getExtends() {
    return _extends;
  }

  @JsonProperty("extends")
  public void setExtends(Extends _extends) {
    this._extends = _extends;
  }

  @JsonProperty("allOf")
  public List<AllOf> getAllOf() {
    return allOf;
  }

  @JsonProperty("allOf")
  public void setAllOf(List<AllOf> allOf) {
    this.allOf = allOf;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(DropFields.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("_extends");
    sb.append('=');
    sb.append(((this._extends == null) ? "<null>" : this._extends));
    sb.append(',');
    sb.append("allOf");
    sb.append('=');
    sb.append(((this.allOf == null) ? "<null>" : this.allOf));
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
    result = ((result * 31) + ((this.allOf == null) ? 0 : this.allOf.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this._extends == null) ? 0 : this._extends.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof DropFields) == false) {
      return false;
    }
    DropFields rhs = ((DropFields) other);
    return ((((this.allOf == rhs.allOf) || ((this.allOf != null) && this.allOf.equals(rhs.allOf)))
            && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
        && ((this._extends == rhs._extends)
            || ((this._extends != null) && this._extends.equals(rhs._extends))));
  }
}
