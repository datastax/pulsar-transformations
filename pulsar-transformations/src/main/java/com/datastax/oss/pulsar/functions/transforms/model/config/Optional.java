package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "description", "default"})
@Generated("jsonschema2pojo")
public class Optional {

  @JsonProperty("type")
  private String type;

  @JsonProperty("description")
  private String description;

  @JsonProperty("default")
  private Boolean _default;

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

  @JsonProperty("default")
  public Boolean getDefault() {
    return _default;
  }

  @JsonProperty("default")
  public void setDefault(Boolean _default) {
    this._default = _default;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Optional.class.getName())
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
    sb.append("_default");
    sb.append('=');
    sb.append(((this._default == null) ? "<null>" : this._default));
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
    result = ((result * 31) + ((this._default == null) ? 0 : this._default.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Optional) == false) {
      return false;
    }
    Optional rhs = ((Optional) other);
    return ((((this.description == rhs.description)
                || ((this.description != null) && this.description.equals(rhs.description)))
            && ((this._default == rhs._default)
                || ((this._default != null) && this._default.equals(rhs._default))))
        && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))));
  }
}
