package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "description", "default"})
@Generated("jsonschema2pojo")
public class Delimiter {

  @JsonProperty("type")
  private List<String> type = new ArrayList<String>();

  @JsonProperty("description")
  private String description;

  @JsonProperty("default")
  private String _default;

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

  @JsonProperty("default")
  public String getDefault() {
    return _default;
  }

  @JsonProperty("default")
  public void setDefault(String _default) {
    this._default = _default;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Delimiter.class.getName())
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
    if ((other instanceof Delimiter) == false) {
      return false;
    }
    Delimiter rhs = ((Delimiter) other);
    return ((((this.description == rhs.description)
                || ((this.description != null) && this.description.equals(rhs.description)))
            && ((this._default == rhs._default)
                || ((this._default != null) && this._default.equals(rhs._default))))
        && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))));
  }
}
