package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "unwrap-key"})
@Generated("jsonschema2pojo")
public class Properties__4 {

  @JsonProperty("type")
  private Type__3 type;

  @JsonProperty("unwrap-key")
  private UnwrapKey unwrapKey;

  @JsonProperty("type")
  public Type__3 getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type__3 type) {
    this.type = type;
  }

  @JsonProperty("unwrap-key")
  public UnwrapKey getUnwrapKey() {
    return unwrapKey;
  }

  @JsonProperty("unwrap-key")
  public void setUnwrapKey(UnwrapKey unwrapKey) {
    this.unwrapKey = unwrapKey;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__4.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("unwrapKey");
    sb.append('=');
    sb.append(((this.unwrapKey == null) ? "<null>" : this.unwrapKey));
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
    result = ((result * 31) + ((this.unwrapKey == null) ? 0 : this.unwrapKey.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__4) == false) {
      return false;
    }
    Properties__4 rhs = ((Properties__4) other);
    return (((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
        && ((this.unwrapKey == rhs.unwrapKey)
            || ((this.unwrapKey != null) && this.unwrapKey.equals(rhs.unwrapKey))));
  }
}
