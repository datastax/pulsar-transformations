package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "drop-fields",
  "cast",
  "merge-key-value",
  "unwrap-key-value",
  "flatten",
  "drop",
  "compute-fields"
})
@Generated("jsonschema2pojo")
public class Mapping {

  @JsonProperty("drop-fields")
  private String dropFields;

  @JsonProperty("cast")
  private String cast;

  @JsonProperty("merge-key-value")
  private String mergeKeyValue;

  @JsonProperty("unwrap-key-value")
  private String unwrapKeyValue;

  @JsonProperty("flatten")
  private String flatten;

  @JsonProperty("drop")
  private String drop;

  @JsonProperty("compute-fields")
  private String computeFields;

  @JsonProperty("drop-fields")
  public String getDropFields() {
    return dropFields;
  }

  @JsonProperty("drop-fields")
  public void setDropFields(String dropFields) {
    this.dropFields = dropFields;
  }

  @JsonProperty("cast")
  public String getCast() {
    return cast;
  }

  @JsonProperty("cast")
  public void setCast(String cast) {
    this.cast = cast;
  }

  @JsonProperty("merge-key-value")
  public String getMergeKeyValue() {
    return mergeKeyValue;
  }

  @JsonProperty("merge-key-value")
  public void setMergeKeyValue(String mergeKeyValue) {
    this.mergeKeyValue = mergeKeyValue;
  }

  @JsonProperty("unwrap-key-value")
  public String getUnwrapKeyValue() {
    return unwrapKeyValue;
  }

  @JsonProperty("unwrap-key-value")
  public void setUnwrapKeyValue(String unwrapKeyValue) {
    this.unwrapKeyValue = unwrapKeyValue;
  }

  @JsonProperty("flatten")
  public String getFlatten() {
    return flatten;
  }

  @JsonProperty("flatten")
  public void setFlatten(String flatten) {
    this.flatten = flatten;
  }

  @JsonProperty("drop")
  public String getDrop() {
    return drop;
  }

  @JsonProperty("drop")
  public void setDrop(String drop) {
    this.drop = drop;
  }

  @JsonProperty("compute-fields")
  public String getComputeFields() {
    return computeFields;
  }

  @JsonProperty("compute-fields")
  public void setComputeFields(String computeFields) {
    this.computeFields = computeFields;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Mapping.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("dropFields");
    sb.append('=');
    sb.append(((this.dropFields == null) ? "<null>" : this.dropFields));
    sb.append(',');
    sb.append("cast");
    sb.append('=');
    sb.append(((this.cast == null) ? "<null>" : this.cast));
    sb.append(',');
    sb.append("mergeKeyValue");
    sb.append('=');
    sb.append(((this.mergeKeyValue == null) ? "<null>" : this.mergeKeyValue));
    sb.append(',');
    sb.append("unwrapKeyValue");
    sb.append('=');
    sb.append(((this.unwrapKeyValue == null) ? "<null>" : this.unwrapKeyValue));
    sb.append(',');
    sb.append("flatten");
    sb.append('=');
    sb.append(((this.flatten == null) ? "<null>" : this.flatten));
    sb.append(',');
    sb.append("drop");
    sb.append('=');
    sb.append(((this.drop == null) ? "<null>" : this.drop));
    sb.append(',');
    sb.append("computeFields");
    sb.append('=');
    sb.append(((this.computeFields == null) ? "<null>" : this.computeFields));
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
    result = ((result * 31) + ((this.flatten == null) ? 0 : this.flatten.hashCode()));
    result = ((result * 31) + ((this.drop == null) ? 0 : this.drop.hashCode()));
    result = ((result * 31) + ((this.cast == null) ? 0 : this.cast.hashCode()));
    result = ((result * 31) + ((this.computeFields == null) ? 0 : this.computeFields.hashCode()));
    result = ((result * 31) + ((this.dropFields == null) ? 0 : this.dropFields.hashCode()));
    result = ((result * 31) + ((this.unwrapKeyValue == null) ? 0 : this.unwrapKeyValue.hashCode()));
    result = ((result * 31) + ((this.mergeKeyValue == null) ? 0 : this.mergeKeyValue.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Mapping) == false) {
      return false;
    }
    Mapping rhs = ((Mapping) other);
    return ((((((((this.flatten == rhs.flatten)
                                || ((this.flatten != null) && this.flatten.equals(rhs.flatten)))
                            && ((this.drop == rhs.drop)
                                || ((this.drop != null) && this.drop.equals(rhs.drop))))
                        && ((this.cast == rhs.cast)
                            || ((this.cast != null) && this.cast.equals(rhs.cast))))
                    && ((this.computeFields == rhs.computeFields)
                        || ((this.computeFields != null)
                            && this.computeFields.equals(rhs.computeFields))))
                && ((this.dropFields == rhs.dropFields)
                    || ((this.dropFields != null) && this.dropFields.equals(rhs.dropFields))))
            && ((this.unwrapKeyValue == rhs.unwrapKeyValue)
                || ((this.unwrapKeyValue != null)
                    && this.unwrapKeyValue.equals(rhs.unwrapKeyValue))))
        && ((this.mergeKeyValue == rhs.mergeKeyValue)
            || ((this.mergeKeyValue != null) && this.mergeKeyValue.equals(rhs.mergeKeyValue))));
  }
}
