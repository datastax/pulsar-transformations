package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "Part",
  "Step",
  "DropFields",
  "Cast",
  "UnwrapKeyValue",
  "MergeKeyValue",
  "Drop",
  "Flatten",
  "ComputeFields"
})
@Generated("jsonschema2pojo")
public class Schemas {

  @JsonProperty("Part")
  private Part part;

  @JsonProperty("Step")
  private Step step;

  @JsonProperty("DropFields")
  private DropFields dropFields;

  @JsonProperty("Cast")
  private Cast cast;

  @JsonProperty("UnwrapKeyValue")
  private UnwrapKeyValue unwrapKeyValue;

  @JsonProperty("MergeKeyValue")
  private MergeKeyValue mergeKeyValue;

  @JsonProperty("Drop")
  private Drop drop;

  @JsonProperty("Flatten")
  private Flatten flatten;

  @JsonProperty("ComputeFields")
  private ComputeFields computeFields;

  @JsonProperty("Part")
  public Part getPart() {
    return part;
  }

  @JsonProperty("Part")
  public void setPart(Part part) {
    this.part = part;
  }

  @JsonProperty("Step")
  public Step getStep() {
    return step;
  }

  @JsonProperty("Step")
  public void setStep(Step step) {
    this.step = step;
  }

  @JsonProperty("DropFields")
  public DropFields getDropFields() {
    return dropFields;
  }

  @JsonProperty("DropFields")
  public void setDropFields(DropFields dropFields) {
    this.dropFields = dropFields;
  }

  @JsonProperty("Cast")
  public Cast getCast() {
    return cast;
  }

  @JsonProperty("Cast")
  public void setCast(Cast cast) {
    this.cast = cast;
  }

  @JsonProperty("UnwrapKeyValue")
  public UnwrapKeyValue getUnwrapKeyValue() {
    return unwrapKeyValue;
  }

  @JsonProperty("UnwrapKeyValue")
  public void setUnwrapKeyValue(UnwrapKeyValue unwrapKeyValue) {
    this.unwrapKeyValue = unwrapKeyValue;
  }

  @JsonProperty("MergeKeyValue")
  public MergeKeyValue getMergeKeyValue() {
    return mergeKeyValue;
  }

  @JsonProperty("MergeKeyValue")
  public void setMergeKeyValue(MergeKeyValue mergeKeyValue) {
    this.mergeKeyValue = mergeKeyValue;
  }

  @JsonProperty("Drop")
  public Drop getDrop() {
    return drop;
  }

  @JsonProperty("Drop")
  public void setDrop(Drop drop) {
    this.drop = drop;
  }

  @JsonProperty("Flatten")
  public Flatten getFlatten() {
    return flatten;
  }

  @JsonProperty("Flatten")
  public void setFlatten(Flatten flatten) {
    this.flatten = flatten;
  }

  @JsonProperty("ComputeFields")
  public ComputeFields getComputeFields() {
    return computeFields;
  }

  @JsonProperty("ComputeFields")
  public void setComputeFields(ComputeFields computeFields) {
    this.computeFields = computeFields;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Schemas.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("part");
    sb.append('=');
    sb.append(((this.part == null) ? "<null>" : this.part));
    sb.append(',');
    sb.append("step");
    sb.append('=');
    sb.append(((this.step == null) ? "<null>" : this.step));
    sb.append(',');
    sb.append("dropFields");
    sb.append('=');
    sb.append(((this.dropFields == null) ? "<null>" : this.dropFields));
    sb.append(',');
    sb.append("cast");
    sb.append('=');
    sb.append(((this.cast == null) ? "<null>" : this.cast));
    sb.append(',');
    sb.append("unwrapKeyValue");
    sb.append('=');
    sb.append(((this.unwrapKeyValue == null) ? "<null>" : this.unwrapKeyValue));
    sb.append(',');
    sb.append("mergeKeyValue");
    sb.append('=');
    sb.append(((this.mergeKeyValue == null) ? "<null>" : this.mergeKeyValue));
    sb.append(',');
    sb.append("drop");
    sb.append('=');
    sb.append(((this.drop == null) ? "<null>" : this.drop));
    sb.append(',');
    sb.append("flatten");
    sb.append('=');
    sb.append(((this.flatten == null) ? "<null>" : this.flatten));
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
    result = ((result * 31) + ((this.drop == null) ? 0 : this.drop.hashCode()));
    result = ((result * 31) + ((this.flatten == null) ? 0 : this.flatten.hashCode()));
    result = ((result * 31) + ((this.cast == null) ? 0 : this.cast.hashCode()));
    result = ((result * 31) + ((this.computeFields == null) ? 0 : this.computeFields.hashCode()));
    result = ((result * 31) + ((this.part == null) ? 0 : this.part.hashCode()));
    result = ((result * 31) + ((this.dropFields == null) ? 0 : this.dropFields.hashCode()));
    result = ((result * 31) + ((this.step == null) ? 0 : this.step.hashCode()));
    result = ((result * 31) + ((this.unwrapKeyValue == null) ? 0 : this.unwrapKeyValue.hashCode()));
    result = ((result * 31) + ((this.mergeKeyValue == null) ? 0 : this.mergeKeyValue.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Schemas) == false) {
      return false;
    }
    Schemas rhs = ((Schemas) other);
    return ((((((((((this.drop == rhs.drop) || ((this.drop != null) && this.drop.equals(rhs.drop)))
                                    && ((this.flatten == rhs.flatten)
                                        || ((this.flatten != null)
                                            && this.flatten.equals(rhs.flatten))))
                                && ((this.cast == rhs.cast)
                                    || ((this.cast != null) && this.cast.equals(rhs.cast))))
                            && ((this.computeFields == rhs.computeFields)
                                || ((this.computeFields != null)
                                    && this.computeFields.equals(rhs.computeFields))))
                        && ((this.part == rhs.part)
                            || ((this.part != null) && this.part.equals(rhs.part))))
                    && ((this.dropFields == rhs.dropFields)
                        || ((this.dropFields != null) && this.dropFields.equals(rhs.dropFields))))
                && ((this.step == rhs.step) || ((this.step != null) && this.step.equals(rhs.step))))
            && ((this.unwrapKeyValue == rhs.unwrapKeyValue)
                || ((this.unwrapKeyValue != null)
                    && this.unwrapKeyValue.equals(rhs.unwrapKeyValue))))
        && ((this.mergeKeyValue == rhs.mergeKeyValue)
            || ((this.mergeKeyValue != null) && this.mergeKeyValue.equals(rhs.mergeKeyValue))));
  }
}
