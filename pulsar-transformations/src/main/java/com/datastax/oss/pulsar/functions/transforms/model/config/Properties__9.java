package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "expression", "type", "optional"})
@Generated("jsonschema2pojo")
public class Properties__9 {

  @JsonProperty("name")
  private Name name;

  @JsonProperty("expression")
  private Expression expression;

  @JsonProperty("type")
  private Type__8 type;

  @JsonProperty("optional")
  private Optional optional;

  @JsonProperty("name")
  public Name getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(Name name) {
    this.name = name;
  }

  @JsonProperty("expression")
  public Expression getExpression() {
    return expression;
  }

  @JsonProperty("expression")
  public void setExpression(Expression expression) {
    this.expression = expression;
  }

  @JsonProperty("type")
  public Type__8 getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(Type__8 type) {
    this.type = type;
  }

  @JsonProperty("optional")
  public Optional getOptional() {
    return optional;
  }

  @JsonProperty("optional")
  public void setOptional(Optional optional) {
    this.optional = optional;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Properties__9.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("name");
    sb.append('=');
    sb.append(((this.name == null) ? "<null>" : this.name));
    sb.append(',');
    sb.append("expression");
    sb.append('=');
    sb.append(((this.expression == null) ? "<null>" : this.expression));
    sb.append(',');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("optional");
    sb.append('=');
    sb.append(((this.optional == null) ? "<null>" : this.optional));
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
    result = ((result * 31) + ((this.name == null) ? 0 : this.name.hashCode()));
    result = ((result * 31) + ((this.optional == null) ? 0 : this.optional.hashCode()));
    result = ((result * 31) + ((this.expression == null) ? 0 : this.expression.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Properties__9) == false) {
      return false;
    }
    Properties__9 rhs = ((Properties__9) other);
    return (((((this.name == rhs.name) || ((this.name != null) && this.name.equals(rhs.name)))
                && ((this.optional == rhs.optional)
                    || ((this.optional != null) && this.optional.equals(rhs.optional))))
            && ((this.expression == rhs.expression)
                || ((this.expression != null) && this.expression.equals(rhs.expression))))
        && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))));
  }
}
