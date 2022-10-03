package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "javaType", "required", "properties", "message", "discriminator"})
@Generated("jsonschema2pojo")
public class Step {

  @JsonProperty("type")
  private String type;

  @JsonProperty("javaType")
  private String javaType;

  @JsonProperty("required")
  private List<String> required = new ArrayList<String>();

  @JsonProperty("properties")
  private Properties__1 properties;

  @JsonProperty("message")
  private Message message;

  @JsonProperty("discriminator")
  private Discriminator discriminator;

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("javaType")
  public String getJavaType() {
    return javaType;
  }

  @JsonProperty("javaType")
  public void setJavaType(String javaType) {
    this.javaType = javaType;
  }

  @JsonProperty("required")
  public List<String> getRequired() {
    return required;
  }

  @JsonProperty("required")
  public void setRequired(List<String> required) {
    this.required = required;
  }

  @JsonProperty("properties")
  public Properties__1 getProperties() {
    return properties;
  }

  @JsonProperty("properties")
  public void setProperties(Properties__1 properties) {
    this.properties = properties;
  }

  @JsonProperty("message")
  public Message getMessage() {
    return message;
  }

  @JsonProperty("message")
  public void setMessage(Message message) {
    this.message = message;
  }

  @JsonProperty("discriminator")
  public Discriminator getDiscriminator() {
    return discriminator;
  }

  @JsonProperty("discriminator")
  public void setDiscriminator(Discriminator discriminator) {
    this.discriminator = discriminator;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(Step.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("javaType");
    sb.append('=');
    sb.append(((this.javaType == null) ? "<null>" : this.javaType));
    sb.append(',');
    sb.append("required");
    sb.append('=');
    sb.append(((this.required == null) ? "<null>" : this.required));
    sb.append(',');
    sb.append("properties");
    sb.append('=');
    sb.append(((this.properties == null) ? "<null>" : this.properties));
    sb.append(',');
    sb.append("message");
    sb.append('=');
    sb.append(((this.message == null) ? "<null>" : this.message));
    sb.append(',');
    sb.append("discriminator");
    sb.append('=');
    sb.append(((this.discriminator == null) ? "<null>" : this.discriminator));
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
    result = ((result * 31) + ((this.message == null) ? 0 : this.message.hashCode()));
    result = ((result * 31) + ((this.required == null) ? 0 : this.required.hashCode()));
    result = ((result * 31) + ((this.properties == null) ? 0 : this.properties.hashCode()));
    result = ((result * 31) + ((this.javaType == null) ? 0 : this.javaType.hashCode()));
    result = ((result * 31) + ((this.discriminator == null) ? 0 : this.discriminator.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Step) == false) {
      return false;
    }
    Step rhs = ((Step) other);
    return (((((((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type)))
                        && ((this.message == rhs.message)
                            || ((this.message != null) && this.message.equals(rhs.message))))
                    && ((this.required == rhs.required)
                        || ((this.required != null) && this.required.equals(rhs.required))))
                && ((this.properties == rhs.properties)
                    || ((this.properties != null) && this.properties.equals(rhs.properties))))
            && ((this.javaType == rhs.javaType)
                || ((this.javaType != null) && this.javaType.equals(rhs.javaType))))
        && ((this.discriminator == rhs.discriminator)
            || ((this.discriminator != null) && this.discriminator.equals(rhs.discriminator))));
  }
}
