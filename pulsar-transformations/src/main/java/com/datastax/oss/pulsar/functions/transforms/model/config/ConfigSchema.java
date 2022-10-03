package com.datastax.oss.pulsar.functions.transforms.model.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"$schema", "components", "type", "properties", "required"})
@Generated("jsonschema2pojo")
public class ConfigSchema {

  @JsonProperty("$schema")
  private String $schema;

  @JsonProperty("components")
  private Components components;

  @JsonProperty("type")
  private String type;

  @JsonProperty("properties")
  private Properties__10 properties;

  @JsonProperty("required")
  private List<String> required = new ArrayList<String>();

  @JsonProperty("$schema")
  public String get$schema() {
    return $schema;
  }

  @JsonProperty("$schema")
  public void set$schema(String $schema) {
    this.$schema = $schema;
  }

  @JsonProperty("components")
  public Components getComponents() {
    return components;
  }

  @JsonProperty("components")
  public void setComponents(Components components) {
    this.components = components;
  }

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  @JsonProperty("properties")
  public Properties__10 getProperties() {
    return properties;
  }

  @JsonProperty("properties")
  public void setProperties(Properties__10 properties) {
    this.properties = properties;
  }

  @JsonProperty("required")
  public List<String> getRequired() {
    return required;
  }

  @JsonProperty("required")
  public void setRequired(List<String> required) {
    this.required = required;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(ConfigSchema.class.getName())
        .append('@')
        .append(Integer.toHexString(System.identityHashCode(this)))
        .append('[');
    sb.append("$schema");
    sb.append('=');
    sb.append(((this.$schema == null) ? "<null>" : this.$schema));
    sb.append(',');
    sb.append("components");
    sb.append('=');
    sb.append(((this.components == null) ? "<null>" : this.components));
    sb.append(',');
    sb.append("type");
    sb.append('=');
    sb.append(((this.type == null) ? "<null>" : this.type));
    sb.append(',');
    sb.append("properties");
    sb.append('=');
    sb.append(((this.properties == null) ? "<null>" : this.properties));
    sb.append(',');
    sb.append("required");
    sb.append('=');
    sb.append(((this.required == null) ? "<null>" : this.required));
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
    result = ((result * 31) + ((this.components == null) ? 0 : this.components.hashCode()));
    result = ((result * 31) + ((this.$schema == null) ? 0 : this.$schema.hashCode()));
    result = ((result * 31) + ((this.type == null) ? 0 : this.type.hashCode()));
    result = ((result * 31) + ((this.properties == null) ? 0 : this.properties.hashCode()));
    result = ((result * 31) + ((this.required == null) ? 0 : this.required.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof ConfigSchema) == false) {
      return false;
    }
    ConfigSchema rhs = ((ConfigSchema) other);
    return ((((((this.components == rhs.components)
                        || ((this.components != null) && this.components.equals(rhs.components)))
                    && ((this.$schema == rhs.$schema)
                        || ((this.$schema != null) && this.$schema.equals(rhs.$schema))))
                && ((this.type == rhs.type) || ((this.type != null) && this.type.equals(rhs.type))))
            && ((this.properties == rhs.properties)
                || ((this.properties != null) && this.properties.equals(rhs.properties))))
        && ((this.required == rhs.required)
            || ((this.required != null) && this.required.equals(rhs.required))));
  }
}
