/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.transforms.jstl;

import jakarta.el.ArrayELResolver;
import jakarta.el.BeanNameELResolver;
import jakarta.el.BeanNameResolver;
import jakarta.el.CompositeELResolver;
import jakarta.el.ELContext;
import jakarta.el.ELResolver;
import jakarta.el.ExpressionFactory;
import jakarta.el.FunctionMapper;
import jakarta.el.ListELResolver;
import jakarta.el.MapELResolver;
import jakarta.el.PropertyNotWritableException;
import jakarta.el.ResourceBundleELResolver;
import jakarta.el.StaticFieldELResolver;
import jakarta.el.ValueExpression;
import jakarta.el.VariableMapper;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * A standard context that mirrors {@link jakarta.el.StandardELContext} with the exception that it
 * registers a custom beans resolver that disables invocations.
 */
public class StandardContext extends ELContext {
  private final ELContext wrappedContext;
  private final VariableMapper variableMapper;
  private final FunctionMapper functionMapper;
  private final CompositeELResolver standardResolver;
  private final CompositeELResolver customResolvers;
  private final Map<String, Object> localBeans = new HashMap<>();

  public StandardContext(ExpressionFactory factory) {
    this.wrappedContext = null;
    this.variableMapper = new StandardVariableMapper();
    this.functionMapper = new StandardFunctionMapper(factory.getInitFunctionMap());
    this.standardResolver = new CompositeELResolver();
    this.customResolvers = new CompositeELResolver();
    ELResolver streamResolver = factory.getStreamELResolver();
    this.standardResolver.add(
        new BeanNameELResolver(new StandardBeanNameResolver(this.localBeans)));
    this.standardResolver.add(this.customResolvers);
    if (streamResolver != null) {
      this.standardResolver.add(streamResolver);
    }

    this.standardResolver.add(new StaticFieldELResolver());
    this.standardResolver.add(new MapELResolver());
    this.standardResolver.add(new ResourceBundleELResolver());
    this.standardResolver.add(new ListELResolver());
    this.standardResolver.add(new ArrayELResolver());
    this.standardResolver.add(new DisabledInvocationBeanResolver());
  }

  public void putContext(Class<?> key, Object contextObject) {
    if (this.wrappedContext == null) {
      super.putContext(key, contextObject);
    } else {
      this.wrappedContext.putContext(key, contextObject);
    }
  }

  public Object getContext(Class<?> key) {
    return this.wrappedContext == null
        ? super.getContext(key)
        : this.wrappedContext.getContext(key);
  }

  public ELResolver getELResolver() {
    return this.standardResolver;
  }

  public void addELResolver(ELResolver resolver) {
    this.customResolvers.add(resolver);
  }

  public FunctionMapper getFunctionMapper() {
    return this.functionMapper;
  }

  public VariableMapper getVariableMapper() {
    return this.variableMapper;
  }

  private static class StandardFunctionMapper extends FunctionMapper {
    private final Map<String, Method> methods = new HashMap<>();

    public StandardFunctionMapper(Map<String, Method> initFunctionMap) {
      if (initFunctionMap != null) {
        this.methods.putAll(initFunctionMap);
      }
    }

    public Method resolveFunction(String prefix, String localName) {
      String key = prefix + ":" + localName;
      return this.methods.get(key);
    }

    public void mapFunction(String prefix, String localName, Method method) {
      String key = prefix + ":" + localName;
      if (method == null) {
        this.methods.remove(key);
      } else {
        this.methods.put(key, method);
      }
    }
  }

  private static class StandardBeanNameResolver extends BeanNameResolver {
    private final Map<String, Object> beans;

    public StandardBeanNameResolver(Map<String, Object> beans) {
      this.beans = beans;
    }

    public boolean isNameResolved(String beanName) {
      return this.beans.containsKey(beanName);
    }

    public Object getBean(String beanName) {
      return this.beans.get(beanName);
    }

    public void setBeanValue(String beanName, Object value) throws PropertyNotWritableException {
      this.beans.put(beanName, value);
    }

    public boolean isReadOnly(String beanName) {
      return false;
    }

    public boolean canCreateBean(String beanName) {
      return true;
    }
  }

  private static class StandardVariableMapper extends VariableMapper {
    private Map<String, ValueExpression> vars;

    private StandardVariableMapper() {}

    public ValueExpression resolveVariable(String variable) {
      return this.vars == null ? null : this.vars.get(variable);
    }

    public ValueExpression setVariable(String variable, ValueExpression expression) {
      if (this.vars == null) {
        this.vars = new HashMap<>();
      }

      return expression == null ? this.vars.remove(variable) : this.vars.put(variable, expression);
    }
  }
}
