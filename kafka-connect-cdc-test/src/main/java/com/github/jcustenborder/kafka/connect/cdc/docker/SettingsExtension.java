/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.cdc.docker;

import com.palantir.docker.compose.DockerComposeRule;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.List;

public abstract class SettingsExtension implements ParameterResolver {
  private static Logger log = LoggerFactory.getLogger(SettingsExtension.class);

  protected abstract List<Class<? extends Annotation>> annotationClasses();

  @Override
  public boolean supports(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    boolean result = false;
    for (Class<? extends Annotation> annotationClass : annotationClasses()) {
      log.trace("Checking if {} is annotated with {}.", parameterContext.getDeclaringExecutable().getName(), annotationClass.getName());
      if (parameterContext.getParameter().isAnnotationPresent(annotationClass)) {
        log.trace("Found {} on {}.", annotationClass.getName(), parameterContext.getDeclaringExecutable().getName());
        result = true;
        break;
      }
    }

    return result;
  }

  @Override
  public Object resolve(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
    ExtensionContext.Namespace namespace = DockerExtension.namespace(extensionContext);
    ExtensionContext.Store store = extensionContext.getStore(namespace);
    DockerComposeRule dockerComposeRule = store.get(DockerExtension.STORE_SLOT_RULE, DockerComposeRule.class);

    Object result = null;

    for (Class<? extends Annotation> annotationClass : annotationClasses()) {
      if (!parameterContext.getParameter().isAnnotationPresent(annotationClass)) {
        log.trace("skipping {}", annotationClass.getName());
        continue;
      }

      Annotation annotation = parameterContext.getParameter().getAnnotation(annotationClass);
      result = handleResolve(parameterContext, extensionContext, annotation, dockerComposeRule);
      break;
    }

    return result;
  }

  protected abstract Object handleResolve(ParameterContext parameterContext, ExtensionContext extensionContext, Annotation annotation, DockerComposeRule docker) throws ParameterResolutionException;
}
