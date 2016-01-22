/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the {@link RelMetadataProvider}
 * interface via the
 * {@link org.apache.calcite.util.Glossary#CHAIN_OF_RESPONSIBILITY_PATTERN}.
 *
 * <p>When a consumer calls the {@link #apply} method to ask for a provider
 * for a particular type of {@link RelNode} and {@link Metadata}, scans the list
 * of underlying providers.</p>
 */
public class ChainedRelMetadataProvider implements RelMetadataProvider {
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelMetadataProvider> providers;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a chain.
   */
  protected ChainedRelMetadataProvider(
      ImmutableList<RelMetadataProvider> providers) {
    this.providers = providers;
  }

  //~ Methods ----------------------------------------------------------------

  public <M extends Metadata> UnboundMetadata<M>
  apply(final Class<? extends RelNode> relClass,
      final Class<? extends M> metadataClass) {
    if (relClass == LogicalAggregate.class
        && metadataClass == BuiltInMetadata.ColumnUniqueness.class) {
      return RelMdColumnUniqueness.SOURCE2.apply(relClass, metadataClass);
    }
    final List<UnboundMetadata<M>> functions = new ArrayList<>();
    for (RelMetadataProvider provider : providers) {
      final UnboundMetadata<M> function =
          provider.apply(relClass, metadataClass);
      if (function == null) {
        continue;
      }
      functions.add(function);
    }
    switch (functions.size()) {
    case 0:
      return null;
    case 1:
      return functions.get(0);
    default:
      return new UnboundMetadata<M>() {
        public M bind(RelNode rel, RelMetadataQuery mq) {
          final List<Metadata> metadataList = Lists.newArrayList();
          for (UnboundMetadata<M> function : functions) {
            final Metadata metadata = function.bind(rel, mq);
            if (metadata != null) {
              metadataList.add(metadata);
            }
          }
          return metadataClass.cast(
              Proxy.newProxyInstance(metadataClass.getClassLoader(),
                  new Class[]{metadataClass},
                  new ChainedInvocationHandler(metadataList)));
        }
      };
    }
  }

  /** Creates a chain. */
  public static RelMetadataProvider of(List<RelMetadataProvider> list) {
    return new ChainedRelMetadataProvider(ImmutableList.copyOf(list));
  }

  /** Invocation handler that calls a list of {@link Metadata} objects,
   * returning the first non-null value. */
  private static class ChainedInvocationHandler implements InvocationHandler {
    private final List<Metadata> metadataList;

    public ChainedInvocationHandler(List<Metadata> metadataList) {
      this.metadataList = ImmutableList.copyOf(metadataList);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      for (Metadata metadata : metadataList) {
        try {
          final Object o = method.invoke(metadata, args);
          if (o != null) {
            return o;
          }
        } catch (InvocationTargetException e) {
          if (e.getCause() instanceof CyclicMetadataException) {
            continue;
          }
          Throwables.propagateIfPossible(e.getCause());
          throw e;
        }
      }
      return null;
    }
  }
}

// End ChainedRelMetadataProvider.java
