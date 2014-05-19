// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using JetBrains.Annotations;
using Microsoft.Data.Entity.InMemory.Utilities;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Query;

namespace Microsoft.Data.Entity.InMemory.Query
{
    public class InMemoryQueryCompilationContext : QueryCompilationContext
    {
        public InMemoryQueryCompilationContext([NotNull] IModel model)
            : base(Check.NotNull(model, "model"))
        {
        }

        public override EntityQueryModelVisitor CreateVisitor()
        {
            return new InMemoryQueryModelVisitor(this);
        }
    }
}
