// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using JetBrains.Annotations;
using Microsoft.Data.Entity.Infrastructure;
using Microsoft.Data.Entity.Relational;
using Microsoft.Data.Entity.Relational.Update;

namespace Microsoft.Data.Entity.SqlServer
{
    public class SqlServerDataStore : RelationalDataStore
    {
        public SqlServerDataStore(
            [NotNull] DbContextConfiguration configuration,
            [NotNull] SqlServerConnection connection,
            [NotNull] CommandBatchPreparer batchPreparer,
            [NotNull] SqlServerBatchExecutor batchExecutor)
            : base(configuration, connection, batchPreparer, batchExecutor)
        {
        }

        protected override RelationalValueReaderFactory ValueReaderFactory
        {
            get { return new RelationalObjectArrayValueReaderFactory(); }
        }
    }
}
