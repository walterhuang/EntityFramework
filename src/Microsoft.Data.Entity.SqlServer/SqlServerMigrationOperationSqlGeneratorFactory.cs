// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using Microsoft.Data.Entity.Migrations;
using Microsoft.Data.Entity.Relational.Model;

namespace Microsoft.Data.Entity.SqlServer
{
    public class SqlServerMigrationOperationSqlGeneratorFactory : IMigrationOperationSqlGeneratorFactory
    {
        public SqlServerMigrationOperationSqlGenerator Create(DatabaseModel database)
        {
            return new SqlServerMigrationOperationSqlGenerator(new SqlServerTypeMapper()) { Database = database };
        }

        MigrationOperationSqlGenerator IMigrationOperationSqlGeneratorFactory.Create(DatabaseModel database)
        {
            return Create(database);
        }
    }
}
