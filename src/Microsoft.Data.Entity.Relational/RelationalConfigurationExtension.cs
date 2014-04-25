// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Data.Common;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Infrastructure;
using Microsoft.Data.Entity.Relational.Utilities;
using System.Reflection;

namespace Microsoft.Data.Entity.Relational
{
    public abstract class RelationalConfigurationExtension : EntityConfigurationExtension
    {
        private string _connectionString;
        private DbConnection _connection;
        private Assembly _migrationAssembly;
        private string _migrationNamespace;

        public virtual string ConnectionString
        {
            get { return _connectionString; }

            [param: NotNull]
            set
            {
                Check.NotEmpty(value, "value");

                _connectionString = value;
            }
        }

        public virtual DbConnection Connection
        {
            get { return _connection; }
            [param: NotNull]
            set
            {
                Check.NotNull(value, "value");

                _connection = value;
            }
        }

        public virtual Assembly MigrationAssembly
        {
            get { return _migrationAssembly; }

            [param: NotNull]
            set { _migrationAssembly = Check.NotNull(value, "value"); }
        }

        public virtual string MigrationNamespace
        {
            get { return _migrationNamespace; }

            [param: NotNull]
            set { _migrationNamespace = Check.NotEmpty(value, "value"); }
        }
    }
}
