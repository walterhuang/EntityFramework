// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Linq;
using System.Text;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Infrastructure;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Migrations.Utilities;
using Microsoft.Data.Entity.Relational;

namespace Microsoft.Data.Entity.Migrations.Infrastructure
{
    public class HistoryRepository
    {
        private readonly DbContextConfiguration _contextConfiguration;
        private IModel _historyModel;
        private bool _databaseExists;

        public HistoryRepository([NotNull] DbContextConfiguration contextConfiguration)
        {
            Check.NotNull(contextConfiguration, "contextConfiguration");

            _contextConfiguration = contextConfiguration;
        }

        public virtual DbContextConfiguration ContextConfiguration
        {
            get { return _contextConfiguration; }
        }

        public virtual SchemaQualifiedName TableName
        {
            get { return "__MigrationHistory"; }
        }

        public virtual IModel HistoryModel
        {
            get { return _historyModel ?? (_historyModel = CreateHistoryModel()); }
        }

        protected virtual IModel CreateHistoryModel()
        {
            var builder = new ModelBuilder();

            builder
                .Entity<HistoryRow>()
                .ToTable(TableName)
                .Properties(
                    ps =>
                    {
                        // TODO: Add column constraints (FixedLength, MaxLength) where needed.
                        ps.Property(e => e.MigrationName);
                        ps.Property(e => e.Timestamp);
                        ps.Property(e => e.ContextKey);
                    })
                // TODO: Key should be {e.MigrationName, e.ContextKey} but composite keys 
                // are not implemented yet.
                .Key(e => new { e.MigrationName });

            return builder.Model;
        }

        public virtual DbContext CreateHistoryContext()
        {
            var contextOptions = new DbContextOptions().UseModel(HistoryModel);

            // TODO: Revisit and decide whether it is ok to use the extension instances 
            // from the user context for the history context.
            foreach (var item in ContextConfiguration.ContextOptions.Extensions)
            {
                var extension = item;
                contextOptions.AddBuildAction(c => c.AddOrUpdateExtension(extension));
            }

            var context = new DbContext(
                ContextConfiguration.Services.ServiceProvider,
                contextOptions.BuildConfiguration());

            if (!_databaseExists && !context.Database.Exists())
            {
                context.Database.Create();
                context.Database.CreateTables();

                _databaseExists = true;
            }

            return context;
        }

        public virtual IReadOnlyList<IMigrationMetadata> Migrations
        {
            get
            {
                using (var historyContext = CreateHistoryContext())
                {
                    return historyContext.Set<HistoryRow>()
                        .Where(h => h.ContextKey == GetContextKey())
                        .Select(h => new MigrationMetadata(h.MigrationName, h.Timestamp))
                        .OrderBy(m => m.Timestamp)
                        .ToArray();
                }
            }
        }

        public virtual IReadOnlyList<SqlStatement> GenerateInsertStatements(
            [NotNull] IMigrationMetadata migration, [NotNull] SqlGenerator sqlGenerator)
        {
            Check.NotNull(migration, "migration");
            Check.NotNull(sqlGenerator, "sqlGenerator");

            // TODO: Figure out what needs to be done to fully generate the INSERT statement 
            // below using the DML SQL generator.

            var stringBuilder = new StringBuilder();
            stringBuilder
                .Append("INSERT INTO ")
                .Append(sqlGenerator.DelimitIdentifier(TableName))
                .Append("(")
                .Append(sqlGenerator.DelimitIdentifier("MigrationName"))
                .Append(", ")
                .Append(sqlGenerator.DelimitIdentifier("Timestamp"))
                .Append(", ")
                .Append(sqlGenerator.DelimitIdentifier("ContextKey"))
                .Append(") VALUES (")
                .Append(sqlGenerator.GenerateLiteral(migration.Name))
                .Append(", ")
                .Append(sqlGenerator.GenerateLiteral(migration.Timestamp))
                .Append(", ")
                .Append(sqlGenerator.GenerateLiteral(GetContextKey()))
                .Append(")");

            return new[] { new SqlStatement(stringBuilder.ToString()) };
        }

        public virtual IReadOnlyList<SqlStatement> GenerateDeleteStatements(
            [NotNull] IMigrationMetadata migration, [NotNull] SqlGenerator sqlGenerator)
        {
            Check.NotNull(migration, "migration");
            Check.NotNull(sqlGenerator, "sqlGenerator");

            // TODO: Figure out what needs to be done to fully generate the DELETE statement 
            // below using the DML SQL generator.

            var stringBuilder = new StringBuilder();
            stringBuilder
                .Append("DELETE FROM ")
                .Append(sqlGenerator.DelimitIdentifier(TableName))
                .Append(" WHERE Timestamp = ")
                .Append(sqlGenerator.GenerateLiteral(migration.Timestamp));

            return new[] { new SqlStatement(stringBuilder.ToString()) };
        }

        protected virtual string GetContextKey()
        {
            return ContextConfiguration.Context.GetType().Name;
        }

        private class HistoryRow
        {
            public string MigrationName { get; set; }
            public string Timestamp { get; set; }
            public string ContextKey { get; set; }            
        }
    }
}
