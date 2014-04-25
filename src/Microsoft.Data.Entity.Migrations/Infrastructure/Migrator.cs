// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Infrastructure;
using Microsoft.Data.Entity.Migrations.Utilities;
using Microsoft.Data.Entity.Relational;

namespace Microsoft.Data.Entity.Migrations.Infrastructure
{
    public class Migrator
    {
        private readonly DbContextConfiguration _contextConfiguration;
        private readonly HistoryRepository _historyRepository;
        private readonly MigrationAssembly _migrationAssembly;
        private readonly DatabaseBuilder _databaseBuilder;
        private readonly IMigrationOperationSqlGeneratorFactory _ddlSqlGeneratorFactory;
        private readonly SqlGenerator _dmlSqlGenerator;
        private readonly SqlStatementExecutor _sqlExecutor;        

        public Migrator(
            [NotNull] DbContextConfiguration contextConfiguration,
            [NotNull] HistoryRepository historyRepository,
            [NotNull] MigrationAssembly migrationAssembly,
            [NotNull] DatabaseBuilder databaseBuilder,
            [NotNull] IMigrationOperationSqlGeneratorFactory ddlSqlGeneratorFactory,
            [NotNull] SqlGenerator dmlSqlGenerator,
            [NotNull] SqlStatementExecutor sqlExecutor)
        {
            Check.NotNull(contextConfiguration, "contextConfiguration");
            Check.NotNull(historyRepository, "historyRepository");
            Check.NotNull(migrationAssembly, "migrationAssembly");
            Check.NotNull(databaseBuilder, "databaseBuilder");
            Check.NotNull(ddlSqlGeneratorFactory, "ddlSqlGeneratorFactory");
            Check.NotNull(dmlSqlGenerator, "dmlSqlGenerator");
            Check.NotNull(sqlExecutor, "sqlExecutor");

            _contextConfiguration = contextConfiguration;
            _historyRepository = historyRepository;
            _migrationAssembly = migrationAssembly;
            _databaseBuilder = databaseBuilder;
            _ddlSqlGeneratorFactory = ddlSqlGeneratorFactory;
            _dmlSqlGenerator = dmlSqlGenerator;
            _sqlExecutor = sqlExecutor;
        }

        public virtual DbContextConfiguration ContextConfiguration
        {
            get { return _contextConfiguration; }
        }

        public virtual HistoryRepository HistoryRepository
        {
            get { return _historyRepository; }
        }

        public virtual MigrationAssembly MigrationAssembly
        {
            get { return _migrationAssembly; }
        }

        public virtual DatabaseBuilder DatabaseBuilder
        {
            get { return _databaseBuilder; }
        }

        public virtual IMigrationOperationSqlGeneratorFactory DdlSqlGeneratorFactory
        {
            get { return _ddlSqlGeneratorFactory; }
        }

        public virtual SqlGenerator DmlSqlGenerator
        {
            get { return _dmlSqlGenerator; }
        }

        public virtual SqlStatementExecutor SqlExecutor
        {
            get { return _sqlExecutor; }
        }

        public virtual void UpdateDatabase()
        {
            UpdateDatabase(GenerateUpdateDatabaseSql());
        }

        public virtual void UpdateDatabase(string targetMigrationName)
        {
            UpdateDatabase(GenerateUpdateDatabaseSql(targetMigrationName));
        }

        protected virtual void UpdateDatabase(IReadOnlyList<SqlStatement> sqlStatements)
        {
            var dbConnection = ((RelationalConnection)_contextConfiguration.Connection).DbConnection;
            _sqlExecutor.ExecuteNonQuery(dbConnection, sqlStatements);
        }

        public virtual IReadOnlyList<SqlStatement> GenerateUpdateDatabaseSql()
        {
            var migrations
                = MigrationAssembly.Migrations
                    .Except(HistoryRepository.Migrations, (lm, dm) => lm.Name == dm.Name)
                    .ToArray();

            return GenerateUpdateDatabaseSql(migrations, downgrade: false);
        }

        public virtual IReadOnlyList<SqlStatement> GenerateUpdateDatabaseSql(string targetMigrationName)
        {
            var skip = true;
            var migrations
                = HistoryRepository.Migrations
                    .SkipWhile(
                        lm =>
                            {
                                if (lm.Name != targetMigrationName)
                                    return skip;

                                skip = false;
                                return true;
                            })
                    .Select(dm => MigrationAssembly.Migrations.Single(lm => lm.Name == dm.Name))
                    .ToArray();

            if (migrations.Any())
            {
                return GenerateUpdateDatabaseSql(migrations, downgrade: true);
            }

            var take = true;
            migrations 
                = MigrationAssembly.Migrations
                    .TakeWhile(
                        lm =>
                            {
                                if (lm.Name != targetMigrationName)
                                    return take;

                                take = false;
                                return true;
                            })
                    .Except(HistoryRepository.Migrations, (lm, dm) => lm.Name == dm.Name)
                    .ToArray();

            if (migrations.Any())
            {
                return GenerateUpdateDatabaseSql(migrations, downgrade: false);
            }

            // TODO: Review and add error message to resources.
            throw new ArgumentException("Target migration not found");
        }

        protected virtual IReadOnlyList<SqlStatement> GenerateUpdateDatabaseSql(
            IReadOnlyList<IMigrationMetadata> migrations, bool downgrade)
        {
            var sqlStatements = new List<SqlStatement>();

            foreach (var migration in migrations)
            {
                var database = DatabaseBuilder.GetDatabase(migration.TargetModel);
                var sqlGenerator = DdlSqlGeneratorFactory.Create(database);

                sqlStatements.AddRange(
                    sqlGenerator.Generate(
                        downgrade
                            ? migration.DowngradeOperations
                            : migration.UpgradeOperations,
                        generateIdempotentSql: true));

                sqlStatements.AddRange(
                    downgrade 
                        ? HistoryRepository.GenerateDeleteStatements(migration, DmlSqlGenerator)
                        : HistoryRepository.GenerateInsertStatements(migration, DmlSqlGenerator));
            }

            return sqlStatements;
        }
    }
}
