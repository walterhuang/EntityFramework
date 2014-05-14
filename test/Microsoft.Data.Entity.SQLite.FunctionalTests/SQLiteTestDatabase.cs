// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.FunctionalTests;
using Microsoft.Data.SQLite;

namespace Microsoft.Data.Entity.SQLite.FunctionalTests
{
    public class SQLiteTestDatabase : TestDatabase, IDisposable
    {
        private const string NorthwindDatabaseName = "Northwind";

        private static readonly HashSet<string> _createdDatabases = new HashSet<string>();

        private static readonly ConcurrentDictionary<string, AsyncLock> _creationLocks
            = new ConcurrentDictionary<string, AsyncLock>();

        /// <summary>
        ///     A transactional test database, pre-populated with Northwind schema/data
        /// </summary>
        public static Task<SQLiteTestDatabase> Northwind()
        {
            return new SQLiteTestDatabase()
                .CreateShared(name: NorthwindDatabaseName, filename: @"..\..\..\Northwind.sl3"); // relative from bin/<config>
        }

        private async Task<SQLiteTestDatabase> CreateShared(string name, string filename = null)
        {
            if (!_createdDatabases.Contains(name))
            {
                var asyncLock
                    = _creationLocks.GetOrAdd(name, new AsyncLock());

                using (await asyncLock.LockAsync())
                {
                    if (!_createdDatabases.Contains(name))
                    {
                        _createdDatabases.Add(name);

                        AsyncLock _;
                        _creationLocks.TryRemove(name, out _);
                    }
                }
            }

            _connection = new SQLiteConnection(CreateConnectionString(filename));

            await _connection.OpenAsync();

            _transaction = _connection.BeginTransaction();

            return this;
        }

        private SQLiteConnection _connection;
        private SQLiteTransaction _transaction;

        private SQLiteTestDatabase()
        {
            // Use async static factory method
        }

        public SQLiteConnection Connection
        {
            get { return _connection; }
        }

        private static string CreateConnectionString(string filename)
        {
            // HACK: Probe for script file as current dir
            // is different between k build and VS run.

            if (!File.Exists(filename))
            {
                var kAppBase = Environment.GetEnvironmentVariable("k_appbase");

                if (kAppBase != null)
                {
                    filename = Path.Combine(kAppBase, Path.GetFileName(filename));
                }
            }

            return new SQLiteConnectionStringBuilder
                {
                    Filename = filename
                }.ConnectionString;
        }

        public void Dispose()
        {
            if (_transaction != null)
            {
                _transaction.Dispose();
            }

            _connection.Dispose();
        }
    }
}
