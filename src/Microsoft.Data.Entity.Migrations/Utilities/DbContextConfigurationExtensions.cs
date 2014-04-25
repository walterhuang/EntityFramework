// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq;
using System.Reflection;
using Microsoft.Data.Entity.Infrastructure;
using Microsoft.Data.Entity.Relational;

namespace Microsoft.Data.Entity.Migrations.Utilities
{
    internal static class DbContextConfigurationExtensions
    {
        public static T GetService<T>(this DbContextConfiguration configuration)
        {
            return (T)configuration.Services.ServiceProvider.GetService(typeof(T));
        }

        public static T GetExtension<T>(this DbContextConfiguration configuration)
            where T : EntityConfigurationExtension
        {
            return configuration.ContextOptions.Extensions.OfType<T>().Single();
        }

        public static Assembly GetMigrationAssembly(this DbContextConfiguration configuration)
        {
            return configuration.GetExtension<RelationalConfigurationExtension>().MigrationAssembly
                ?? configuration.Context.GetType().GetTypeInfo().Assembly;
        }

        public static string GetMigrationNamespace(this DbContextConfiguration configuration)
        {
            return configuration.GetExtension<RelationalConfigurationExtension>().MigrationNamespace
                ?? configuration.Context.GetType().Namespace + ".Migrations";
        }
    }
}
