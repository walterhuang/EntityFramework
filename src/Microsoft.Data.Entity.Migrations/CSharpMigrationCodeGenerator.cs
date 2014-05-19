// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Migrations.Infrastructure;
using Microsoft.Data.Entity.Migrations.Model;
using Microsoft.Data.Entity.Migrations.Utilities;
using Microsoft.Data.Entity.Relational.Model;
using Microsoft.Data.Entity.Utilities;

namespace Microsoft.Data.Entity.Migrations
{
    public class CSharpMigrationCodeGenerator : MigrationCodeGenerator
    {
        public CSharpMigrationCodeGenerator()
            : this(new CSharpModelCodeGenerator())
        {
        }

        public CSharpMigrationCodeGenerator(CSharpModelCodeGenerator modelCodeGenerator)
            : base(modelCodeGenerator)
        {
        }

        public override string CodeFileExtension
        {
            get { return ".cs"; }
        }

        public static string Generate<T>([NotNull] T migrationOperation)
            where T : MigrationOperation
        {
            var generator = new CSharpMigrationCodeGenerator();
            var stringBuilder = new IndentedStringBuilder();

            migrationOperation.GenerateCode(generator, stringBuilder);

            return stringBuilder.ToString();
        }

        public override void GenerateClass(
            string @namespace,
            string className,
            IMigrationMetadata migration,
            IndentedStringBuilder stringBuilder)
        {
            var operations = migration.UpgradeOperations.Concat(migration.DowngradeOperations);

            foreach (var ns in GetNamespaces(operations).OrderBy(n => n).Distinct())
            {
                stringBuilder
                    .Append("using ")
                    .Append(ns)
                    .AppendLine(";");
            }

            stringBuilder
                .AppendLine()
                .Append("namespace ")
                .AppendLine(@namespace)
                .AppendLine("{");

            using (stringBuilder.Indent())
            {
                stringBuilder
                    .Append("public partial class ")
                    .Append(className)
                    .AppendLine(" : Migration")
                    .AppendLine("{");

                using (stringBuilder.Indent())
                {
                    GenerateMigrationMethod("Up", migration.UpgradeOperations, stringBuilder);

                    stringBuilder.AppendLine().AppendLine();

                    GenerateMigrationMethod("Down", migration.DowngradeOperations, stringBuilder);
                }

                stringBuilder
                    .AppendLine()
                    .Append("}");
            }

            stringBuilder
                .AppendLine()
                .Append("}");
        }

        public override void GenerateDesignerClass(
            string @namespace,
            string className,
            IMigrationMetadata migration,
            IndentedStringBuilder stringBuilder)
        {
            foreach (var ns in GetMetadataDefaultNamespaces()
                .Concat(ModelCodeGenerator.GetNamespaces(migration.TargetModel))
                .OrderBy(n => n)
                .Distinct())
            {
                stringBuilder
                    .Append("using ")
                    .Append(ns)
                    .AppendLine(";");
            }

            stringBuilder
                .AppendLine()
                .Append("namespace ")
                .AppendLine(@namespace)
                .AppendLine("{");            

            using (stringBuilder.Indent())
            {
                stringBuilder
                    .Append("public partial class ")
                    .Append(className)
                    .AppendLine(" : IMigrationMetadata")
                    .AppendLine("{");

                using (stringBuilder.Indent())
                {
                    GenerateMigrationProperty(
                        "string IMigrationMetadata.Name", 
                        () => stringBuilder
                            .Append("return ")
                            .Append(GenerateLiteral(migration.Name))
                            .Append(";"), 
                        stringBuilder);

                    stringBuilder.AppendLine().AppendLine();

                    GenerateMigrationProperty(
                        "string IMigrationMetadata.Timestamp",
                        () => stringBuilder
                            .Append("return ")
                            .Append(GenerateLiteral(migration.Timestamp))
                            .Append(";"),
                        stringBuilder);

                    stringBuilder.AppendLine().AppendLine();

                    GenerateMigrationProperty(
                        "IModel IMigrationMetadata.TargetModel",
                        () => ModelCodeGenerator.Generate(migration.TargetModel, stringBuilder),
                        stringBuilder);
                }

                stringBuilder
                    .AppendLine()
                    .Append("}");
            }

            stringBuilder
                .AppendLine()
                .Append("}");            
        }

        protected virtual void GenerateMigrationMethod(
            [NotNull] string methodName,
            [NotNull] IReadOnlyList<MigrationOperation> migrationOperations,
            [NotNull] IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(methodName, "methodName");
            Check.NotNull(migrationOperations, "migrationOperations");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("public override void ")
                .Append(methodName)
                .AppendLine("(MigrationBuilder migrationBuilder)")
                .AppendLine("{");

            using (stringBuilder.Indent())
            {
                foreach (var operation in migrationOperations)
                {
                    stringBuilder.Append("migrationBuilder.");

                    operation.GenerateCode(this, stringBuilder);

                    stringBuilder.AppendLine(";");
                }
            }

            stringBuilder.Append("}");
        }

        protected virtual void GenerateMigrationProperty(
            [NotNull] string signature,
            [NotNull] Action generateCode,
            [NotNull] IndentedStringBuilder stringBuilder)
        {
            stringBuilder
                .AppendLine(signature)
                .AppendLine("{");

            using (stringBuilder.Indent())
            {
                stringBuilder
                    .AppendLine("get")
                    .AppendLine("{");

                using (stringBuilder.Indent())
                {
                    generateCode();
                }

                stringBuilder
                    .AppendLine()
                    .AppendLine("}");
            }

            stringBuilder.Append("}");
        }

        public override void Generate(CreateDatabaseOperation createDatabaseOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(createDatabaseOperation, "createDatabaseOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("CreateDatabase(")
                .Append(GenerateLiteral(createDatabaseOperation.DatabaseName))
                .Append(")");
        }

        public override void Generate(DropDatabaseOperation dropDatabaseOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropDatabaseOperation, "dropDatabaseOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropDatabase(")
                .Append(GenerateLiteral(dropDatabaseOperation.DatabaseName))
                .Append(")");
        }

        public override void Generate(CreateSequenceOperation createSequenceOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(createSequenceOperation, "createSequenceOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            var sequence = createSequenceOperation.Sequence;

            stringBuilder
                .Append("CreateSequence(")
                .Append(GenerateLiteral(sequence.Name))
                .Append(", ")
                .Append(GenerateLiteral(sequence.DataType))
                .Append(", ")
                .Append(sequence.StartWith)
                .Append(", ")
                .Append(sequence.IncrementBy)
                .Append(")");
        }

        public override void Generate(DropSequenceOperation dropSequenceOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropSequenceOperation, "dropSequenceOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropSequence(")
                .Append(GenerateLiteral(dropSequenceOperation.SequenceName))
                .Append(")");
        }

        public override void Generate(CreateTableOperation createTableOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(createTableOperation, "createTableOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            var table = createTableOperation.Table;

            stringBuilder
                .Append("CreateTable(")
                .Append(GenerateLiteral(createTableOperation.Table.Name))
                .AppendLine(",");

            using (stringBuilder.Indent())
            {
                GenerateColumns(table.Columns, stringBuilder);

                stringBuilder.Append(")");

                var primaryKey = table.PrimaryKey;
                if (primaryKey != null)
                {
                    stringBuilder
                        .AppendLine()
                        .Append(".PrimaryKey(")
                        .Append(GenerateLiteral(primaryKey.Name))
                        .Append(",");

                    if (primaryKey.Columns.Count == 1)
                    {
                        stringBuilder.Append(" ");

                        GenerateColumnReference(primaryKey.Columns[0], stringBuilder);
                    }
                    else
                    {
                        using (stringBuilder.AppendLine().Indent())
                        {
                            GenerateColumnReferences(primaryKey.Columns, stringBuilder);
                        }
                    }

                    stringBuilder.Append(")");
                }
            }
        }

        public override void Generate(DropTableOperation dropTableOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropTableOperation, "dropTableOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropTable(")
                .Append(GenerateLiteral(dropTableOperation.TableName))
                .Append(")");
        }

        public override void Generate(RenameTableOperation renameTableOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameTableOperation, "renameTableOperation");

            stringBuilder
                .Append("RenameTable(")
                .Append(GenerateLiteral(renameTableOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(renameTableOperation.NewTableName))
                .Append(")");
        }

        public override void Generate(MoveTableOperation moveTableOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(moveTableOperation, "moveTableOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("MoveTable(")
                .Append(GenerateLiteral(moveTableOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(moveTableOperation.NewSchema))
                .Append(")");
        }

        public override void Generate(AddColumnOperation addColumnOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(addColumnOperation, "addColumnOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("AddColumn(")
                .Append(GenerateLiteral(addColumnOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(addColumnOperation.Column.Name))
                .Append(", c => ");

            GenerateColumn(addColumnOperation.Column, stringBuilder, emitName: false);

            stringBuilder.Append(")");
        }

        public override void Generate(DropColumnOperation dropColumnOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropColumnOperation, "dropColumnOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropColumn(")
                .Append(GenerateLiteral(dropColumnOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(dropColumnOperation.ColumnName))
                .Append(")");
        }

        public override void Generate(RenameColumnOperation renameColumnOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameColumnOperation, "renameColumnOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("RenameColumn(")
                .Append(GenerateLiteral(renameColumnOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(renameColumnOperation.ColumnName))
                .Append(", ")
                .Append(GenerateLiteral(renameColumnOperation.NewColumnName))
                .Append(")");
        }

        public override void Generate(AlterColumnOperation alterColumnOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(alterColumnOperation, "alterColumnOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("AlterColumn(")
                .Append(GenerateLiteral(alterColumnOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(alterColumnOperation.NewColumn.Name))
                .Append(", c => ");

            GenerateColumn(alterColumnOperation.NewColumn, stringBuilder, emitName: false);

            stringBuilder.Append(")");
        }

        public override void Generate(AddDefaultConstraintOperation addDefaultConstraintOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(addDefaultConstraintOperation, "addDefaultConstraintOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("AddDefaultConstraint(")
                .Append(GenerateLiteral(addDefaultConstraintOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(addDefaultConstraintOperation.ColumnName))
                .Append(", DefaultConstraint.");

            if (addDefaultConstraintOperation.DefaultValue != null)
            {
                stringBuilder
                    .Append("Value(")
                    .Append(GenerateLiteral((dynamic)addDefaultConstraintOperation.DefaultValue));
            }
            else
            {
                stringBuilder
                    .Append("Sql(")
                    .Append(GenerateLiteral(addDefaultConstraintOperation.DefaultSql));
            }

            stringBuilder.Append("))");
        }

        public override void Generate(DropDefaultConstraintOperation dropDefaultConstraintOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropDefaultConstraintOperation, "dropDefaultConstraintOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropDefaultConstraint(")
                .Append(GenerateLiteral(dropDefaultConstraintOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(dropDefaultConstraintOperation.ColumnName))
                .Append(")");
        }

        public override void Generate(AddPrimaryKeyOperation addPrimaryKeyOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(addPrimaryKeyOperation, "addPrimaryKeyOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("AddPrimaryKey(")
                .Append(GenerateLiteral(addPrimaryKeyOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(addPrimaryKeyOperation.PrimaryKeyName))
                .Append(", new[] { ")
                .Append(addPrimaryKeyOperation.ColumnNames.Select(GenerateLiteral).Join())
                .Append(" }, isClustered: ")
                .Append(GenerateLiteral(addPrimaryKeyOperation.IsClustered))
                .Append(")");
        }

        public override void Generate(DropPrimaryKeyOperation dropPrimaryKeyOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropPrimaryKeyOperation, "dropPrimaryKeyOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropPrimaryKey(")
                .Append(GenerateLiteral(dropPrimaryKeyOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(dropPrimaryKeyOperation.PrimaryKeyName))
                .Append(")");
        }

        public override void Generate(AddForeignKeyOperation addForeignKeyOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(addForeignKeyOperation, "addForeignKeyOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("AddForeignKey(")
                .Append(GenerateLiteral(addForeignKeyOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(addForeignKeyOperation.ForeignKeyName))
                .Append(", new[] { ")
                .Append(addForeignKeyOperation.ColumnNames.Select(GenerateLiteral).Join())
                .Append(" }, ")
                .Append(GenerateLiteral(addForeignKeyOperation.ReferencedTableName))
                .Append(", new[] { ")
                .Append(addForeignKeyOperation.ReferencedColumnNames.Select(GenerateLiteral).Join())
                .Append(" }, cascadeDelete: ")
                .Append(GenerateLiteral(addForeignKeyOperation.CascadeDelete))
                .Append(")");
        }

        public override void Generate(DropForeignKeyOperation dropForeignKeyOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropForeignKeyOperation, "dropForeignKeyOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropForeignKey(")
                .Append(GenerateLiteral(dropForeignKeyOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(dropForeignKeyOperation.ForeignKeyName))
                .Append(")");
        }

        public override void Generate(CreateIndexOperation createIndexOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(createIndexOperation, "createIndexOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("CreateIndex(")
                .Append(GenerateLiteral(createIndexOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(createIndexOperation.IndexName))
                .Append(", new[] { ")
                .Append(createIndexOperation.ColumnNames.Select(GenerateLiteral).Join())
                .Append(" }, isUnique: ")
                .Append(GenerateLiteral(createIndexOperation.IsUnique))
                .Append(", isClustered: ")
                .Append(GenerateLiteral(createIndexOperation.IsClustered))
                .Append(")");
        }

        public override void Generate(DropIndexOperation dropIndexOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(dropIndexOperation, "dropIndexOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("DropIndex(")
                .Append(GenerateLiteral(dropIndexOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(dropIndexOperation.IndexName))
                .Append(")");
        }

        public override void Generate(RenameIndexOperation renameIndexOperation, IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(renameIndexOperation, "renameIndexOperation");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder
                .Append("RenameIndex(")
                .Append(GenerateLiteral(renameIndexOperation.TableName))
                .Append(", ")
                .Append(GenerateLiteral(renameIndexOperation.IndexName))
                .Append(", ")
                .Append(GenerateLiteral(renameIndexOperation.NewIndexName))
                .Append(")");
        }

        public virtual string EscapeString([NotNull] string str)
        {
            Check.NotEmpty(str, "str");

            return str.Replace("\"", "\\\"");
        }

        public virtual string GenerateLiteral([NotNull] byte[] value)
        {
            Check.NotNull(value, "value");

            return "new byte[] {" + string.Join(", ", value) + "}";
        }

        public virtual string GenerateLiteral(bool value)
        {
            return value ? "true" : "false";
        }

        public virtual string GenerateLiteral(long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        public virtual string GenerateLiteral(decimal value)
        {
            return value.ToString(CultureInfo.InvariantCulture) + "m";
        }

        public virtual string GenerateLiteral(float value)
        {
            return value.ToString(CultureInfo.InvariantCulture) + "f";
        }

        public virtual string GenerateLiteral(TimeSpan value)
        {
            return "new TimeSpan(" + value.Ticks + ")";
        }

        public virtual string GenerateLiteral(DateTime value)
        {
            return "new DateTime(" + value.Ticks + ", DateTimeKind."
                   + Enum.GetName(typeof(DateTimeKind), value.Kind) + ")";
        }

        public virtual string GenerateLiteral(DateTimeOffset value)
        {
            return "new DateTimeOffset(" + value.Ticks + ", "
                   + GenerateLiteral(value.Offset) + ")";
        }

        public virtual string GenerateLiteral(Guid value)
        {
            return "new Guid(" + GenerateLiteral(value.ToString()) + ")";
        }

        public virtual string GenerateLiteral([NotNull] string value)
        {
            Check.NotNull(value, "value");

            return "\"" + EscapeString(value) + "\"";
        }

        public virtual string Generate([NotNull] object value)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}", value);
        }

        protected virtual void GenerateColumns([NotNull] IReadOnlyList<Column> columns, [NotNull] IndentedStringBuilder stringBuilder)
        {
            Check.NotNull(columns, "columns");
            Check.NotNull(stringBuilder, "stringBuilder");

            stringBuilder.AppendLine("c => new");

            using (stringBuilder.Indent())
            {
                stringBuilder.AppendLine("{");

                using (stringBuilder.Indent())
                {
                    for (var i = 0; i < columns.Count; i++)
                    {
                        var column = columns[i];
                        var columnIdentifier = GenerateColumnIdentifier(column.Name);
                        var emitName = !string.Equals(columnIdentifier, column.Name, StringComparison.Ordinal);

                        if (i > 0)
                        {
                            stringBuilder.AppendLine(",");
                        }

                        stringBuilder
                            .Append(columnIdentifier)
                            .Append(" = ");

                        GenerateColumn(column, stringBuilder, emitName);
                    }
                }

                stringBuilder
                    .AppendLine()
                    .Append("}");
            }
        }

        protected virtual void GenerateColumn([NotNull] Column column, [NotNull] IndentedStringBuilder stringBuilder, bool emitName)
        {
            Check.NotNull(column, "column");

            stringBuilder
                .Append("c.")
                .Append(TranslateColumnType(column.ClrType))
                .Append("(");

            var args = new List<string>();

            if (emitName)
            {
                args.Add("name: " + GenerateLiteral(column.Name));
            }

            if (!string.IsNullOrWhiteSpace(column.DataType))
            {
                args.Add("dataType: " + GenerateLiteral(column.DataType));
            }

            if (!column.IsNullable)
            {
                args.Add("nullable: " + GenerateLiteral(false));
            }

            if (column.MaxLength.HasValue)
            {
                args.Add("maxLength: " + GenerateLiteral(column.MaxLength.Value));
            }

            if (column.Precision.HasValue)
            {
                args.Add("precision: " + GenerateLiteral(column.Precision.Value));
            }

            if (column.Scale.HasValue)
            {
                args.Add("scale: " + GenerateLiteral(column.Scale.Value));
            }

            if (column.IsFixedLength.HasValue)
            {
                args.Add("fixedLength: " + GenerateLiteral(column.IsFixedLength.Value));
            }

            if (column.IsUnicode.HasValue)
            {
                args.Add("unicode: " + GenerateLiteral(column.IsUnicode.Value));
            }

            if (column.ValueGenerationStrategy == StoreValueGenerationStrategy.Identity)
            {
                args.Add("identity: " + GenerateLiteral(true));
            }

            if (column.DefaultValue != null)
            {
                args.Add("defaultValue: " + GenerateLiteral((dynamic)column.DefaultValue));
            }

            if (!string.IsNullOrWhiteSpace(column.DefaultSql))
            {
                args.Add("defaultSql: " + GenerateLiteral(column.DefaultSql));
            }

            if (column.IsTimestamp)
            {
                args.Add("timestamp: " + GenerateLiteral(true));
            }

            stringBuilder
                .Append(string.Join(", ", args))
                .Append(")");
        }

        protected virtual void GenerateColumnReference(
            [NotNull] Column column, [NotNull] IndentedStringBuilder stringBuilder)
        {
            stringBuilder
                .Append("t => t.")
                .Append(GenerateColumnIdentifier(column.Name));
        }

        protected virtual void GenerateColumnReferences(
            [NotNull] IReadOnlyList<Column> columns, [NotNull] IndentedStringBuilder stringBuilder)
        {
            stringBuilder.AppendLine("t => new");

            using (stringBuilder.Indent())
            {
                stringBuilder.AppendLine("{");

                using (stringBuilder.Indent())
                {
                    for (var i = 0; i < columns.Count; i++)
                    {
                        var columnIdentifier = GenerateColumnIdentifier(columns[i].Name);

                        if (i > 0)
                        {
                            stringBuilder.AppendLine(",");
                        }

                        stringBuilder
                            .Append(columnIdentifier)
                            .Append(" => t.")
                            .Append(columnIdentifier);
                    }
                }

                stringBuilder
                    .AppendLine()
                    .Append("}");
            }
        }

        protected virtual string GenerateColumnIdentifier([NotNull] string columnName)
        {
            Check.NotEmpty(columnName, "columnName");

            var invalidCharsRegex
                = new Regex(@"[^\p{Ll}\p{Lu}\p{Lt}\p{Lo}\p{Nd}\p{Nl}\p{Mn}\p{Mc}\p{Cf}\p{Pc}\p{Lm}]");

            var identifier = invalidCharsRegex.Replace(columnName, string.Empty);
            var firstChar = identifier[0];

            if ((!char.IsLetter(firstChar) && firstChar != '_'))
            {
                identifier = "_" + identifier;
            }

            // TODO: Validate identifiers (not keywords, unique).

            return identifier;
        }

        protected virtual string TranslateColumnType([NotNull] Type clrType)
        {
            if (clrType == typeof(short))
            {
                return "Short";
            }

            if (clrType == typeof(int))
            {
                return "Int";
            }

            if (clrType == typeof(long))
            {
                return "Long";
            }

            if (clrType == typeof(byte[]))
            {
                return "Binary";
            }

            if (clrType == typeof(TimeSpan))
            {
                return "Time";
            }

            return clrType.Name;
        }
    }
}
