// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Query;
using Microsoft.Framework.Logging;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionTreeVisitors;
using Remotion.Linq.Parsing;

namespace Microsoft.Data.Entity.Relational
{
    public partial class RelationalDataStore
    {
        private class QueryModelVisitor : EntityQueryModelVisitor
        {
            private readonly Dictionary<IQuerySource, EntityQuery> _queriesBySource
                = new Dictionary<IQuerySource, EntityQuery>();

            private class EntityQuery
            {
                private readonly SqlSelect _sqlSelect = new SqlSelect();
                private readonly Dictionary<IProperty, int> _propertyIndexes = new Dictionary<IProperty, int>();

                public EntityQuery(string tableName)
                {
                    _sqlSelect.Table = tableName;
                }

                public void AddToProjection(IProperty property)
                {
                    if (!_propertyIndexes.ContainsKey(property))
                    {
                        _propertyIndexes
                            .Add(property, _sqlSelect.AddToSelectList(property.StorageName));
                    }
                }

                public int GetProjectionIndex(IProperty property)
                {
                    return _propertyIndexes[property];
                }

                public override string ToString()
                {
                    return _sqlSelect.ToString();
                }
            }

            public QueryModelVisitor(IModel model)
                : base(model)
            {
            }

            protected override ExpressionTreeVisitor CreateQueryingExpressionTreeVisitor(
                IQuerySource querySource, bool isolateSubqueries = false)
            {
                return new RelationalQueryingExpressionTreeVisitor(this, querySource, isolateSubqueries);
            }

            protected override ExpressionTreeVisitor CreateProjectionExpressionTreeVisitor(
                IQuerySource querySource, bool isolateSubqueries = false)
            {
                return new RelationalProjectionSubQueryExpressionTreeVisitor(
                    this, querySource, isolateSubqueries);
            }

            protected override Expression ReplaceClauseReferences(
                Expression expression, QuerySourceMapping querySourceMapping, bool throwOnUnmappedReferences)
            {
                return new MemberAccessToValueReaderReferenceReplacingExpressionTreeVisitor(
                    querySourceMapping, throwOnUnmappedReferences, this)
                    .VisitExpression(expression);
            }

            private class MemberAccessToValueReaderReferenceReplacingExpressionTreeVisitor : ReferenceReplacingExpressionTreeVisitor
            {
                private readonly QueryModelVisitor _queryModelVisitor;

                public MemberAccessToValueReaderReferenceReplacingExpressionTreeVisitor(
                    QuerySourceMapping querySourceMapping,
                    bool throwOnUnmappedReferences,
                    QueryModelVisitor queryModelVisitor)
                    : base(querySourceMapping, throwOnUnmappedReferences)
                {
                    _queryModelVisitor = queryModelVisitor;
                }

                private static readonly MethodInfo _readValueMethodInfo
                    = typeof(IValueReader).GetTypeInfo().GetDeclaredMethod("ReadValue");

                public override Expression VisitExpression(Expression expression)
                {
                    return base.VisitExpression(expression);
                }

                protected override Expression VisitMemberExpression(MemberExpression expression)
                {
                    var newExpression = VisitExpression(expression.Expression);

                    if (newExpression != expression.Expression)
                    {
                        if (newExpression.Type == typeof(IValueReader))
                        {
                            var querySourceReferenceExpression
                                = (QuerySourceReferenceExpression)expression.Expression;

                            var entityType
                                = _queryModelVisitor._model
                                    .GetEntityType(querySourceReferenceExpression.ReferencedQuerySource.ItemType);

                            var property = entityType.GetProperty(expression.Member.Name);

                            EntityQuery entityQuery;
                            if (_queryModelVisitor._queriesBySource
                                .TryGetValue(querySourceReferenceExpression.ReferencedQuerySource, out entityQuery))
                            {
                                return Expression.Call(
                                    newExpression,
                                    _readValueMethodInfo.MakeGenericMethod(expression.Type),
                                    new Expression[] { Expression.Constant(entityQuery.GetProjectionIndex(property)) });
                            }
                        }

                        return Expression.MakeMemberAccess(newExpression, expression.Member);
                    }

                    return expression;
                }
            }

            private class RelationalQueryingExpressionTreeVisitor : QueryingExpressionTreeVisitor
            {
                private readonly QueryModelVisitor _queryModelVisitor;
                private readonly IQuerySource _querySource;

                public RelationalQueryingExpressionTreeVisitor(
                    QueryModelVisitor queryModelVisitor, IQuerySource querySource, bool isolateSubqueries)
                    : base(queryModelVisitor._model, isolateSubqueries)
                {
                    _queryModelVisitor = queryModelVisitor;
                    _querySource = querySource;
                }

                protected override Expression VisitSubQueryExpression(SubQueryExpression expression)
                {
                    var queryModelVisitor = new QueryModelVisitor(_model) { _isRootVisitor = _isolateSubqueries };

                    queryModelVisitor.VisitQueryModel(expression.QueryModel);

                    return queryModelVisitor._expression;
                }

                protected override Expression VisitMemberExpression(MemberExpression expression)
                {
                    var querySourceReferenceExpression
                        = expression.Expression as QuerySourceReferenceExpression;

                    if (querySourceReferenceExpression != null)
                    {
                        var querySource = querySourceReferenceExpression.ReferencedQuerySource;

                        if (!_queryModelVisitor.QuerySourceRequiresMaterialization(querySource))
                        {
                            var entityType = _model.TryGetEntityType(querySource.ItemType);

                            if (entityType != null)
                            {
                                var property = entityType.TryGetProperty(expression.Member.Name);

                                if (property != null)
                                {
                                    EntityQuery entityQuery;
                                    if (_queryModelVisitor._queriesBySource.TryGetValue(querySource, out entityQuery))
                                    {
                                        entityQuery.AddToProjection(property);
                                    }
                                }
                            }
                        }
                    }

                    return base.VisitMemberExpression(expression);
                }

                protected override Expression VisitEntityQueryable(Type elementType)
                {
                    var queryMethodInfo = _queryValuesMethodInfo;
                    var entityType = _model.GetEntityType(elementType);

                    var entityQuery = new EntityQuery(entityType.StorageName);

                    _queryModelVisitor._queriesBySource.Add(_querySource, entityQuery);

                    if (_queryModelVisitor.QuerySourceRequiresMaterialization(_querySource))
                    {
                        foreach (var property in entityType.Properties)
                        {
                            entityQuery.AddToProjection(property);
                        }

                        queryMethodInfo = _queryEntitiesMethodInfo.MakeGenericMethod(elementType);
                    }

                    return Expression.Call(
                        queryMethodInfo, _queryContextParameter, Expression.Constant(entityQuery));
                }

                private static readonly MethodInfo _queryValuesMethodInfo
                    = typeof(RelationalQueryingExpressionTreeVisitor).GetTypeInfo()
                        .GetDeclaredMethod("QueryValues");

                [UsedImplicitly]
                private static IEnumerable<IValueReader> QueryValues(QueryContext queryContext, EntityQuery entityQuery)
                {
                    var relationalQueryContext = (RelationalQueryContext)queryContext;

                    return new Enumerable<IValueReader>(
                        relationalQueryContext.Connection,
                        entityQuery.ToString(),
                        r => relationalQueryContext.ValueReaderFactory.Create(r),
                        queryContext.Logger);
                }

                private static readonly MethodInfo _queryEntitiesMethodInfo
                    = typeof(RelationalQueryingExpressionTreeVisitor).GetTypeInfo()
                        .GetDeclaredMethod("QueryEntities");

                [UsedImplicitly]
                private static IEnumerable<TEntity> QueryEntities<TEntity>(QueryContext queryContext, EntityQuery entityQuery)
                {
                    var relationalQueryContext = ((RelationalQueryContext)queryContext);

                    return new Enumerable<TEntity>(
                        relationalQueryContext.Connection,
                        entityQuery.ToString(),
                        r => (TEntity)queryContext.StateManager
                            .GetOrMaterializeEntry(
                                queryContext.Model.GetEntityType(typeof(TEntity)),
                                relationalQueryContext.ValueReaderFactory.Create(r)).Entity,
                        queryContext.Logger);
                }
            }

            private class RelationalProjectionSubQueryExpressionTreeVisitor : RelationalQueryingExpressionTreeVisitor
            {
                public RelationalProjectionSubQueryExpressionTreeVisitor(
                    QueryModelVisitor queryModelVisitor, IQuerySource querySource, bool isolateSubqueries)
                    : base(queryModelVisitor, querySource, isolateSubqueries)
                {
                }

                protected override Expression VisitSubQueryExpression(SubQueryExpression expression)
                {
                    return VisitProjectionSubQuery(expression, new QueryModelVisitor(_model));
                }
            }

            private sealed class Enumerable<T> : IEnumerable<T>
            {
                private readonly RelationalConnection _connection;
                private readonly string _sql;
                private readonly Func<DbDataReader, T> _shaper;
                private readonly ILogger _logger;

                public Enumerable(
                    RelationalConnection connection,
                    string sql,
                    Func<DbDataReader, T> shaper,
                    ILogger logger)
                {
                    _connection = connection;
                    _sql = sql;
                    _shaper = shaper;
                    _logger = logger;
                }

                public IEnumerator<T> GetEnumerator()
                {
                    return new Enumerator(this);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return GetEnumerator();
                }

                private sealed class Enumerator : IEnumerator<T>
                {
                    private readonly Enumerable<T> _enumerable;

                    private DbCommand _command;
                    private DbDataReader _reader;

                    public Enumerator(Enumerable<T> enumerable)
                    {
                        _enumerable = enumerable;
                    }

                    public bool MoveNext()
                    {
                        if (_reader == null)
                        {
                            _enumerable._connection.Open();

                            _command = _enumerable._connection.DbConnection.CreateCommand();
                            _command.CommandText = _enumerable._sql;

                            _enumerable._logger.WriteSql(_enumerable._sql);

                            _reader = _command.ExecuteReader();
                        }

                        return _reader.Read();
                    }

                    public T Current
                    {
                        get
                        {
                            if (_reader == null)
                            {
                                return default(T);
                            }

                            return _enumerable._shaper(_reader);
                        }
                    }

                    object IEnumerator.Current
                    {
                        get { return Current; }
                    }

                    public void Dispose()
                    {
                        if (_reader != null)
                        {
                            _reader.Dispose();
                        }

                        if (_command != null)
                        {
                            _command.Dispose();
                        }

                        if (_enumerable._connection != null)
                        {
                            _enumerable._connection.Close();
                        }
                    }

                    public void Reset()
                    {
                        throw new NotImplementedException();
                    }
                }
            }
        }
    }
}
