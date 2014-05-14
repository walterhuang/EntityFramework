// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Query;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Parsing;

namespace Microsoft.Data.Entity.InMemory
{
    public partial class InMemoryDataStore
    {
        private class QueryModelVisitor : EntityQueryModelVisitor
        {
            public QueryModelVisitor(IModel model)
                : base(model)
            {
            }

            protected override ExpressionTreeVisitor CreateQueryingExpressionTreeVisitor(
                IQuerySource querySource, bool isolateSubqueries = false)
            {
                return new InMemoryQueryingExpressionTreeVisitor(_model, isolateSubqueries);
            }

            protected override ExpressionTreeVisitor CreateProjectionExpressionTreeVisitor(
                IQuerySource querySource, bool isolateSubqueries = false)
            {
                return new InMemoryProjectionSubQueryExpressionTreeVisitor(_model, isolateSubqueries);
            }

            private static readonly MethodInfo _entityScanMethodInfo
                = typeof(QueryModelVisitor).GetTypeInfo().GetDeclaredMethod("EntityScan");

            [UsedImplicitly]
            private static IEnumerable<TEntity> EntityScan<TEntity>(QueryContext queryContext)
            {
                var entityType = queryContext.Model.GetEntityType(typeof(TEntity));

                return ((InMemoryQueryContext)queryContext).Database.GetTable(entityType)
                    .Select(t => (TEntity)queryContext.StateManager
                        .GetOrMaterializeEntry(entityType, new ObjectArrayValueReader(t)).Entity);
            }

            private class InMemoryQueryingExpressionTreeVisitor : QueryingExpressionTreeVisitor
            {
                public InMemoryQueryingExpressionTreeVisitor(IModel model, bool isolateSubqueries)
                    : base(model, isolateSubqueries)
                {
                }

                protected override Expression VisitSubQueryExpression(SubQueryExpression expression)
                {
                    var queryModelVisitor = new QueryModelVisitor(_model) { _isRootVisitor = _isolateSubqueries };

                    queryModelVisitor.VisitQueryModel(expression.QueryModel);

                    return queryModelVisitor._expression;
                }

                protected override Expression VisitEntityQueryable(Type elementType)
                {
                    return Expression.Call(
                        _entityScanMethodInfo.MakeGenericMethod(elementType),
                        _queryContextParameter);
                }
            }

            private class InMemoryProjectionSubQueryExpressionTreeVisitor : InMemoryQueryingExpressionTreeVisitor
            {
                public InMemoryProjectionSubQueryExpressionTreeVisitor(IModel model, bool isolateSubqueries)
                    : base(model, isolateSubqueries)
                {
                }

                protected override Expression VisitSubQueryExpression(SubQueryExpression expression)
                {
                    return VisitProjectionSubQuery(expression, new QueryModelVisitor(_model));
                }
            }
        }
    }
}
