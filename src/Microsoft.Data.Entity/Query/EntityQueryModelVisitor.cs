// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JetBrains.Annotations;
using Microsoft.Data.Entity.Metadata;
using Microsoft.Data.Entity.Utilities;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionTreeVisitors;
using Remotion.Linq.Clauses.StreamedData;
using Remotion.Linq.Parsing;

namespace Microsoft.Data.Entity.Query
{
    public abstract class EntityQueryModelVisitor : QueryModelVisitorBase
    {
        protected static readonly ParameterExpression _queryContextParameter
            = Expression.Parameter(typeof(QueryContext));

        private static readonly ParameterExpression _querySourceScopeParameter
            = Expression.Parameter(typeof(QuerySourceScope));

        protected readonly QuerySourceMapping _querySourceMapping = new QuerySourceMapping();

        protected readonly IModel _model;
        private readonly ILinqOperatorProvider _linqOperatorProvider;

        protected bool _isRootVisitor;

        protected Expression _expression;
        protected StreamedSequenceInfo _streamedSequenceInfo;

        private ISet<IQuerySource> _querySourcesRequiringMaterialization;

        protected EntityQueryModelVisitor([NotNull] IModel model)
            : this(Check.NotNull(model, "model"), new LinqOperatorProvider())
        {
        }

        protected EntityQueryModelVisitor(
            [NotNull] IModel model,
            [NotNull] ILinqOperatorProvider linqOperatorProvider)
        {
            Check.NotNull(model, "model");
            Check.NotNull(linqOperatorProvider, "linqOperatorProvider");

            _model = model;
            _linqOperatorProvider = linqOperatorProvider;
        }

        protected abstract ExpressionTreeVisitor CreateQueryingExpressionTreeVisitor(
            IQuerySource querySource, bool isolateSubqueries = false);

        protected abstract ExpressionTreeVisitor CreateProjectionExpressionTreeVisitor(
            IQuerySource querySource, bool isolateSubqueries = false);

        public Func<QueryContext, IEnumerable<TResult>> CreateQueryExecutor<TResult>([NotNull] QueryModel queryModel)
        {
            Check.NotNull(queryModel, "queryModel");

            _isRootVisitor = true;

            VisitQueryModel(queryModel);

            if (_streamedSequenceInfo == null)
            {
                _expression
                    = Expression.Call(
                        _linqOperatorProvider.ToSequence
                            .MakeGenericMethod(typeof(TResult)),
                        _expression);
            }

            return Expression
                .Lambda<Func<QueryContext, IEnumerable<TResult>>>(_expression, _queryContextParameter)
                .Compile();
        }

        protected bool QuerySourceRequiresMaterialization([NotNull] IQuerySource querySource)
        {
            Check.NotNull(querySource, "querySource");

            return _querySourcesRequiringMaterialization.Contains(querySource);
        }

        public override void VisitQueryModel([NotNull] QueryModel queryModel)
        {
            Check.NotNull(queryModel, "queryModel");

            var requiresEntityMaterializationExpressionTreeVisitor
                = new RequiresEntityMaterializationExpressionTreeVisitor(_model);

            queryModel.TransformExpressions(requiresEntityMaterializationExpressionTreeVisitor.VisitExpression);

            _querySourcesRequiringMaterialization
                = requiresEntityMaterializationExpressionTreeVisitor.QuerySourcesRequiringMaterialization;

            foreach (var groupJoinClause in queryModel.BodyClauses.OfType<GroupJoinClause>())
            {
                _querySourcesRequiringMaterialization.Add(groupJoinClause.JoinClause);
            }

            base.VisitQueryModel(queryModel);
        }

        private class RequiresEntityMaterializationExpressionTreeVisitor : ExpressionTreeVisitor
        {
            private readonly Dictionary<IQuerySource, int> _querySources = new Dictionary<IQuerySource, int>();
            private readonly IModel _model;

            public RequiresEntityMaterializationExpressionTreeVisitor(IModel model)
            {
                _model = model;
            }

            public ISet<IQuerySource> QuerySourcesRequiringMaterialization
            {
                get { return new HashSet<IQuerySource>(_querySources.Where(kv => kv.Value > 0).Select(kv => kv.Key)); }
            }

            protected override Expression VisitQuerySourceReferenceExpression(QuerySourceReferenceExpression expression)
            {
                if (!_querySources.ContainsKey(expression.ReferencedQuerySource))
                {
                    _querySources.Add(expression.ReferencedQuerySource, 0);
                }

                _querySources[expression.ReferencedQuerySource]++;

                return base.VisitQuerySourceReferenceExpression(expression);
            }

            protected override Expression VisitMemberExpression(MemberExpression expression)
            {
                var newExpression = base.VisitMemberExpression(expression);

                var querySourceReferenceExpression
                    = expression.Expression as QuerySourceReferenceExpression;

                if (querySourceReferenceExpression != null)
                {
                    var entityType
                        = _model.TryGetEntityType(querySourceReferenceExpression.ReferencedQuerySource.ItemType);

                    if (entityType != null
                        && entityType.TryGetProperty(expression.Member.Name) != null)
                    {
                        _querySources[querySourceReferenceExpression.ReferencedQuerySource]--;
                    }
                }

                return newExpression;
            }
        }

        public override void VisitMainFromClause(
            [NotNull] MainFromClause fromClause, [NotNull] QueryModel queryModel)
        {
            Check.NotNull(fromClause, "fromClause");
            Check.NotNull(queryModel, "queryModel");

            _expression
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(fromClause)
                        .VisitExpression(fromClause.FromExpression));

            var elementType = _expression.Type.GetSequenceType();

            var itemParameter
                = Expression.Parameter(elementType);

            var parentScopeExpression
                = _isRootVisitor
                    ? (Expression)Expression.Default(typeof(QuerySourceScope))
                    : _querySourceScopeParameter;

            var scopeCreatorExpression
                = QuerySourceScope
                    .Create(fromClause, itemParameter, parentScopeExpression);

            _expression
                = Expression.Call(
                    _linqOperatorProvider.SelectMany
                        .MakeGenericMethod(typeof(QuerySourceScope), typeof(QuerySourceScope)),
                    Expression.Call(
                        _linqOperatorProvider.ToSequence
                            .MakeGenericMethod(typeof(QuerySourceScope)),
                        parentScopeExpression),
                    Expression.Lambda(
                        Expression.Call(
                            _linqOperatorProvider.Select
                                .MakeGenericMethod(elementType, typeof(QuerySourceScope)),
                            _expression,
                            Expression.Lambda(
                                scopeCreatorExpression,
                                new[] { itemParameter })),
                        new[] { _querySourceScopeParameter }));

            _querySourceMapping.AddMapping(
                fromClause,
                QuerySourceScope.GetResult(_querySourceScopeParameter, fromClause, elementType));
        }

        public override void VisitAdditionalFromClause(
            [NotNull] AdditionalFromClause fromClause, [NotNull] QueryModel queryModel, int index)
        {
            Check.NotNull(fromClause, "fromClause");
            Check.NotNull(queryModel, "queryModel");

            var innerExpression
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(fromClause)
                        .VisitExpression(fromClause.FromExpression));

            var innerElementType = innerExpression.Type.GetSequenceType();

            var itemParameter
                = Expression.Parameter(innerElementType);

            var scopeCreatorExpression
                = QuerySourceScope
                    .Create(fromClause, itemParameter, _querySourceScopeParameter);

            _expression
                = Expression.Call(
                    _linqOperatorProvider.SelectMany
                        .MakeGenericMethod(typeof(QuerySourceScope), typeof(QuerySourceScope)),
                    _expression,
                    Expression.Lambda(
                        Expression.Call(
                            _linqOperatorProvider.Select
                                .MakeGenericMethod(innerElementType, typeof(QuerySourceScope)),
                            innerExpression,
                            Expression.Lambda(
                                scopeCreatorExpression,
                                new[] { itemParameter })),
                        new[] { _querySourceScopeParameter }));

            _querySourceMapping.AddMapping(
                fromClause,
                QuerySourceScope.GetResult(_querySourceScopeParameter, fromClause, innerElementType));
        }

        public override void VisitJoinClause(
            [NotNull] JoinClause joinClause, [NotNull] QueryModel queryModel, int index)
        {
            Check.NotNull(joinClause, "joinClause");
            Check.NotNull(queryModel, "queryModel");

            var innerSequenceExpression
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(joinClause, isolateSubqueries: true)
                        .VisitExpression(joinClause.InnerSequence));

            var innerElementType
                = innerSequenceExpression.Type.GetSequenceType();

            var itemParameter
                = Expression.Parameter(innerElementType);

            _querySourceMapping.AddMapping(joinClause, itemParameter);

            var outerKeySelector
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(joinClause)
                        .VisitExpression(joinClause.OuterKeySelector));

            var innerKeySelector
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(joinClause)
                        .VisitExpression(joinClause.InnerKeySelector));

            var scopeCreatorExpression
                = QuerySourceScope
                    .Create(joinClause, itemParameter, _querySourceScopeParameter);

            _expression
                = Expression.Call(
                    _linqOperatorProvider.Join.MakeGenericMethod(
                        typeof(QuerySourceScope),
                        innerElementType,
                        outerKeySelector.Type,
                        typeof(QuerySourceScope)),
                    _expression,
                    innerSequenceExpression,
                    Expression.Lambda(outerKeySelector, _querySourceScopeParameter),
                    Expression.Lambda(innerKeySelector, itemParameter),
                    Expression.Lambda(
                        scopeCreatorExpression,
                        new[] { _querySourceScopeParameter, itemParameter }));

            _querySourceMapping.ReplaceMapping(
                joinClause,
                QuerySourceScope.GetResult(_querySourceScopeParameter, joinClause, innerElementType));
        }

        public override void VisitGroupJoinClause(
            [NotNull] GroupJoinClause groupJoinClause, [NotNull] QueryModel queryModel, int index)
        {
            Check.NotNull(groupJoinClause, "groupJoinClause");
            Check.NotNull(queryModel, "queryModel");

            var innerSequenceExpression
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(groupJoinClause.JoinClause, isolateSubqueries: true)
                        .VisitExpression(groupJoinClause.JoinClause.InnerSequence));

            var innerElementType
                = innerSequenceExpression.Type.GetSequenceType();

            var itemParameter
                = Expression.Parameter(innerElementType);

            _querySourceMapping.AddMapping(groupJoinClause.JoinClause, itemParameter);

            var outerKeySelector
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(groupJoinClause)
                        .VisitExpression(groupJoinClause.JoinClause.OuterKeySelector));

            var innerKeySelector
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(groupJoinClause)
                        .VisitExpression(groupJoinClause.JoinClause.InnerKeySelector));

            var itemsParameter
                = Expression.Parameter(innerSequenceExpression.Type);

            var scopeCreatorExpression
                = QuerySourceScope
                    .Create(groupJoinClause, itemsParameter, _querySourceScopeParameter);

            _expression
                = Expression.Call(
                    _linqOperatorProvider.GroupJoin.MakeGenericMethod(
                        typeof(QuerySourceScope),
                        innerElementType,
                        outerKeySelector.Type,
                        typeof(QuerySourceScope)),
                    _expression,
                    innerSequenceExpression,
                    Expression.Lambda(outerKeySelector, _querySourceScopeParameter),
                    Expression.Lambda(innerKeySelector, itemParameter),
                    Expression.Lambda(
                        scopeCreatorExpression,
                        new[] { _querySourceScopeParameter, itemsParameter }));

            _querySourceMapping.AddMapping(
                groupJoinClause,
                QuerySourceScope.GetResult(_querySourceScopeParameter, groupJoinClause, innerSequenceExpression.Type));
        }

        public override void VisitWhereClause(
            [NotNull] WhereClause whereClause, [NotNull] QueryModel queryModel, int index)
        {
            Check.NotNull(whereClause, "whereClause");
            Check.NotNull(queryModel, "queryModel");

            var predicate
                = ReplaceClauseReferences(
                    CreateQueryingExpressionTreeVisitor(queryModel.MainFromClause)
                        .VisitExpression(whereClause.Predicate));

            _expression
                = Expression.Call(
                    _linqOperatorProvider.Where.MakeGenericMethod(typeof(QuerySourceScope)),
                    _expression,
                    Expression.Lambda(predicate, _querySourceScopeParameter));
        }

        public override void VisitSelectClause(
            [NotNull] SelectClause selectClause, [NotNull] QueryModel queryModel)
        {
            Check.NotNull(selectClause, "selectClause");
            Check.NotNull(queryModel, "queryModel");

            if (_streamedSequenceInfo != null)
            {
                return;
            }

            var selector
                = ReplaceClauseReferences(
                    CreateProjectionExpressionTreeVisitor(queryModel.MainFromClause)
                        .VisitExpression(selectClause.Selector));

            _expression
                = Expression.Call(
                    _linqOperatorProvider.Select
                        .MakeGenericMethod(typeof(QuerySourceScope), selector.Type),
                    _expression,
                    Expression.Lambda(selector, _querySourceScopeParameter));

            _streamedSequenceInfo
                = (StreamedSequenceInfo)selectClause.GetOutputDataInfo()
                    .AdjustDataType(typeof(IEnumerable<>));
        }

        public override void VisitOrdering(
            [NotNull] Ordering ordering, [NotNull] QueryModel queryModel, [NotNull] OrderByClause orderByClause, int index)
        {
            Check.NotNull(ordering, "ordering");
            Check.NotNull(queryModel, "queryModel");
            Check.NotNull(orderByClause, "orderByClause");

            var resultType = queryModel.GetResultType();

            if (resultType.GetTypeInfo().IsGenericType
                && resultType.GetGenericTypeDefinition() == typeof(IOrderedEnumerable<>))
            {
                VisitSelectClause(queryModel.SelectClause, queryModel);

                var parameterExpression
                    = Expression.Parameter(_streamedSequenceInfo.ResultItemType);

                _querySourceMapping
                    .ReplaceMapping(
                        queryModel.MainFromClause, parameterExpression);

                var expression
                    = ReplaceClauseReferences(
                        CreateQueryingExpressionTreeVisitor(queryModel.MainFromClause)
                            .VisitExpression(ordering.Expression));

                _expression
                    = Expression.Call(
                        (index == 0
                            ? _linqOperatorProvider.OrderBy
                            : _linqOperatorProvider.ThenBy)
                            .MakeGenericMethod(_streamedSequenceInfo.ResultItemType, expression.Type),
                        _expression,
                        Expression.Lambda(expression, parameterExpression),
                        Expression.Constant(ordering.OrderingDirection));
            }
            else
            {
                var expression
                    = ReplaceClauseReferences(
                        CreateQueryingExpressionTreeVisitor(queryModel.MainFromClause)
                            .VisitExpression(ordering.Expression));

                _expression
                    = Expression.Call(
                        (index == 0
                            ? _linqOperatorProvider.OrderBy
                            : _linqOperatorProvider.ThenBy)
                            .MakeGenericMethod(typeof(QuerySourceScope), expression.Type),
                        _expression,
                        Expression.Lambda(expression, _querySourceScopeParameter),
                        Expression.Constant(ordering.OrderingDirection));
            }
        }

        public override void VisitResultOperator(
            [NotNull] ResultOperatorBase resultOperator, [NotNull] QueryModel queryModel, int index)
        {
            Check.NotNull(resultOperator, "resultOperator");
            Check.NotNull(queryModel, "queryModel");

            // TODO: sub-queries in result op. expressions

            var streamedDataInfo
                = resultOperator.GetOutputDataInfo(_streamedSequenceInfo);

            _expression
                = Expression.Call(
                    _executeResultOperatorMethodInfo
                        .MakeGenericMethod(_streamedSequenceInfo.ResultItemType, streamedDataInfo.DataType),
                    _expression,
                    Expression.Constant(resultOperator),
                    Expression.Constant(_streamedSequenceInfo));

            _streamedSequenceInfo = streamedDataInfo as StreamedSequenceInfo;
        }

        private static readonly MethodInfo _executeResultOperatorMethodInfo
            = typeof(EntityQueryModelVisitor)
                .GetTypeInfo().GetDeclaredMethod("ExecuteResultOperator");

        [UsedImplicitly]
        private static TResult ExecuteResultOperator<TSource, TResult>(
            IEnumerable<TSource> source, ResultOperatorBase resultOperator, StreamedSequenceInfo streamedSequenceInfo)
        {
            var streamedData
                = resultOperator.ExecuteInMemory(
                    new StreamedSequence(source, streamedSequenceInfo));

            return (TResult)streamedData.Value;
        }

        private Expression ReplaceClauseReferences(Expression expression)
        {
            return ReplaceClauseReferences(expression, _querySourceMapping, _isRootVisitor);
        }

        protected virtual Expression ReplaceClauseReferences(
            [NotNull] Expression expression,
            [NotNull] QuerySourceMapping querySourceMapping,
            bool throwOnUnmappedReferences)
        {
            Check.NotNull(expression, "expression");
            Check.NotNull(querySourceMapping, "querySourceMapping");

            return ReferenceReplacingExpressionTreeVisitor
                .ReplaceClauseReferences(expression, querySourceMapping, throwOnUnmappedReferences);
        }

        protected abstract class QueryingExpressionTreeVisitor : ExpressionTreeVisitor
        {
            protected readonly IModel _model;
            protected readonly bool _isolateSubqueries;

            protected QueryingExpressionTreeVisitor(IModel model, bool isolateSubqueries)
            {
                _model = model;
                _isolateSubqueries = isolateSubqueries;
            }

            protected override Expression VisitConstantExpression(ConstantExpression expression)
            {
                if (expression.Type.GetTypeInfo().IsGenericType
                    && expression.Type.GetGenericTypeDefinition() == typeof(EntityQueryable<>))
                {
                    return VisitEntityQueryable(((IQueryable)expression.Value).ElementType);
                }

                return expression;
            }

            protected abstract Expression VisitEntityQueryable(Type elementType);

            protected Expression VisitProjectionSubQuery(
                SubQueryExpression expression, EntityQueryModelVisitor queryModelVisitor)
            {
                queryModelVisitor._isRootVisitor = _isolateSubqueries;

                queryModelVisitor.VisitQueryModel(expression.QueryModel);

                var subExpression = queryModelVisitor._expression;

                if (queryModelVisitor._streamedSequenceInfo == null)
                {
                    return subExpression;
                }

                if (typeof(IQueryable).GetTypeInfo().IsAssignableFrom(expression.Type.GetTypeInfo()))
                {
                    subExpression
                        = Expression.Call(
                            _asQueryableShim.MakeGenericMethod(
                                queryModelVisitor._streamedSequenceInfo.ResultItemType),
                            subExpression);
                }

                return Expression.Convert(subExpression, expression.Type);
            }

            private static readonly MethodInfo _asQueryableShim
                = typeof(QueryingExpressionTreeVisitor)
                    .GetTypeInfo().GetDeclaredMethod("AsQueryableShim");

            [UsedImplicitly]
            private static IOrderedQueryable<TSource> AsQueryableShim<TSource>(IEnumerable<TSource> source)
            {
                return new EnumerableQuery<TSource>(source);
            }
        }
    }
}
