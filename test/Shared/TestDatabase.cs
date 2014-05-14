// Copyright (c) Microsoft Open Technologies, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Data.FunctionalTests
{
    public abstract class TestDatabase
    {
        protected sealed class AsyncLock
        {
            private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
            private readonly Task<IDisposable> _releaser;

            public AsyncLock()
            {
                _releaser = Task.FromResult<IDisposable>(new Releaser(this));
            }

            public Task<IDisposable> LockAsync()
            {
                var waitTask = _semaphore.WaitAsync();

                return waitTask.IsCompleted
                    ? _releaser
                    : waitTask.ContinueWith(
                        (_, state) => (IDisposable)state,
                        _releaser.Result,
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
            }

            private sealed class Releaser : IDisposable
            {
                private readonly AsyncLock _asyncLock;

                public Releaser(AsyncLock asyncLock)
                {
                    _asyncLock = asyncLock;
                }

                public void Dispose()
                {
                    _asyncLock._semaphore.Release();
                }
            }
        }
    }
}
