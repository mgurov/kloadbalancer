package com.github.mgurov.loadbalancer

import java.lang.Exception
import java.lang.RuntimeException
import java.time.Duration
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy(),
        val simultaneousCallSingleProviderLimit: Int? = null //can get bonkers when reaching MAXINT upon multiplication by the number of active nodes. Which is perhaps to exotic of a case to care about.
) {
    private val lock = ReentrantReadWriteLock()
    private val pickerLock = ReentrantLock()
    private val pendingCalls = AtomicInteger()
    @Volatile //TODO: do I still need it?
    private var providers: List<ProviderStatusHolder> = listOf() //TODO: thread unsafe yet

    /**
     * `register` adds a new provider to the LoadBalancer.
     *
     *  No checks are performed to ensure uniqueness of the providers specified - this is a caller's concern.
     */
    fun register(provider: Provider) {
        lock.write {
            check(providers.size < capacity) {
                "Can't register more than $capacity providers"
            }
            providers = providers + ProviderStatusHolder(provider, status = ProviderStatus.OK)
        }
    }

    /**
     * `unregister` removes the specified provider from the LoadBalancer based on equality.
     *
     *  There might be pending or even (for a brief period time) new requests to the removed provider after this method returns.
     */
    fun unregister(provider: Provider) {
        lock.write {
            providers = providers.filter { it.provider != provider }
        }
    }

    /**
     *  `get` returns the response of one of the active providers, determined by the balancingStrategy.
     *
     *  Active provider is a provider that is registered and healthy.
     *
     *  Throws one of the `LoadBalancingException` upon a failure.
     *
     *  In the real implementation, a hierarchy of exceptions or an Either object would've been used to indicate a kind of a problem
     *  encountered, potentially imposing certain error reporting expectations on the Provider interface contract.
     *
     *  It's possible that a just unregister provider would still receive a new call.
     *
     */
    fun get(): String? {

        //The read lock is very limited to avoid having the selected provider's `.get()` invoked from within the lock, since
        //the providers aren't controlled and a slow responding one may compromise the performance of the whole load balancer.
        val activeProviders = lock.read {
            providers.filter { it.status == ProviderStatus.OK }
        }
        if (activeProviders.isEmpty()) {
            throw NoActiveProvidersAvailableException("No active providers to serve the request")
        }

        val newPendingCallsCount = pendingCalls.incrementAndGet()

        try {
            if (simultaneousCallSingleProviderLimit != null && newPendingCallsCount > simultaneousCallSingleProviderLimit * activeProviders.size) {
                throw ClusterCapacityExceededException("Cluster capacity limit exceeded: already pending=$simultaneousCallSingleProviderLimit size=${activeProviders.size}")
            }

            val chosenProviderIndex = pickerLock.withLock {
                balancingStrategy.selectNextIndex(activeProviders.size)
            }
            val chosenProvider = activeProviders[chosenProviderIndex]

            //The call below can be potentially unstable - avoid potentially blocking locks.
            return try {chosenProvider.get()} catch (e: Exception) {
                throw UnderlyingProviderException("There was a problem calling provider", e)
            }

        } finally {
            pendingCalls.decrementAndGet()
        }
    }

    internal fun checkProvidersHealth() {
        lock.read {
            providers.toList() //make a copy to avoid calling `check` of a potentially misbehaving provider within a lock
        }.forEach {
            //a grossly misbehaving provider check implementation can still impede our health checking. To prevent this, we could've
            //performed the checks parallel, marking providers timed-out on check as unhealthy.
            it.check()
        }
    }

    private val healthCheckTimer = AtomicReference<ScheduledThreadPoolExecutor?>(null)

    fun startHealthChecking(period: Duration) {
        healthCheckTimer.getAndUpdate { previousState ->
            if (previousState != null) {
                throw IllegalStateException("The health check is already running")
            }
            val newHealthCheckExecutor = ScheduledThreadPoolExecutor(1)
            //technically, the line below produces a "weak" side-effect in a form of a scheduled job - which is warned against by java.util.concurrent.atomic.AtomicReference.getAndUpdate
            //but practically that shouldn't be a problem since the start/stop of the health check timer isn't supposed to be happening frequent on a given LB
            newHealthCheckExecutor.scheduleAtFixedRate({this.checkProvidersHealth()}, 0L, period.toNanos(), TimeUnit.NANOSECONDS)
            newHealthCheckExecutor
        }
    }

    fun stopHealthChecking(awaitTermination: Duration = Duration.ofHours(1)) {
        healthCheckTimer.getAndUpdate {
            it?.shutdown();
            it?.awaitTermination(awaitTermination.toNanos(), TimeUnit.NANOSECONDS);
            null
        }
    }

    private class ProviderStatusHolder(
            val provider: Provider,
            @Volatile
            var status: ProviderStatus
    ) {
        fun get(): String? {
            return provider.get()
        }

        fun check() {
            status = if (provider.check()) {
                //In a high contention environment, we might've considered to sync the status update more strongly,
                //but in the current setup the checks are executed sequentially upon a timer normally set to seconds
                if (status == ProviderStatus.NOK) {
                    ProviderStatus.RECOVERING
                } else {
                    ProviderStatus.OK
                }
            } else {
                ProviderStatus.NOK
            }
        }
    }

    enum class ProviderStatus{
        OK,
        RECOVERING,
        NOK
    }
}

abstract class LoadBalancingException(message: String, cause: Exception?): RuntimeException(message, cause) {
    constructor(message: String) : this(message, null)
}

class UnderlyingProviderException(message: String, cause: Exception): LoadBalancingException(message, cause)
class NoActiveProvidersAvailableException(message: String): LoadBalancingException(message)
class ClusterCapacityExceededException(message: String): LoadBalancingException(message)

//TODO: mention potential performance benefit of lambda's
//TODO: read on the thread barriers.