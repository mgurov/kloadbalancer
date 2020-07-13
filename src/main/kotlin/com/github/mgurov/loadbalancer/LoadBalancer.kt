package com.github.mgurov.loadbalancer

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
import kotlin.random.Random

class LoadBalancer(
        val capacity: Int = 10, //max number of providers allowed to be registered
        val balancingStrategy: BalancingStrategy = RandomBalancingStrategy(),
        val simultaneousCallSingleProviderLimit: Int? = null //TODO: document? + on high values can exceed max int.
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
     */
    fun unregister(provider: Provider) {
        lock.write {
            providers = providers.filter { it.provider != provider }
        }
    }

    //TODO: describe can return null if no backing providers available
    //TODO: describe "optimistic" selection
    /**
     *  `get` returns the response of one of the active providers, determined by the balancingStrategy.
     *
     *  Active provider is a provider that is registered, healthy and doesn't have the number of the pending calls exceeding the simultaneousCallSingleProviderLimit
     *
     *  Successfull call always returns non-null value.
     *
     *  `null` is returned when no active providers are available.
     *
     *  Possible exceptions thrown by providers aren't handled and are supposed to be handled by the callers of the method.
     *
     *  In the real implementation, a hierarchy of exceptions or an Either object would've been used to indicate a kind of a problem
     *  encountered, potentially imposing certain error reporting expectations on the Provider interface contract.
     *
     *  It's possible that a just unregister provider would still receive a new call.
     *
     *  TODO: throw exceptions.
     *
     */
    fun get(): String? {

        //The read lock is very limited to avoid having the selected provider's `.get()` invoked from within the lock, since
        //the providers aren't controlled and a slow responding one may compromise the performance of the whole load balancer.
        val activeProviders = lock.read {
            providers.filter { it.status == ProviderStatus.OK }
        }
        if (activeProviders.isEmpty()) {
            return null
        }

        val newPendingCallsCount = pendingCalls.incrementAndGet()

        try {
            if (simultaneousCallSingleProviderLimit != null && newPendingCallsCount > simultaneousCallSingleProviderLimit * activeProviders.size) {
                return null; // TODO: backpressure exception?
            }

            val chosenProviderIndex = pickerLock.withLock {
                balancingStrategy.selectNextIndex(activeProviders.size)
            }
            val chosenProvider = activeProviders[chosenProviderIndex]

            //TODO: handle exceptions here.
            //The call below can be potentially unstable - avoid potentially blocking locks.
            return chosenProvider.get()

        } finally {
            pendingCalls.decrementAndGet()
        }
    }

    //TODO: describe since we're reading from providers, those shall stay intact
    //TODO: mention single thread
    internal fun checkProvidersHealth() {
        lock.read {
            providers.forEach {
                it.check()
            }
        }
    }

    private val healthCheckExecutor = AtomicReference<ScheduledThreadPoolExecutor?>(null)

    fun startHealthChecking(period: Duration) {
        healthCheckExecutor.getAndUpdate { previousState ->
            if (previousState != null) {
                throw IllegalStateException("The health check is already running")
            }
            val newHealthCheckExecutor = ScheduledThreadPoolExecutor(1)
            newHealthCheckExecutor.scheduleAtFixedRate({this.checkProvidersHealth()}, 0L, period.toNanos(), TimeUnit.NANOSECONDS)
            //TODO: document it's not completely side-effect free, but that shouldn't matter realistically.
            newHealthCheckExecutor
        }
    }

    fun stopHealthChecking(awaitTermination: Duration = Duration.ofHours(1)) {
        healthCheckExecutor.getAndUpdate {
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
        fun get(): String {
            return provider.get()
        }

        fun check() {
            status = if (provider.check()) {
                //TODO: timing shijt.
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

//TODO: describe supposed to be called serially
interface BalancingStrategy {
    /**
     * TODO: optionsCount > 0
     */
    fun selectNextIndex(optionsCount: Int): Int
}

class RandomBalancingStrategy(
        private val random: Random = Random.Default
): BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        return random.nextInt(optionsCount) //TODO: thread unsafe re. random
    }
}

//TODO: describe simplified approach and when we'd want to use a more complicated - when want to real track which was previous and not.
class RoundRobinBalancingStrategy(
        private var position: Int = 0
): BalancingStrategy {
    override fun selectNextIndex(optionsCount: Int): Int {
        val theNextOne = position % optionsCount
        position = (position + 1) % optionsCount
        return theNextOne
   }
}

//TODO: mention potential performance benefit of lambda's