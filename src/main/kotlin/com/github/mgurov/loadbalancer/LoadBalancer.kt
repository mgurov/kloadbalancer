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

    //TODO: document up to client to ensure uniqueness of the providers.
    //TODO: document assumes infrequent modifications
    fun register(provider: Provider) {
        lock.write {
            check(providers.size < capacity) {
                "Can't register more than $capacity providers"
            }
            providers = providers + ProviderStatusHolder(provider, status = ProviderStatus.OK)
        }
    }

    //TODO: document first only and by equality
    //TODO: document returns true if unregistered false if not found
    //TODO: document I'd rather have a provider ID.
    fun unregister(provider: Provider) {
        lock.write {
            providers = providers.filter { it.provider != provider }
        }
    }

    //TODO: make it be executed periodically (every X sec)
    //TODO: describe since we're reading from providers, those shall stay intact
    //TODO: mention single thread
    fun checkProvidersHealth() {
        lock.read {
            providers.forEach {
                it.check()
            }
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
     *  TODO: fix the timing issues.
     *
     *  TODO: throw exceptions.
     *
     */
    fun get(): String? {

        //TODO: will it be OK with the timing issues?
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

            //TODO: handle exceptions here.
            return activeProviders[chosenProviderIndex].get()

        } finally {
            pendingCalls.decrementAndGet()
        }
    }

    val healthCheckExecutor = AtomicReference<ScheduledThreadPoolExecutor?>(null)

    fun startHealthChecking(period: Duration) {
        healthCheckExecutor.getAndUpdate { previousState ->
            if (previousState != null) {
                throw IllegalStateException("The health check is already running")
            }
            val newHealthCheckExecutor = ScheduledThreadPoolExecutor(1)
            newHealthCheckExecutor.scheduleAtFixedRate({this.checkProvidersHealth()}, 0L, period.toNanos(), TimeUnit.NANOSECONDS)
            newHealthCheckExecutor
        }
    }

    fun stopHealthChecking(awaitTermination: Duration = Duration.ofHours(1)) {
        healthCheckExecutor.getAndUpdate { it?.shutdown(); it?.awaitTermination(awaitTermination.toNanos(), TimeUnit.NANOSECONDS); null }
    }

    //TODO: make data class with copying
    //TODO: wrap as "provider wrapper" interface?
    //TODO: make private parts?
    class ProviderStatusHolder(
            val provider: Provider,
            @Volatile
            var status: ProviderStatus
    ) {
        fun get(): String {
            return provider.get()
        }

        fun check() {
            status = if (provider.check()) {
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