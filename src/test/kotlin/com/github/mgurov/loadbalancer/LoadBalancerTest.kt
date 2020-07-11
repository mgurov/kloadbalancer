package com.github.mgurov.loadbalancer

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.random.Random


class LoadBalancerTest {
    @Test
    fun `should reject providers exceeding capacity`() {
        val loadBalancer = LoadBalancer(capacity = 2)
        loadBalancer.register(TestProvider("first provider"))
        loadBalancer.register(TestProvider("second provider"))

        Assertions.assertThatIllegalStateException().isThrownBy {
            loadBalancer.register(TestProvider("third provides exceeds the capacity of 2"))
        }
    }

    @Test
    fun `should call random providers when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RandomBalancingStrategy(Random(0L)))
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        val actuals = (1..10).map { loadBalancer.get() }

        assertThat(actuals).containsExactly("2", "1", "2", "2", "2", "1", "1", "1", "1", "2")
    }

    @Test
    fun `should call providers sequentially when configured with such strategy`() {
        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        val actuals = (1..10).map { loadBalancer.get() }

        assertThat(actuals).containsExactly("1", "2", "1", "2", "1", "2", "1", "2", "1", "2")
    }

    @Test
    fun `should not invoke balancing strategy if no providers`() {
        val loadBalancer = LoadBalancer(balancingStrategy = object: BalancingStrategy {
            override fun selectNext(providers: List<Provider>): Provider {
                throw RuntimeException("should've not called me")
            }
        })

        assertThat(loadBalancer.get()).isNull()
    }

    @Test
    fun `should be possible to add providers`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        loadBalancer.register(TestProvider("2"))

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        loadBalancer.register(TestProvider("3"))
        //then
        assertThatCallsReturn(loadBalancer, 4, "2", "3", "1", "2")
    }

    @Test
    fun `should be possible to remove providers`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        val secondProvider = TestProvider("2")
        loadBalancer.register(secondProvider)

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1")

        //when
        loadBalancer.unregister(secondProvider)
        //then
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")
    }

    @Test
    fun `health check should disable unhealthy nodes and enable them back after two successive OKs`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(TestProvider("1"))
        val secondProvider = TestProvider("2")
        secondProvider.healthy.set(false)
        loadBalancer.register(secondProvider)

        assertThatCallsReturn(loadBalancer, 3, "1", "2", "1") //health not checked yet

        //when
        loadBalancer.checkProvidersHealth()
        //then
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")

        //provider is getting healthy
        secondProvider.healthy.set(true)
        loadBalancer.checkProvidersHealth() //first check the node is still out of the active pool
        assertThatCallsReturn(loadBalancer, 4, "1", "1", "1", "1")
        loadBalancer.checkProvidersHealth() //second check the node is back to duty
        assertThatCallsReturn(loadBalancer, 4, "1", "2", "1", "2")

    }

    @Test
    fun `shouldn't call balancing strategy when no active nodes`() {

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())
        loadBalancer.register(object: Provider {
            override fun get(): String {
                throw RuntimeException("should've not even called me")
            }

            override fun check(): Boolean {
                return false
            }

        })

        loadBalancer.checkProvidersHealth()

        assertThat(loadBalancer.get()).isNull()
    }

    @Test
    fun `should apply backpressure on exceeding requests`() {

        val mayGo = CountDownLatch(1) //TODO: choose between these two
        val hasPaused = CountDownLatch(2)

        val loadBalancer = LoadBalancer(balancingStrategy = RoundRobinBalancingStrategy())

        loadBalancer.register(object: Provider {
            override fun get(): String {
                println("new call through")
                hasPaused.countDown()
                println("awaiting")
                mayGo.await()
                println("may go")
                return "OK"
            }

            override fun check(): Boolean {
                return true
            }
        })

        val executorService: ExecutorService = ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())

        val call = object : Callable<String?> {
            override fun call(): String? {
                println("starting call")
                val result = loadBalancer.get()
                println("got result $result")
                return result
            }
        }
        val firstCall = executorService.submit(call)

        val secondCall = executorService.submit(call)

        hasPaused.await()

        val thirdCall = executorService.submit(call)

        mayGo.countDown()

        assertThat(listOf(firstCall.get(), secondCall.get(), thirdCall.get())).containsExactly("OK", "OK", null)
    }
}

//TODO: test or document provider misbehavior.
//TODO: cover interaction between number of calls and availability

//TODO: fancier assertions maybe
private fun assertThatCallsReturn(loadBalancer: LoadBalancer, upTo: Int, vararg expected: String) {
    val actuals = (1..upTo).map { loadBalancer.get() }
    assertThat(actuals).containsExactly(*expected)
}

class TestProvider(
        val id: String,
        val healthy: AtomicBoolean = AtomicBoolean(true)
): Provider {
    override fun get(): String = id
    override fun check(): Boolean = healthy.get()
}