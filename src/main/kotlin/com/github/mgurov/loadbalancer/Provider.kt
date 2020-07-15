package com.github.mgurov.loadbalancer

import java.util.*

interface Provider {
    fun get(): String?

    /**
     * False indicates the provider isn't healthy. The provider is supposed to timely respond avoiding network calls.
     */
    fun check(): Boolean
}

/**
 * This class exists to satisfy the requirement Step 1 – Generate provider.
 */
@Suppress("unused")
class ExampleProvider : Provider {

    private val instanceId = UUID.randomUUID().toString()

    override fun get() = instanceId

    override fun check() = true
}