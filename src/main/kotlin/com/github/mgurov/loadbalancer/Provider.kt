package com.github.mgurov.loadbalancer

interface Provider {
    fun get(): String

    /**
     * False indicates the provider isn't healthy. The provider is supposed to timely respond avoiding network calls.
     */
    fun check(): Boolean
}