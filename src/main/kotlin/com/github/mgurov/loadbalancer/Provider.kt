package com.github.mgurov.loadbalancer

interface Provider {
    fun get(): String
    fun check(): Boolean
}