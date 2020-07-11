package com.github.mgurov.loadbalancer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class LoadbalancerApplication

fun main(args: Array<String>) {
	runApplication<LoadbalancerApplication>(*args)
}
