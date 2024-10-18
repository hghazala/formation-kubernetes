package com.example.formation_kube

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class Hello {

    @GetMapping("/hello")
    fun hello(): String {
        return "com.example.formation_kube.Hello, World!"
    }

}
