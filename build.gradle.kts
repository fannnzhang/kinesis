// Top-level build file where you can add configuration options common to all sub-projects/modules.

buildscript {
    repositories {
        maven { url = uri("https://plugins.gradle.org/m2/") }  // 插件仓库
        mavenCentral()
        google()
    }
}

plugins {
    id("maven-publish")
    alias(libs.plugins.jetbrains.kotlin.jvm) apply false

}