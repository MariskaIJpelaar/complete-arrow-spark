package org.apache.spark.sql.util

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.execution.datasources.ArrowFileSourceStrategy

object ArrowSparkExtensionWrapper {
  type ExtensionBuilder = SparkSessionExtensions => Unit

  val injectArrowFileSourceStrategy: ExtensionBuilder = { e => {
    e.injectPlannerStrategy(ArrowFileSourceStrategy)
  } }

  val injectAll: ExtensionBuilder = { e => {
    e.injectPlannerStrategy(ArrowFileSourceStrategy)
  }}
}



