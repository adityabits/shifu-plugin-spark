shifu-plugin-spark
==========
Plugin for Shifu.ml

Normalizes data and computes stats using Spark.
Should be run as a plugin for shifu.ml

Usage:
  - Create an assembly jar of the plugin using maven: "mvn package"
  - Copy the assembly jar from the target directory into SHIFU_HOME/lib
  - Use via Shifu using appropriate request object for stats or normalization
