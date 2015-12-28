package org.apache.spark.deploy.worker

import org.apache.spark.SparkConf

object RubyWorkerStarter {
  def startWorker(master_url: String, host: String, port: Int, web_ui_port: Int, conf: SparkConf) = {
    val temp_conf = new SparkConf
    val argv = Array(master_url)
    val args = new WorkerArguments(argv, temp_conf)

    Worker.startRpcEnvAndEndpoint(host, port, web_ui_port, args.cores, args.memory, args.masters, args.workDir, conf = conf)
  }
}