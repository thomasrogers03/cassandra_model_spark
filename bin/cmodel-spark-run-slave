#!/bin/bash

if [ -z "${CMODEL_SPARK_HOME}" ]; then
  export CMODEL_SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

`"${CMODEL_SPARK_HOME}/bin/cmodel-spark-env.rb"`
"${CMODEL_SPARK_HOME}/bin/cmodel-spark-slaves.rb" run
