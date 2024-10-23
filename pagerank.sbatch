#!/bin/bash

#SBATCH --job-name=spark-pagerank-example
#SBATCH --output=spark-pagerank.out
#SBATCH --error=spark-pagerank.err
#SBATCH --nodes=1
#SBATCH --ntasks=10
#SBATCH --mem=160G
#SBATCH --partition=caslake
#SBATCH --account=macs40123

module load python/anaconda-2022.05 spark/3.3.2

export PYSPARK_DRIVER_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3
export PYSPARK_PYTHON=/software/python-anaconda-2022.05-el8-x86_64/bin/python3

spark-submit --total-executor-cores 9 --executor-memory 16G --jars /project/macs40123/spark-jars/graphframes-0.8.3-spark3.4-s_2.12.jar create_intervention_df.py

# to filter out logging messages from output, run the following:
# cat spark.out | grep -vE "INFO|WARN|rdd|UserWarning"
