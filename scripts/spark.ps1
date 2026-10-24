# ----------------------------------------------------------------------
# SciPulse Insights - run a Spark job inside the container, from Windows
#
# Usage (PowerShell, from the project root):
#   .\scripts\spark.ps1 build_silver
#   .\scripts\spark.ps1 build_silver --source arxiv
#   .\scripts\spark.ps1 run_full_pipeline
#
# Why the container: local PySpark on Windows needs Java + winutils.exe
# + HADOOP_HOME and breaks easily. In the container, Java, Spark 3.5.0
# and Python are already aligned.
#
# If PowerShell refuses to run the script:
#   Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
#
# (ASCII only on purpose: Windows PowerShell 5.1 misreads UTF-8 files
#  without BOM.)
# ----------------------------------------------------------------------
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Job,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$JobArgs
)

$EsSparkVersion = if ($env:ES_SPARK_VERSION) { $env:ES_SPARK_VERSION } else { "8.13.4" }
$Packages = "org.apache.hadoop:hadoop-aws:3.3.4,org.elasticsearch:elasticsearch-spark-30_2.12:$EsSparkVersion"
$DriverMemory = if ($env:SPARK_DRIVER_MEMORY) { $env:SPARK_DRIVER_MEMORY } else { "4g" }

$cmd = @(
    "compose", "exec", "spark",
    "/opt/spark/bin/spark-submit",
    "--master", "local[*]",
    "--driver-memory", $DriverMemory,
    "--conf", "spark.jars.ivy=/opt/ivy",
    "--packages", $Packages,
    "/opt/spark-apps/src/spark_jobs/$Job.py"
)
if ($JobArgs) { $cmd += $JobArgs }

Write-Host ">> spark-submit $Job $JobArgs" -ForegroundColor Green
& docker @cmd
exit $LASTEXITCODE
