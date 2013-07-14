@echo off
set FWDIR=%~dp0
set SPARK_LAUNCH_WITH_SCALA=1
if "x%ADD_JARS%"=="x" goto no_add_jars
  echo Adding JARs to classpath: %ADD_JARS%
  set TMP=%ADD_JARS:,=;%
  set SPARK_CLASSPATH=%SPARK_CLASSPATH%;%TMP%
:no_add_jars
cmd /V /E /C %FWDIR%run2.cmd spark.repl.Main %*
