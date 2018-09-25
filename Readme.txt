Programa que obtem arquivo de partidos politicos e inseri em uma base de dados com uso da biblioteca do spark.
	
Inicar o master
C:\Spark\bin>spark-class org.apache.spark.deploy.master.Master

Iniciar o slave
C:\Spark\bin>spark-class org.apache.spark.deploy.worker.Worker spark://169.254.33.209:7077

Executar pacote
>spark-submit  --class org.apache.spark.examples.JavaSparkPi --master spark://169.254.33.209:7077 C:\Spark\examples\jars\spark-examples_2.11-2.2.1.jar	