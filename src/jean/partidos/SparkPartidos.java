package jean.partidos;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkPartidos {

	static String url = "jdbc:postgresql://localhost/dados_publicos";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkPartidosPoliticos").setMaster("local[*]");
		// create Spark Context
		SparkContext context = new SparkContext(conf);
		// create spark Session
		
		SparkSession sparkSession = new SparkSession(context);

		Dataset<Row> load_csv = sparkSession.read().format("com.databricks.spark.csv")
				.option("header", true)
				.option("delimiter", ";")
				.option("inferSchema", true)
				.load("C:\\Users\\jeans\\git\\FarejadorAssunto\\baixados\\aplic\\sead\\lista_filiados\\uf\\");
		
		System.out.println("========== SAIDA DO HEADER - AGRUPADO ============");
		load_csv.count();
		load_csv.printSchema();
		
		Dataset<Row> result = load_csv
				.groupBy("SIGLA DO PARTIDO", "NOME DO PARTIDO", "SITUACAO DO REGISTRO","MOTIVO DO CANCELAMENTO")
				.count().alias("QTD_CANDIDATOS")
				.orderBy("SIGLA DO PARTIDO")
				.withColumnRenamed("SIGLA DO PARTIDO", "sigla_partido")
				.withColumnRenamed("NOME DO PARTIDO", "nome_partido")
				.withColumnRenamed("SITUACAO DO REGISTRO", "situacao")
				.withColumnRenamed("MOTIVO DO CANCELAMENTO", "motivo_cancel")
				.withColumnRenamed("count", "qtd_candidatos");
			
		// ("hdfs://localhost:9000/usr/local/hadoop_data/loan_100.csv");
		System.out.println("========== SAIDA DO HEADER - AGRUPADO ============");
		result.printSchema();
		System.out.println("========== SHOW DE 20 REGISTRO - AGRUPADOS ==============");
		result.show(100);
		
		Dataset<Row> output = result.select("*").filter("nome_partido is not null").filter("situacao is not null");
		
		output.show(100);

		//Properties connectionProperties = new Properties();
	    //connectionProperties.put("user", "root");
	    //connectionProperties.put("password", "root");
	    //result.write().jdbc("jdbc:mysql://localhost:3306/aula", "tb_partidos", connectionProperties);

		Properties connectionProperties = new Properties();
	    connectionProperties.put("user", "postgres");
	    connectionProperties.put("password", "postgres");
	    connectionProperties.put("driver", "org.postgresql.Driver");
	    //output.write().jdbc("jdbc:postgresql://localhost/dados_publicos", "partidos", connectionProperties);
	    output.write().mode(SaveMode.Overwrite).jdbc(url, "partidos", connectionProperties);
	    sparkSession.close();
	    
	}

}
