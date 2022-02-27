# Solución Big Data Processing

## Google Cloud Console:

Lo primero que hacemos es configurar una instancia de VM(máquina virtual) en Google Cloud Platform (GCP) y hemos abierto para que comunique con todas las direcciones IP y hemos abierto el puerto de Kafka.

Se procede a realizar todo el proceso de instalación de Zookeeper con una conexión SSH a la instancia. Se comprueba que Kafka comunica abriendo otra conexión y se ha instalado antes Docker para clonar el comunicador de los datos en streaming.

Una vez que se comprueba que los datos comunican en streaming. Ya se pasa a programar en Spark.

Como los datos se van a grabar en una base de datos postgresql dentro de GCP, se levanta un postgresql para este fin.

Con esto esta montada la infraestructura para la comunicación de los datos y repositorio. Tanto de la VM como de SQL copiamos las IP externas para permitir la comunicación con el IDE que utilizamos.

## Spark streaming

Para realizar la práctica se ha usado como IDE IntelliJ IDEA CE.

### JdbcProvisoner

* El primr paso a sido configurar el esquema de las bases de datos de postgresqlen en la clase objeto **_JdbcProvisioner_**, y así se han creado las siguientes tablas:

1. bytes
2. bytes_hourly
3. metadata
4. user_metadata
5. user-quota_limit

Hemos cargado los datos de forma manual de user_metadata según el esquema anteriormente establecido.

### AntennaStreamingJobs

El siguiente paso ha sido el programar la ingesta de datos de forma streaming para terminar con el procesamiento grabado en una tabla de postgresql y la generación de unos archivos parquet para la fase en Batch. Todo esto se hace en la clase objeto **AntennaStreamingJobs**

Se ha establecido el contexto de sesión de Spark, y nos hemos conectado al topic devices de los datos que genera en streaming Kafka. 
	
* En el método **readFromKafka** leemos los datos de Kafka subscribiendonos al topic de devices.
	
* Posteriormente se parsea los datos de Kafka con el método **parserJsonData** según como los necesitamos y se parsean los datos del json que están en la columna value  parserJsonData. Primero son transformamos en una cadena de String y después los ponemos en forma columnar según una estructura que le hemos pasado previamente. Y subimos los valores de nivel para que sea el nivel siguiente al de root. Para los datos timestamp los casteamos de String a que sean de tipo Timestamp.
	
* Ahora leemos una tabla estática que es de tipo batch y por eso es spark read y se graba en la base de datos con los datos de la URI que se ha puesto para que pueda conectar con el postgres de GCP. Esto se hace en el método **readAntennaMetadata**
	
* En el método **enrichAntennaWithMetadata** lo que se hace es enriquecer los datos uniendo a los datos del consumo de los bytes los datos de los mails del usurario y de la cuota de consumo que tienen. Esto se hace haciendo un join entre las dos tablas que teníamos, que es la tabla que obtenemos hasta el método anterior con la que tenemos en postgresql de user_metadata. La únión es por el campo id de las dos tablas.
	
* En los métodos que empiezan por **computeDevices** hacemos las agregaciones que nos pide el ejercicio para obtener los KPI de análisis necesarios. En nuestro caso se han generado 3 ya que son tres las métricas que nos piden en la resolución del ejercicio. Ponemos una venta de 90 segundos con una watermark de 15 segundos para poder recoger los datos que se han podido perder en ese tiempo por conexión entre el emisor y el receptor de los datos.
	
* El método **writeToJdbc** es el que se utiliza para escribir a postgresql lo que hemos realizado antes y lo generamos como un futuro ya que se debe de generar en asincrono y paralelo para que grabe en la tabla en postgresql y se grabe también en local para generar los parquet. Se hace appends para que se vayan agregando a la tabla de postgresql y la escritura se hace por cada batch.
El resultado final son los datos que aparecen en la imagen adjunta aparte con título postgresql metodo streaming metricas
	
		
* El último método **writeToStorage** es para agregar el dataFrame de origen y enriquecido en local con formato parquet. El resultado final son los ficheros parquet que se adjuntan en imagenes aparte con el nombre de metodos streaming ficheros parquet 1 y 2.

No se lanza el programa mediante argumentos como esta establecido en **StreamingJobs** sino que se realiza encapsulando los métodos.

### AntennaBatchJobs

En este objeto clase se ingesta los datos desde los ficheros en formato parquet generados en la fase streaming y se realiza el procesamiento de los mismo mediante el método batch.

Lo que cambia es que está el método **readFromStorage** para leer desde donde se han archivado los ficheros parquet generados en streaming.

Las métricas hay una que es igual y otras dos que son diferentes. La tabla en postgresql donde se guardan los datos son distintas que en el caso anterior.

Aquí el proceso se lanza con los datos generados cada hora y ya no hay un watermark al ser estáticos y no dinámicos como en el caso streaming. También se lanza una secuencia de futuros con una ruta diferente para guardar en local.

El resultado final se puede ver en la imagen de postgresql metodo batch metricas 1 y uso. 



