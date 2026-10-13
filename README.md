# Taller MapReduce: casos de COVID-19 por país

Programa en Hadoop MapReduce que calcula el número total de casos positivos de COVID-19 por país usando el dataset `owid-covid-data.csv` de Our World in Data.

## Dataset

El archivo tiene una fila por país y por día, con 67 columnas. Para este taller solo se usan dos:

| Columna     | Posición | Descripción                     |
|-------------|----------|---------------------------------|
| `location`  | 3        | Nombre del país                 |
| `new_cases` | 6        | Casos nuevos reportados ese día |

La idea es agrupar las filas por `location` y sumar todos los `new_cases` de cada país para obtener su total de casos.

## Estructura del proyecto

```
.
├── pom.xml                              # Dependencias de Hadoop 3.5.0
├── src/main/java/org/jdsotomayor/
│   ├── Covid_Mapper.java                # Emite (país, casos nuevos) por cada fila del CSV
│   ├── Covid_Reducer.java               # Suma los casos de cada país
│   └── Covid_Runner.java                # Configura y lanza el job
├── taller-mapreduce-pmdd.jar            # Jar compilado
├── entrada/owid-covid-data.csv          # Dataset que se sube a HDFS
├── resultado/part-00000                 # Salida del job
└── capturas/                            # Evidencia de la ejecución en HDFS
```

## Clases

### Covid_Mapper

Recibe cada línea del CSV y la separa por comas. Se salta el encabezado (la línea que empieza en el byte 0 del archivo) y las filas donde `new_cases` está vacío, que son los días sin casos reportados.

Como el dataset trae los casos con formato decimal (`5.0`), el valor se lee como `double` y se redondea antes de emitirlo:

```java
String[] fields = value.toString().split(",", -1);
...
country.set(fields[LOCATION]);
cases.set(Math.round(Double.parseDouble(fields[NEW_CASES])));
output.collect(country, cases);
```

### Covid_Reducer

Suma todos los valores que llegan para un mismo país. Se usa `long` en lugar de `int` porque algunos totales son bastante grandes, por ejemplo el de World pasa de 400 millones.

```java
long sum = 0;
while (values.hasNext()){
    sum+=values.next().get();
}
output.collect(key, new LongWritable(sum));
```

### Covid_Runner

Configura el job con `Text` como clave (país) y `LongWritable` como valor (casos). El reducer también se usa como combiner, así cada mapper adelanta sumas parciales antes de enviar los datos al reducer.

## Ejecución

```bash
# Compilar
mvn clean package
cp target/taller-mapreduce-pmdd-1.0-SNAPSHOT.jar taller-mapreduce-pmdd.jar

# Subir el dataset a HDFS
hdfs dfs -mkdir -p /entrada
hdfs dfs -put entrada/owid-covid-data.csv /entrada

# Ejecutar el job
hadoop jar taller-mapreduce-pmdd.jar org.jdsotomayor.Covid_Runner /entrada /salida

# Descargar el resultado
hdfs dfs -get /salida/part-00000 resultado/part-00000
```

## Resultado

La salida completa está en [`resultado/part-00000`](resultado/part-00000). Cada línea tiene el país y su total de casos separados por un tabulador:

```
Afghanistan	174339
Africa	11254551
Albania	272030
Algeria	265227
Andorra	38434
...
```

Además de los países, el dataset incluye filas que agrupan regiones o niveles de ingreso (World, Africa, Europe, European Union, High income, etc.), y estas también aparecen en el resultado con su propio total.

Vista del archivo `part-00000` desde el explorador de HDFS:

![Resultado en HDFS](capturas/1.png)
