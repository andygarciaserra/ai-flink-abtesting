# Práctica Flink / A-B Testing

## 1. Descripción

Esta práctica implementa un pipeline de predicción en tiempo real para decidir si se debe mostrar una nueva categoría de productos a usuarios de `silocompro.com`.

El sistema lee dos flujos de Kafka:

- `topic_demographic`: información de edad y sexo del cliente.
- `topic_historic`: histórico de compras del cliente en distintas categorías.

Ambos flujos se unen por el campo `uuid`. Una vez unidos, se aplica un modelo de clasificación previamente entrenado y exportado en formato PMML. El resultado se publica en:

- `topic_student_prediction`

El mensaje de salida tiene el siguiente formato:

```json
{"uuid":276724,"value":1,"token":"<TOKEN_DEL_EXPERIMENTO>"}
```

Donde:

- `uuid`: identificador del cliente.
- `value = 1`: mostrar la nueva categoría.
- `value = 0`: no mostrar la nueva categoría.
- `token`: token generado por la plataforma de A/B testing para el experimento.

---

## 2. Estructura del proyecto

La entrega se organiza separando la parte offline de entrenamiento y la parte online de ejecución en Flink:

```text
practica-flink-abtesting/
├── README.md
├── env.example.sh
├── .gitignore
├── offline/
│   ├── model_training/
│   │   ├── train_model.py
│   │   ├── validate_model.py
│   │   └── requirements.txt
│   ├── models/
│   │   ├── model.pmml
│   │   └── feature_order.json
│   └── reports/
│       ├── metrics.json
│       └── threshold_analysis.csv
└── flink/
    ├── build.sbt
    ├── project/
    │   ├── build.properties
    │   └── plugins.sbt
    └── src/main/
        ├── resources/
        │   ├── model.pmml
        │   └── feature_order.json
        └── scala/es/dmr/uimp/flink/
            ├── StudentPredictionJob.scala
            ├── LocalModelSmokeTest.scala
            ├── domain/Events.scala
            ├── json/JsonUtils.scala
            ├── model/PmmlLogisticRegression.scala
            ├── join/CustomerJoinFunction.scala
            └── prediction/
                ├── FeatureBuilder.scala
                └── PredictionMapFunction.scala
```

Los ficheros `model.pmml` y `feature_order.json` aparecen en dos lugares:

- `offline/models/`: artefactos generados por el entrenamiento offline.
- `flink/src/main/resources/`: artefactos usados por el job de Flink durante la ejecución.

---

## 3. Requisitos

La práctica ha sido desarrollada y probada con:

```text
Java 11
Apache Flink 1.16.3
Scala 2.12
sbt
Python 3
Kafka CLI 3.2.3, opcional para comprobaciones manuales
```

Es importante usar Java 11 para ejecutar Flink 1.16.3 de forma estable.

---

## 4. Parte offline: entrenamiento y validación

La parte offline entrena un modelo de clasificación binaria utilizando:

- información demográfica: `age`, `man`, `woman`;
- histórico de compras en categorías;
- etiqueta binaria de compra de la nueva categoría.

El modelo utilizado es una regresión logística binaria.

El dataset de entrenamiento debe colocarse en:

```text
offline/resources/X_train.csv
offline/resources/y_train.csv
```

### 4.1. Instalación de dependencias

```bash
cd offline

python3 -m venv .venv
source .venv/bin/activate

pip install -r model_training/requirements.txt
```

### 4.2. Entrenamiento del modelo

```bash
python model_training/train_model.py \
  --x resources/X_train.csv \
  --y resources/y_train.csv \
  --out models \
  --reports reports
```

Este proceso genera:

```text
models/model.pmml
models/feature_order.json
reports/metrics.json
reports/threshold_analysis.csv
```

### 4.3. Validación offline

```bash
python model_training/validate_model.py \
  --metrics reports/metrics.json \
  --thresholds reports/threshold_analysis.csv
```

### 4.4. Modelo y umbral final

El modelo devuelve una probabilidad estimada de interés del usuario por la nueva categoría.

La decisión final se toma con un umbral:

```text
probabilidad >= threshold  → value = 1
probabilidad < threshold   → value = 0
```

El umbral usado en el experimento final que obtuvo resultado positivo fue:

```text
threshold = 0.65
```

---

## 5. Parte Flink

La parte online está implementada en Apache Flink con Scala 2.12.

El pipeline realiza los siguientes pasos:

1. Lee mensajes de `topic_demographic`.
2. Lee mensajes de `topic_historic`.
3. Parsea los mensajes JSON.
4. Une ambos streams por `uuid`.
5. Construye el vector de características en el orden usado durante el entrenamiento.
6. Carga el modelo PMML.
7. Calcula la probabilidad de compra/interés.
8. Aplica el umbral configurado.
9. Publica la decisión en `topic_student_prediction`.

---

## 6. Compilación del proyecto Flink

Desde la raíz del proyecto:

```bash
cd flink
sbt clean assembly
```

El JAR generado queda en:

```text
flink/target/scala-2.12/flink-abtesting-assembly-1.0.jar
```

---

## 7. Prueba local del modelo

Antes de lanzar el job contra Kafka, se puede comprobar que el modelo PMML se carga correctamente:

```bash
cd flink
sbt "runMain es.dmr.uimp.flink.LocalModelSmokeTest"
```

Esta prueba no usa Kafka. Solo verifica que el modelo se puede cargar y que genera una predicción.

---

## 8. Configuración del entorno

Se proporciona un archivo de ejemplo:

```text
env.example.sh
```

Para ejecutar la práctica, copiarlo como `env.sh`:

```bash
cp env.example.sh env.sh
```

Editar `env.sh` y sustituir:

```bash
export EXPERIMENT_TOKEN="<TOKEN_DEL_EXPERIMENTO>"
```

por el token real generado por la plataforma.

Contenido esperado del archivo de entorno:

```bash
#!/usr/bin/env bash

export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export FLINK_HOME="$HOME/apps/flink-1.16.3"
export KAFKA_HOME="$HOME/apps/kafka_2.13-3.2.3"

export PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$KAFKA_HOME/bin:$PATH"

export KAFKA_BOOTSTRAP_SERVERS="bigdatamaster.dataspartan.com:19093,bigdatamaster.dataspartan.com:29093,bigdatamaster.dataspartan.com:39093"

export TOPIC_DEMOGRAPHIC="topic_demographic"
export TOPIC_HISTORIC="topic_historic"
export TOPIC_PREDICTION="topic_student_prediction"

export MODEL_THRESHOLD="0.65"
export EXPERIMENT_TOKEN="<TOKEN_DEL_EXPERIMENTO>"
```

Cargar el entorno:

```bash
source ./env.sh
```

---

## 9. Configuración del experimento A/B

La plataforma de experimentos se encuentra en:

```text
http://bigdatamaster.dataspartan.com/
```

Parámetros usados en el experimento:

```text
Student UID: 21701893J
Significance: 0.05
Power: 0.8
Sample size: 5678
Effect size: 0.7
```

En nuestro caso usamos un `Experiment UID` corto:

```text
ag02
```

Antes de lanzar Flink, es importante comprobar que la línea del experimento esté publicada en la web con un resultado vacío.

---

## 10. Ejecución del pipeline

### 10.1. Arrancar Flink

Desde la raíz del proyecto:

```bash
source ./env.sh
$FLINK_HOME/bin/start-cluster.sh
```

### 10.2. Lanzar el job

```bash
cd flink
source ../env.sh

flink run -d \
  -c es.dmr.uimp.flink.StudentPredictionJob \
  target/scala-2.12/flink-abtesting-assembly-1.0.jar \
  --bootstrap.servers "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic.demographic "$TOPIC_DEMOGRAPHIC" \
  --topic.historic "$TOPIC_HISTORIC" \
  --topic.prediction "$TOPIC_PREDICTION" \
  --token "$EXPERIMENT_TOKEN" \
  --threshold "$MODEL_THRESHOLD"
```

Lanzado en modo detached con `-d`.

### 10.3. Comprobar estado del job

```bash
flink list
```

El job debe aparecer en estado `RUNNING`.

---

## 11. Comprobación opcional de mensajes en Kafka

Podemos comprobar los últimos mensajes enviados y revisar si todo está enviándose en un formato correcto:

```bash
source ./env.sh

TOKEN_PREFIX="${EXPERIMENT_TOKEN:0:30}"

timeout 90s kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --topic "$TOPIC_PREDICTION" \
  --group "debug-$USER-$(date +%s)" \
  --consumer-property auto.offset.reset=latest \
  | grep -F --line-buffered "$TOKEN_PREFIX"
```

Ejemplo de mensaje esperado:

```json
{"uuid":276724,"value":1,"token":"<TOKEN_DEL_EXPERIMENTO>"}
```

---

## 12. Resultado final obtenido

El experimento final se lanzó con:

```text
Experiment UID: ag02
Threshold: 0.65
Significance: 0.05
Power: 0.8
Sample size: 5678
Effect size: 0.7
```

La plataforma devolvió:

```text
Result: 1
```

Por tanto, el sistema propuesto superó el test estadístico de la plataforma frente al sistema base.

---

## 13. Parada del sistema

Para detener el job:

```bash
flink list
flink cancel <JOB_ID>
```

Para parar el clúster local de Flink:

```bash
$FLINK_HOME/bin/stop-cluster.sh
```

Comprobar que no quedan procesos:

```bash
jps -l
```
