#!/usr/bin/env bash

# Entorno de ejecución para la práctica Flink/A-B testing.
# Copiar este archivo como env.sh y sustituir EXPERIMENT_TOKEN por el token real.

export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export FLINK_HOME="$HOME/apps/flink-1.16.3"
export KAFKA_HOME="$HOME/apps/kafka_2.13-3.2.3"

export PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$KAFKA_HOME/bin:$PATH"

export KAFKA_BOOTSTRAP_SERVERS="bigdatamaster.dataspartan.com:19093,bigdatamaster.dataspartan.com:29093,bigdatamaster.dataspartan.com:39093"

export TOPIC_DEMOGRAPHIC="topic_demographic"
export TOPIC_HISTORIC="topic_historic"
export TOPIC_PREDICTION="topic_student_prediction"

# Umbral usado en el experimento final que devolvió resultado 1.
export MODEL_THRESHOLD="0.65"

# Sustituir por el token generado por la plataforma.
export EXPERIMENT_TOKEN="<TOKEN_DEL_EXPERIMENTO>"
