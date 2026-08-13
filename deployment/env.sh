#!/bin/bash

# Environment setup for local development

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export MAVEN_HOME=$(dirname $(dirname $(readlink -f $(which mvn))))

# Spring configuration
export SPRING_DATASOURCE_URL="jdbc:postgresql://localhost:5432/iceberg_vector"
export SPRING_DATASOURCE_USERNAME="postgres"
export SPRING_DATASOURCE_PASSWORD="postgres"
export CONTROL_API_URL="http://localhost:8080"

# Iceberg configuration
export ICEBERG_CATALOG_TYPE="hadoop"
export ICEBERG_CATALOG_WAREHOUSE="s3://iceberg-warehouse/"
export ICEBERG_VECTOR_NAMESPACE="vector"

# AWS/MinIO configuration
export AWS_S3_ENDPOINT="http://localhost:9000"
export AWS_S3_ACCESS_KEY="minioadmin"
export AWS_S3_SECRET_KEY="minioadmin"
export AWS_S3_PATH_STYLE_ACCESS="true"

# Application ports
export CONTROL_API_PORT=8080
export WORKER_PORT=8081
export SEARCH_API_PORT=8082

echo "✓ Environment configured for Iceberg Vector development"
echo ""
echo "Services will listen on:"
echo "  - Control API: http://localhost:$CONTROL_API_PORT"
echo "  - Worker: http://localhost:$WORKER_PORT"
echo "  - Search API: http://localhost:$SEARCH_API_PORT"
echo ""
echo "Database: $SPRING_DATASOURCE_URL"
echo "Storage: $AWS_S3_ENDPOINT"
