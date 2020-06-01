#!/usr/bin/env bash

trap cleanup_containers INT
function cleanup_containers() {
    for container in beefheart-kibana beefheart-es
    do
        docker stop $container && docker rm $container
    done
}

# Shrink down the default ES memory values for just a demo
docker run -d --name beefheart-es     -p9200:9200 -e ES_JAVA_OPTS="-Xms256M -Xmx256M" -e discovery.type=single-node \
       docker.elastic.co/elasticsearch/elasticsearch:7.4.0
docker run -d --name beefheart-kibana -p5601:5601 -e ELASTICSEARCH_HOSTS=http://beefheart-es:9200 --link beefheart-es \
       docker.elastic.co/kibana/kibana:7.4.0

# Give our containers some time to come online before the app, which needs the
# API available to load in values like mappings.
until curl http://localhost:9200
do
    echo "Waiting for Elasticsearch to come up..."
    sleep 5
done

# Finally, run the app via `make` target.
make LOG_FORMAT=$1 run
