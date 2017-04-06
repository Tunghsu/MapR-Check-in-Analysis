docker run --name=hbase-docker-1.2.4 -h hbase-docker -d -p 8080:8080 -p 2181:2181 -p 8085:8085 -p 9090:9090 -p 9095:9095 -p 16010:16010 -v $PWD/data:/data dajobe/hbase
