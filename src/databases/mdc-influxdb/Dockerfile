FROM influxdb:1.7-alpine
RUN apk add tzdata && \
    cp /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    echo "Asia/Seoul" > /etc/timezone
ADD influxdb.conf /etc/influxdb
