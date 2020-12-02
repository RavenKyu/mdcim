FROM python:3.8.6-alpine3.12

# ==============================================================================
# 타임존 설정
RUN apk add tzdata && \
    cp /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    echo "Asia/Seoul" > /etc/timezone

ENV PYTHONUNBUFFERED=0

# ==============================================================================
RUN apk add --no-cache --virtual .build-deps gcc musl-dev
ADD data_collector/requirements.txt /tmp/.
RUN pip install -r /tmp/requirements.txt

RUN mkdir -p /src/data_collector
ADD data_collector /src

ADD . /src
WORKDIR /src


# ==============================================================================
# 설치파일 정리
WORKDIR /src
RUN apk del .build-deps

ENTRYPOINT ["python", "-m", "data_collector"]
