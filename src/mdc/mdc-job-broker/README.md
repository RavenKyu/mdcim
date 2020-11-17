# Job Broker
셀러리(Celery)를 사용하여 분산 작업 요청을 처리합니다.
* 수집 데이터 DB에 넣기
* PDF 보고서 파일 생성
* 기타 긴 처리시간을 요구하는 작업

## 단독 테스트
테스트를 위한 브로커 및 레디스 컨테이너 실행
```bash
$ docker-compose -f docker-compose.test.yml up --build
```

웹으로 접속하여 동작 확인. 새로고침시 마다 새로운 작업 수행
`http://localhost:8080`

## 각 Task 테스트 코드 실행
```bash
$ docker run --rm -it mdc-job-broker:latest pytest
```