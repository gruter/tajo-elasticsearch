# Apache Tajo & Elasticsearch
* Collaborate Apache Tajo + Elasticsearch
* Apache Tajo의 External Storage로 구현된 내용입니다.
* [설치 가이드](https://github.com/gruter/tajo-elasticsearch/blob/master/README.md)

# Software Stack
* 스택이랄 것까지는 없지만 기본은 Apache Tajo + Elasticsearch 입니다.
* 각 open source 별 패키지 종속은 갖기 때문에 사용전에 확인 하시고 필요한 것들을 설치해 주셔야 합니다.
* 기본적으로 소스 받으셔서 빌드 하신 후 설치 사용하시면 됩니다.

## Apache Tajo
* Hadoop 2.3.0 or higher (up to 2.5.1)
* Java 1.6 or 1.7
* Protocol buffer 2.5.0

## Elasticsearch
* 버전별로 JDK 종속을 갖습니다.
* 1.2 이상 부터는 JDK 1.7 이상
* 1.1 이하 부터는 JDK 1.6

# 동작방식
* Apache Tajo에 external table로 생성해서 Elasticsearch로 질의하는 방식 입니다.
* 현재 구현된 기능은 아래 두 가지 입니다.
  * CREATE EXTERNAL TABLE
  * SELECT
* Meta 정보를 Tajo에서 저장하고 있고 실제 데이터는 Elasticsearch에 위치하게 됩니다.
* SQL 질의 시 Tajo에서 Elasticsearch로 QueryDSL로 변환된 질의를 수행하여 데이터를 가져오게 됩니다.
* 이렇게 획득한 데이터를 WHERE 조건에 맞게 Selection 해서 결과를 리턴하게 됩니다.

# 어디에 사용하면 될까요?
* Elasticsearch에서 JOIN 사용에 대한 아쉬움이 있으셨던 분들
* HDFS에 저장된 데이터와 함께 분석 또는 질의에 대한 요구가 있으신 분들
* HDFS 데이터에 대한 중간 결과를 Elasticsearch로 저장해서 활용하고 싶으셨던 분들
* 검색엔진은 잘 모르겠고 그냥 SQL만 아시는 분들

# JDBC Driver 사용은 가능 한가요?
* Apache Tajo의 JDBC Driver를 이용해서 사용하시면 됩니다.

# 사용 시 주의사항
* 현재 QUAL에 대한 조건이 내려오지 않아 Full Scan 하기 때문에 실시간 서비스용으로는 적합하지 않습니다.
* Batch 또는 관리/분석 도구에서 사용하는 용도로 쓰시기 바랍니다.
* QUAL을 내려 주는 기능은 Apache Tajo 팀에서 현재 구현중에 있어 완료 되면 반영할 예정입니다.

# 문의
* 요청사항이나 개선요구사항이 있으신 분들은 메일이나 이슈 등록해 주시면 최대한 반영해 보겠습니다.