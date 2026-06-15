# CLAUDE.md — table-bench-mark

이 저장소에서 작업하기 전에 읽어주세요. (지침 문서)

## 프로젝트 개요

**실시간·쓰기 빈번 워크로드**에서 **적재→조회 지연(load-to-query latency)** 이 낮아야 하는
사용자를 위해, **Iceberg 테이블 포맷** 의 조합별 성능을 **공정하고 재현 가능**하게 비교하는
벤치마크다. 적재는 **Spark**, 조회는 **StarRocks** 로 고정하고, 다음을 변수로 둔다.

- **Iceberg 포맷 버전**: v2, v3
- **쓰기 모드**: COW(copy-on-write), MOR(merge-on-read)
- **StarRocks(조회) 버전**: 3.5, 4.1

즉 **Iceberg {v2,v3} × {COW,MOR} = 4 시나리오** 를 **StarRocks 각 버전** 으로 조회하여
**(시나리오 × 조회엔진 버전)** 매트릭스의 호환성과 지연을 측정한다.

## 비교 후보 (4개)

| 후보 | format-version | write.{delete,update,merge}.mode | 적재 | 조회 |
|---|---|---|---|---|
| `iceberg-v2-cow` | 2 | copy-on-write | Spark | StarRocks |
| `iceberg-v2-mor` | 2 | merge-on-read  | Spark | StarRocks |
| `iceberg-v3-cow` | 3 | copy-on-write | Spark | StarRocks |
| `iceberg-v3-mor` | 3 | merge-on-read  | Spark | StarRocks |

모두 Polaris(Iceberg REST) 카탈로그 + MinIO(S3) 웨어하우스를 사용한다.

## 왜 Spark가 적재하나
StarRocks는 Iceberg를 **쓰지 못한다**(외부 카탈로그에 `format-version` 지정 시 충돌). 따라서
네 시나리오의 적재 엔진을 **Spark로 통일**하여 공정성을 확보하고(동일 엔진·동일 바이트),
StarRocks는 **순수 조회 엔진** 으로서 버전만 바꿔 비교한다. Spark는 v2/v3 및 COW/MOR,
`MERGE INTO` 업서트를 모두 지원한다.

## StarRocks 버전 차원 (순차 교체)
단일 StarRocks 컨테이너의 버전을 바꿔가며 **버전당 1회** 4 시나리오를 실행하고, 결과를
**하나의 디렉터리에 누적** 후 단일 리포트로 병합한다. `scripts/run.sh` 가 `.env` 의
`SR_VERSIONS`(예: `3.5.5 4.1.1`)를 순회하며 컨테이너 교체 → healthy 대기 → 러너 실행을 반복한다.
각 결과 레코드에는 `query_engine`(예: `starrocks-4.1.1`)이 기록된다.

## 벤치마크 시나리오 (후보별)

1. **100컬럼** 테이블 생성: **double 80, 정수 10, `char(16)` 10**. 정수 중 둘은 예약 —
   `pk_id`(기본키), `round_id`(적재 회차). 초기 **10만 행** 시드.
2. **매 라운드** 10만 행 upsert: `(1−U)` 비율 신규 PK + `U` 비율 기존 PK 갱신(기본 `U=0.20`).
   Spark `MERGE INTO` 로 적용. 신규/갱신 행은 현재 `round_id` 를 가진다.
3. **최근 2개 회차 조회**(`WHERE round_id IN (n-1, n)`, ≈20만 행) — 지연 측정.
4. **2–3을 10회 반복**, 매 라운드 `load`/`query` 시간 기록.

### 공정성 규칙 (위반 금지)
- **랜덤 생성은 절대 측정에 포함하지 않는다.** 모든 라운드 페이로드는 **시드 기반 Parquet** 로
  사전 1회 생성(`staging/` + MinIO 업로드)한다. 모든 후보가 동일 바이트를 사용한다. 타이머는
  오직 *적재*(staging→테이블)와 *조회* 만 감싼다.
- **Polaris 메타데이터 캐시 비활성화**(`enable_iceberg_metadata_cache=false`) + 측정 조회 전
  강제 새로고침 → 신선도가 캐시에 가려지지 않는다.
- 후보마다 **새 테이블**(격리); 러너가 후보 전환 시 drop/recreate 한다.

## 실행 방법

```bash
cp .env.example .env            # 필요시 SR_VERSIONS 등 조정
scripts/run.sh up               # 이미지 빌드 + 스택 기동(헬스 대기)
scripts/run.sh gen              # 시드 Parquet 사전 생성(측정 제외) — smoke/bench가 자동 수행도 함
scripts/run.sh smoke            # 4 시나리오 × SR버전, 소량 1라운드 (정상성 점검)
scripts/run.sh bench            # 100k × 10라운드, 4 시나리오 × SR버전 -> results/<ts>/
scripts/run.sh report           # 매트릭스 리포트(report.md) 생성
scripts/run.sh down             # 중지 (볼륨까지 삭제하려면 down -v)
```
`scripts/run.sh bench --candidate iceberg-v3-mor` 로 단일 시나리오만 실행 가능.
버전 순회는 `.env` 의 `SR_VERSIONS` 로 제어한다.

## 새 비교 대상(시나리오) 추가법 — 확장성
한 후보 = **YAML 1개**. 코드 변경 불필요(어댑터 `iceberg_spark` 재사용).
1. `benchmark/config/candidates/<name>.yaml` 추가: `iceberg_format_version`,
   `mode`, `table_properties`(write.*.mode), `capabilities` 지정.
2. 새 엔진/포맷이 필요하면 `engines/` 에 클라이언트 추가 + `adapters/` 에 어댑터 클래스 추가
   (`base.py` 의 `Adapter` 인터페이스 구현: `prepare`, `load_round`, `run_query`, `cleanup`,
   `capabilities`). 미지원 연산은 `UnsupportedOperation` 을 던지면 러너가 `unsupported` 로 기록.

StarRocks 조회 버전을 추가/변경하려면 `.env` 의 `SR_VERSIONS` 에 태그를 넣으면 된다.

## 메트릭
(query_engine, candidate, round) 별: `load`(초), `query`(p50/p95, 반복 측정), `rows`(적재행/조회행),
`status`(`ok`|`unsupported`|`failed` + 오류). 리포트는 **호환성 매트릭스(✓/△/✗)**, 조회·적재 지연
매트릭스, 자동 해설(최저 지연, 비호환 사유)을 한국어로 출력한다.

## 아키텍처
`docker-compose` 스택: **minio**(+init), **postgres**(Polaris 메타데이터),
**polaris**(+admin-bootstrap 스키마 + bootstrap 카탈로그/권한), **spark**(Spark Connect, Iceberg 적재),
**starrocks**(조회, 버전 교체), **bench-runner**(Python 오케스트레이터). 공유 볼륨: `staging/`, `results/`.

코드: `benchmark/src/bench/` — `config.py`, `schema.py`, `datagen.py`, `runner.py`, `metrics.py`,
`report.py`; `engines/`(StarRocks=MySQL 프로토콜, Spark=Spark Connect, Polaris=REST);
`adapters/iceberg_spark.py`(시나리오 공통 어댑터).

## 고정 버전
[`.env.example`](.env.example) 참조. 현재: StarRocks `SR_VERSIONS=3.5.5 4.1.1`, Polaris 1.5,
Spark 3.5.3, Iceberg(spark-runtime) 1.10.2, Postgres 16, MinIO.
- bench-runner 베이스는 **Python 3.11**(pyspark 3.5가 3.12에서 제거된 `distutils` 사용).

## 카탈로그/포맷 설정 치트시트
- **StarRocks → Polaris(Iceberg REST)** 외부 카탈로그: `iceberg.catalog.type=rest`,
  `iceberg.catalog.uri=http://polaris:8181/api/catalog`, `iceberg.catalog.warehouse=<catalog>`,
  `security=oauth2`, `oauth2.credential=<id>:<secret>`, `oauth2.scope=PRINCIPAL_ROLE:ALL`,
  MinIO S3 속성(`aws.s3.*`, path-style), **`enable_iceberg_metadata_cache=false`**.
- **Spark → Polaris**: `spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog`,
  `type=rest`, `uri/warehouse/credential/scope/oauth2-server-uri`, `io-impl=S3FileIO`,
  `s3.endpoint/s3.path-style-access/...` (infra/spark/spark-defaults.conf).
- **COW vs MOR**: `write.{delete,update,merge}.mode = copy-on-write | merge-on-read`.
- **포맷 버전**: `TBLPROPERTIES('format-version'='2'|'3')`.

## 알려진 호환성 / 리스크 (실측 반영)
1. **Iceberg v3 MOR(deletion vector) 조회 불가 — StarRocks 3.5·4.1 모두**
   (`Parquet file magic not matched`). 4.1 업그레이드로도 해소되지 않음(실측 확정).
   v3 COW, v2 COW/MOR 는 두 버전 모두 정상.
   - 쓰기 특성(실측): **COW 적재 비용은 테이블이 커질수록 증가**(merge 시 데이터파일 재작성;
     라운드10 기준 ~12s), **MOR 적재는 평탄·저비용**(delete 파일만; ~4s 유지).
   - 결론: StarRocks로 Iceberg를 조회한다면 **v2-MOR** 가 적재(평탄·저비용)+조회(빠름) 모두
     유리. v3-MOR 는 적재는 빠르나 **조회 불가**라 현재 사용 부적합. COW는 조회는 되나 적재 비용 증가.
2. **StarRocks는 Iceberg 쓰기 불가** → 적재는 Spark 전담(공정성 확보).
3. **Spark Connect 서버는 로컬 모드**(local[*]) — 머신 과부하 시 executor 하트비트 타임아웃으로
   종료될 수 있다. unhealthy 시 `docker-compose up -d --force-recreate --no-deps spark` 로 복구.
4. **Polaris 1.x(relational-jdbc)** 는 서버 기동 전에 admin-tool 로 **스키마 부트스트랩**이
   선행되어야 한다(compose의 `polaris-admin-bootstrap` 단계가 수행, 재실행에 안전).

## 트러블슈팅
- **이미지 pull TLS 타임아웃**: 동시 pull 충돌/일시적 네트워크. 단일 pull로 재시도.
- **Flink 흔적 제거됨**: 이전 설계의 Flink/Paimon/StarRocks-internal 은 모두 제거됨.
- `docker-compose`(v1.29 standalone) 사용 — `.env` 치환과 healthcheck 조건을 지원.

## 컨벤션
- Python: stdlib + `pyarrow`, `pandas`, `numpy`, `pymysql`, `requests`, `pyyaml`, `pyspark`.
- 모든 튜너블은 `benchmark/config/*.yaml` 과 `.env` 에. 코드에 매직넘버 금지.
- 결과는 `results/<timestamp>/` 에 append-only. 기존 실행을 덮어쓰지 않는다.
- 결정성: 모든 난수는 `datagen.py` 의 시드 생성기를 거친다.
- 문서·리포트 출력은 **한국어**, 코드 주석/docstring 은 영어.


<claude-mem-context>
# Recent Activity

<!-- This section is auto-generated by claude-mem. Edit content outside the tags. -->

*No recent activity*
</claude-mem-context>