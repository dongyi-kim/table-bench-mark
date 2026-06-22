# CLAUDE.md — table-bench-mark

이 저장소에서 작업하기 전에 읽어주세요. (지침 문서)

## 프로젝트 개요

**실시간·쓰기 빈번 워크로드**에서 **적재→조회 지연(load-to-query latency)** 이 낮아야 하는
사용자를 위해, **Iceberg 테이블 포맷** 의 조합별 성능을 **공정하고 재현 가능**하게 비교하는
벤치마크다. 적재는 **Spark** 로 고정하고, 다음을 변수로 둔다.

- **Iceberg 포맷 버전**: v2, v3
- **쓰기 모드**: COW(copy-on-write), MOR(merge-on-read)
- **compaction 정책**: 없음 / 격(매 10) 라운드 / 매 라운드 (`rewrite_data_files`)
- **조회 엔진**: 설정값 `read_engines` 로 선택(현재 **Spark**; StarRocks 도 옵션이며 **버전 차원**
  `SR_VERSIONS` 로 교체 비교 가능 — v3-MOR deletion vector 호환성 차원).

즉 **Iceberg {v2,v3} × {COW,MOR} = 4 시나리오** 를 **3 compaction 정책** 하에서 조회하여
**(시나리오 × compaction × 조회엔진)** 매트릭스의 호환성·지연·쓰기비용을 측정한다.

> 핵심 비교 관점은 **"각 compaction 정책 하에서 버전·모드를 비교"** 다: COW vs MOR, v2-MOR vs
> v3-MOR, v2-COW vs v3-COW. 측정은 **재현성**을 위해 환경을 격리하고(아래 *측정 방법론*),
> **신뢰도**를 위해 전체 매트릭스를 **N회 반복·평균**한다.

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

값은 [`benchmark/config/benchmark.yaml`](benchmark/config/benchmark.yaml) 기준(환경변수로 오버라이드).

1. **30컬럼** 테이블 생성: **double 24, 정수 3, `char(16)` 3**(원 80/10/10 비율을 단일 머신
   50라운드에 맞춰 축소). 정수 중 둘은 예약 — `pk_id`(기본키), `round_id`(적재 회차). 초기 **10만 행** 시드.
2. **매 라운드** 10만 행 upsert: `(1−U)` 비율 신규 PK + `U` 비율 기존 PK 갱신(기본 `U=0.20`).
   Spark `MERGE INTO` 로 적용. compaction 정책에 따라 `rewrite_data_files`(+maintain) 수행.
3. **freshness(쓰기→가시성)**: 커밋 직후 **최신 `round_id` 가 조회에 보일 때까지의 지연** — 경량
   가시성 프로브로 측정(아래 *측정 방법론*).
4. **조회(query)**: **최근 2개 회차**(`WHERE round_id IN (n-1, n)`, ≈20만 행)를 `query_repeats`회
   반복해 **p50/p95** 산출.
5. **2–4를 50라운드 반복**, 매 라운드 `load`/`compact`/`maintain`/`freshness`/`query` 시간 기록.

### 공정성 규칙 (위반 금지)
- **랜덤 생성은 절대 측정에 포함하지 않는다.** 모든 라운드 페이로드는 **시드 기반 Parquet** 로
  사전 1회 생성(`staging/` + MinIO 업로드)한다. 모든 후보가 동일 바이트를 사용한다. 타이머는
  오직 *적재*·*compaction*·*maintain*·*freshness*·*조회* 만 감싼다.
- **측정 직전 정착(settle, 타이머 밖)**: 각 측정 스텝 직전 `BENCH_SETTLE_S` 만큼 대기해 직전
  스텝의 잔여 IO/커밋/GC를 배수한다. 모든 후보·라운드에 동일 적용(측정값 의미 불변).
- **Polaris 메타데이터 캐시 비활성화**(`enable_iceberg_metadata_cache=false`) + 측정 조회 전
  강제 새로고침 → 신선도가 캐시에 가려지지 않는다.
- 후보마다 **새 테이블**(격리); 러너가 후보 전환 시 drop/recreate 한다.
- **자원 격리**: 컨테이너 mem/cpu 캡(.env)과 Docker VM 사이징으로 스왑/경합을 차단(아래).

## 측정 방법론 (정밀도·재현성)

벤치마크는 두 가지 신뢰성 목표를 분리해서 다룬다.

### 1) 측정환경 격리 — 오염(스파이크) 발생 자체를 막는다
`load`/`compact`/`maintain`/`freshness` 는 **라운드당 1회** 측정이라 통계로 평활화할 수 없다. 한 번의
측정 창에 스왑·GC stop-the-world·IO 스톨이 겹치면 그 값이 영구히 오염된다(과거 실측: 무작위 3~6배
스파이크). 따라서 **발생을 예방**한다.
- **Docker VM 사이징**: 스택 전체가 Docker VM 안에서 돈다. VM 메모리가 작으면 내부 스왑 thrash로
  스파이크가 난다. **VM ≥ 24g / 8코어 권장**(macOS Docker Desktop: Settings→Resources 또는
  `~/Library/Group Containers/group.com.docker/settings.json` 의 `memoryMiB`/`cpus`).
- **컨테이너 자원 캡**(.env): `SPARK_MEM`/`STARROCKS_MEM`/… 와 `*_CPUS`. **모든 `*_MEM` 합계 ≤
  VM 메모리**가 불변식. 캡은 상한이라, 측정 중 동시에 풀가동되지 않는 서비스(Spark↔StarRocks)는
  상한이 겹쳐도 무방.
- **측정 직전 정착(settle)** + **Spark 드라이버 GC 튜닝**: G1GC/`MaxGCPauseMillis`/`IHOP=35`/
  `AlwaysPreTouch`(긴 멈춤·page-fault 억제) + `network/rpc` 타임아웃(최중량 조합의 1회성 Connect 급사 방지).
- 검증 지표: **이웃-상대 고립 스파이크**(한 라운드가 양옆 평균의 1.8배 초과 + 절대 +1s)가 0이어야
  한다. 단조 증가 추세(COW가 테이블 성장에 비례)는 **정상 신호이므로 유지**한다.

### 2) freshness 프로브 — 단일 콜드 측정의 잡음원을 제거
freshness 는 한 write 당 1회만 의미가 있어 같은 write 를 반복 측정할 수 없다. 신뢰도는 두 갈래로 얻는다.
- **표본당 잡음 축소**: 무거운 `COUNT(*)` 대신 **경량 가시성 프로브**(최신 `round_id` 존재를
  `SELECT 1 … WHERE round_id=n LIMIT 1` 로 확인)로 측정해, 전체 읽기 실행비용이라는 잡음원을 제거.
  `freshness.poll_s` 를 촘촘히 둬 폴 양자화 오차도 축소. → freshness=쓰기→가시성 지연(저잡음),
  query=정상상태 읽기 p50 으로 **역할 분리**.
- **표본 수 확보**: freshness 는 라운드(50)×run(N) 의 **독립 표본 분포**(median/IQR/p95)로 본다.
  라운드별 1점이 흔들려도 분포 추정의 신뢰구간은 1/√N 로 좁아진다 — 라운드별 선이 아니라 분포로 해석.
- **엔진별 단독 측정(교차 워밍 방지)**: freshness 는 **`read_engines` 에 엔진 1개만 두고 run 별로 측정**
  한다. 두 엔진을 한 run 에서 순차 측정하면, 뒤 엔진은 앞 엔진의 조회·`REFRESH` 가 데워놓은 *정착된*
  스냅샷을 읽어 가시성 지연이 실제보다 낮게 나온다. **쓰기가 결정적**(seed → 라운드별 테이블
  byte-identical)이므로 엔진 비교는 **엔진별 단독 run 을 `aggregate` 로 병합**해 공정하게 한다
  (`aggregate` 가 `query_engine` 을 키에 포함 → 엔진별 조회는 각자 평균, 쓰기는 전체 run 평균).

### 3) 다회 반복·평균 — 통계적 신뢰도
전체 매트릭스를 **N회(기본 3) 실행**하고 `(candidate, compaction, query_engine, round, phase)` 키로
**run 간 평균**(+ run 간 표준편차)을 내 평균 결과 디렉터리를 만든다(`aggregate` 서브커맨드). 리포트는
평균값과 **변동성(CV/IQR/p95)** 을 함께 보여 잡음을 은폐하지 않는다.

### 4) 비교 리포트 구조
**compaction 정책별 패널**에서 4개 방식을 같은 축으로 비교(그래프: 패널=compaction, 선=방식).
명시적 쌍대 비교 표를 출력: **COW vs MOR**(버전별), **v2-MOR vs v3-MOR**, **v2-COW vs v3-COW**
(델타·비율·변동성). 마지막에 **freshness·write/read 확보에 좋은 구성** 결론(균형 종합).

## 실행 방법

```bash
cp .env.example .env            # 필요시 SR_VERSIONS·자원 캡 등 조정 (먼저 Docker VM ≥24g/8코어)
scripts/run.sh up               # 이미지 빌드 + 스택 기동(헬스 대기)
scripts/run.sh gen              # 시드 Parquet 사전 생성(측정 제외) — smoke/bench가 자동 수행도 함
scripts/run.sh smoke            # 4 시나리오 × 3 compaction, 소량 (정상성 점검)
scripts/run.sh bench            # 100k × 50라운드, 4 시나리오 × 3 compaction -> results/<ts>/
scripts/run.sh report           # 매트릭스 리포트(report.md) 생성
scripts/run.sh down             # 중지 (볼륨까지 삭제하려면 down -v)
```
`scripts/run.sh bench --candidate iceberg-v3-mor` 로 단일 시나리오만 실행 가능.
조회 버전 순회는 `.env` 의 `SR_VERSIONS` 로 제어한다.

**다회 반복·평균(신뢰도)**: 전체 매트릭스를 N회 실행해 각각 `results/bench-<ts>/` 에 누적한 뒤,
평균 디렉터리를 만든다.
```bash
# 같은 설정으로 N회 bench 실행 후
python -m bench aggregate --run-dirs results/bench-A results/bench-B results/bench-C \
                          --output-dir results/avg-<라벨>     # (컨테이너 내부 경로 /results/…)
```
`aggregate` 는 `(candidate,compaction,query_engine,round,phase)` 로 run 간 평균(+표준편차)을 내고
평균 결과로 단일 리포트를 생성한다.

## 새 비교 대상(시나리오) 추가법 — 확장성
한 후보 = **YAML 1개**. 코드 변경 불필요(어댑터 `iceberg_spark` 재사용).
1. `benchmark/config/candidates/<name>.yaml` 추가: `iceberg_format_version`,
   `mode`, `table_properties`(write.*.mode), `capabilities` 지정.
2. 새 엔진/포맷이 필요하면 `engines/` 에 클라이언트 추가 + `adapters/` 에 어댑터 클래스 추가
   (`base.py` 의 `Adapter` 인터페이스 구현: `prepare`, `load_round`, `run_query`, `cleanup`,
   `capabilities`). 미지원 연산은 `UnsupportedOperation` 을 던지면 러너가 `unsupported` 로 기록.

StarRocks 조회 버전을 추가/변경하려면 `.env` 의 `SR_VERSIONS` 에 태그를 넣으면 된다.

## 메트릭
`(query_engine, candidate, compaction, round, phase)` 별:
- `load`(초, staging→테이블), `compact`/`maintain`(초, rewrite/expire+orphan),
  `freshness`(초, 쓰기→가시성 지연), `query`(p50/p95, `query_repeats`회 반복; raw stats 는 `extra.stats`).
- `rows`, `status`(`ok`|`unsupported`|`failed` + 오류).

리포트(한국어)는 **호환성 매트릭스(✓/△/✗)**, compaction 정책별 **조회·freshness·적재/compaction/
maintain** 표, **쌍대 비교**(COW vs MOR / v2-MOR vs v3-MOR / v2-COW vs v3-COW; 델타·비율·변동성),
그래프(패널=compaction·선=방식), 그리고 **결론**(freshness·write/read 확보 구성)을 출력한다.
다회 평균 시 각 값은 run 간 평균이며 변동성(CV/IQR/p95)을 함께 표기한다.

## 아키텍처
`docker-compose` 스택: **minio**(+init), **postgres**(Polaris 메타데이터),
**polaris**(+admin-bootstrap 스키마 + bootstrap 카탈로그/권한), **spark**(Spark Connect, Iceberg 적재),
**starrocks**(조회, 버전 교체), **bench-runner**(Python 오케스트레이터). 공유 볼륨: `staging/`, `results/`.

코드: `benchmark/src/bench/` — `config.py`, `schema.py`, `datagen.py`, `runner.py`, `metrics.py`,
`report.py`; `engines/`(StarRocks=MySQL 프로토콜, Spark=Spark Connect, Polaris=REST);
`adapters/iceberg_spark.py`(시나리오 공통 어댑터).

## 고정 버전
[`.env.example`](.env.example) 참조. 현재: StarRocks `SR_VERSIONS=4.1.1`, Polaris 1.5,
Spark 3.5.3, Iceberg(spark-runtime) 1.10.2, Postgres 16, MinIO.
- bench-runner 베이스는 **Python 3.11**(pyspark 3.5가 3.12에서 제거된 `distutils` 사용).
- **자원 캡(.env)**: `SPARK_MEM=11g`(드라이버 힙 8g, [spark-defaults.conf](infra/spark/spark-defaults.conf)),
  `STARROCKS_MEM=6g`, `MINIO_MEM=2g`, `POLARIS_MEM=1500m`, `POSTGRES_MEM=1g`, `RUNNER_MEM=1g`(+`*_CPUS`).
  **합계 ≤ Docker VM 메모리**(권장 ≥24g). `BENCH_SETTLE_S`(측정 직전 정착, 기본 1.0s).
- **Docker VM**: macOS Docker Desktop 기준 권장 **memoryMiB≈24576 / cpus 8 / swapMiB≈4096**.

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
   - **v3-COW vs v2-COW (every_round, 계측 확정)**: 둘은 재작성 **행수·파일수·바이트가 동일**(행당
     +0.6%)인데도 v3 적재 시간만 **R40부터 1.5~1.9× 더 큼**(R50 v2 14.5s vs v3 22.2s). 즉 **쓰기량(I/O)
     차이가 아니라 v3 의무 row-lineage(`_row_id`,`_last_updated_sequence_number`) 유지의 per-row 연산
     오버헤드**가, every_round 가 테이블 전체를 매라운드 재작성하게 만드는 포화 구간(≈R35-40)에서
     계단형으로 드러나는 것. row-lineage 는 v3 필수라 비활성 불가 → **공격적 compaction 대형 COW 라면 v2 가 v3보다 쓰기 저렴**.
   - 결론: StarRocks로 Iceberg를 조회한다면 **v2-MOR** 가 적재(평탄·저비용)+조회(빠름) 모두
     유리. v3-MOR 는 적재는 빠르나 **조회 불가**라 현재 사용 부적합. COW는 조회는 되나 적재 비용 증가.
2. **StarRocks는 Iceberg 쓰기 불가** → 적재는 Spark 전담(공정성 확보).
3. **Spark Connect 서버는 로컬 모드**(local[*]) — 머신 과부하 시 하트비트 타임아웃으로 종료될 수 있다.
   최중량 조합 **v3-cow/every_round**(≈4.8M행 COW + 매라운드 compact)가 라운드48에서 1회성 급사한
   실측 있음 → **드라이버 힙 8g·`SPARK_MEM=11g`·`network/rpc` 타임아웃 600s** 로 완화. unhealthy 시
   `docker-compose up -d --force-recreate --no-deps spark` 로 복구.
4. **측정환경 오염(해결됨, 실측)**: Docker VM 이 2GB/5코어로 과소 할당돼 스택 전체가 VM 내부 스왑
   thrash → 일부 스텝 무작위 3~6배 스파이크. **VM 24g/8코어 + 자원 캡 + settle + GC 튜닝**으로
   고립 스파이크 0 달성. 새 머신에서 재현 시 **VM 사이징을 먼저 확인**할 것(*측정 방법론* 참조).
5. **Polaris 1.x(relational-jdbc)** 는 서버 기동 전에 admin-tool 로 **스키마 부트스트랩**이
   선행되어야 한다(compose의 `polaris-admin-bootstrap` 단계가 수행, 재실행에 안전).

## 트러블슈팅
- **결과 스파이크(처리시간 튐)**: 거의 항상 **Docker VM 과소 할당**으로 인한 VM 내부 스왑이 원인.
  `docker info` 로 `MemTotal`·`NCPU` 확인 → VM ≥24g/8코어로 키우고 재기동. `docker stats` 로 측정 중
  컨테이너가 한계에 닿지 않는지 확인. `*_MEM` 합계 ≤ VM 메모리인지 점검(.env).
- **freshness 값이 흔들림**: 단일 콜드 측정의 정상 잡음(고립 스파이크 아님). 경량 가시성 프로브 +
  분포(median/IQR)로 해석. 절대값은 라운드별 선이 아니라 **분포**로 본다(*측정 방법론* 참조).
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