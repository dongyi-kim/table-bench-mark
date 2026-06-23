# table-bench-mark 결과 리포트

결과 디렉터리: `bench-20260620-142102` · 라운드 수: 50 · 압축: zstd(Parquet)

## 0. 방법론 · 지표 정의

- **목적**: 쓰기가 빈번한 워크로드에서 Iceberg 구성별 **적재→조회 지연**을 공정하게 비교.
- **적재 엔진** = Apache Spark(`MERGE INTO` 업서트), **조회 엔진** = 설정된 `read_engines`(현재 Spark; StarRocks 옵션). 카탈로그 = Polaris(REST), 스토리지 = MinIO(S3).
- **시나리오** = Iceberg 포맷버전(v2/v3) × 쓰기모드(COW/MOR). **compaction 모드** = none / every_10_rounds / every_round (Spark `rewrite_data_files`, MOR의 deletion vector·delete를 데이터파일에 흡수).
- **시나리오당 흐름**: 초기 시드 후 매 라운드 10만 행 업서트(신규 80% + 기존 PK 20% 갱신) → compaction(주기 해당 시) → 각 엔진이 최근 2회차(~20만 행) 조회.
- **지표**:
  - `적재(load)` = staging→테이블 1라운드 쓰기 시간(Spark).
  - `compaction` = rewrite_data_files 1회 시간(Spark).
  - `maintain` = compaction마다 스냅샷 1개만 남기고(expire) orphan 파일 제거하는 시간(별도 추적).
  - `freshness(write→read)` = **커밋 직후 최신 round_id가 조회에 보일 때까지의 지연** (경량 가시성 프로브 폴링; 못 읽으면 실패). 한 write당 1회 측정이라 라운드별 잡음은 정상 — **분포(median/IQR/p95)** 로 해석.
  - `조회(query)` = 가시화 이후 정상상태 조회 지연(반복 측정 p50).
- **공정성·정밀도**: 랜덤 데이터는 사전 시드 Parquet으로 1회 생성(측정 제외)·동일 바이트, Polaris 메타데이터 캐시 비활성, 후보마다 새 테이블(격리), **측정 직전 정착(settle, 타이머 밖)**, 컨테이너 자원 캡 + Docker VM 사이징으로 스왑/경합 차단. **신뢰도**는 전체 매트릭스 **N회 반복·평균**(run 간 평균·변동성)으로 확보.
- **비교 관점**: compaction 정책별 패널에서 방식 비교 + 쌍대(COW vs MOR / v2-MOR vs v3-MOR / v2-COW vs v3-COW).

## 1. 호환성 매트릭스 (✓ 정상 / △ 부분 / ✗ 불가 / - 없음)

| 시나리오           | compaction      | starrocks-4.1.1   |
|----------------|-----------------|-------------------|
| iceberg-v2-cow | none            | ✓                 |
| iceberg-v2-cow | every_10_rounds | ✓                 |
| iceberg-v2-cow | every_round     | ✓                 |
| iceberg-v2-mor | none            | ✓                 |
| iceberg-v2-mor | every_10_rounds | ✓                 |
| iceberg-v2-mor | every_round     | ✓                 |
| iceberg-v3-cow | none            | ✓                 |
| iceberg-v3-cow | every_10_rounds | ✓                 |
| iceberg-v3-cow | every_round     | ✓                 |
| iceberg-v3-mor | none            | ✗                 |
| iceberg-v3-mor | every_10_rounds | ✗                 |
| iceberg-v3-mor | every_round     | ✗                 |

## 2. 조회 지연 (정상상태 p50 평균, 초)

| 시나리오           | compaction      | starrocks-4.1.1   |
|----------------|-----------------|-------------------|
| iceberg-v2-cow | none            | 0.048             |
| iceberg-v2-cow | every_10_rounds | 0.048             |
| iceberg-v2-cow | every_round     | 0.048             |
| iceberg-v2-mor | none            | 0.076             |
| iceberg-v2-mor | every_10_rounds | 0.056             |
| iceberg-v2-mor | every_round     | 0.045             |
| iceberg-v3-cow | none            | 0.051             |
| iceberg-v3-cow | every_10_rounds | 0.049             |
| iceberg-v3-cow | every_round     | 0.049             |
| iceberg-v3-mor | none            | —                 |
| iceberg-v3-mor | every_10_rounds | —                 |
| iceberg-v3-mor | every_round     | —                 |

## 3. 신선도 write→read (커밋→조회가능 지연 평균, 초)

| 시나리오           | compaction      | starrocks-4.1.1   |
|----------------|-----------------|-------------------|
| iceberg-v2-cow | none            | 0.119             |
| iceberg-v2-cow | every_10_rounds | 0.121             |
| iceberg-v2-cow | every_round     | 0.113             |
| iceberg-v2-mor | none            | 0.134             |
| iceberg-v2-mor | every_10_rounds | 0.112             |
| iceberg-v2-mor | every_round     | 0.112             |
| iceberg-v3-cow | none            | 0.150             |
| iceberg-v3-cow | every_10_rounds | 0.113             |
| iceberg-v3-cow | every_round     | 0.117             |
| iceberg-v3-mor | none            | —                 |
| iceberg-v3-mor | every_10_rounds | —                 |
| iceberg-v3-mor | every_round     | —                 |

## 4. 적재 · compaction · maintain(스냅샷 expire+orphan) 비용 (초)

| 시나리오           | compaction      |   적재 평균(s) | compaction 평균(s)   |   compaction 총합(s) | maintain 평균(s)   |   maintain 총합(s) |
|----------------|-----------------|------------|--------------------|--------------------|------------------|------------------|
| iceberg-v2-cow | none            |      6.836 | —                  |                0   | —                |              0   |
| iceberg-v2-cow | every_10_rounds |      6.826 | 7.659              |               38.3 | 1.469            |              7.3 |
| iceberg-v2-cow | every_round     |      6.667 | 6.588              |              329.4 | 0.756            |             37.8 |
| iceberg-v2-mor | none            |      3.259 | —                  |                0   | —                |              0   |
| iceberg-v2-mor | every_10_rounds |      2.297 | 8.012              |               40.1 | 1.369            |              6.8 |
| iceberg-v2-mor | every_round     |      1.877 | 6.954              |              347.7 | 0.777            |             38.8 |
| iceberg-v3-cow | none            |      8.013 | —                  |                0   | —                |              0   |
| iceberg-v3-cow | every_10_rounds |      6.515 | 7.797              |               39   | 1.331            |              6.7 |
| iceberg-v3-cow | every_round     |      8.898 | 8.035              |              401.8 | 0.924            |             46.2 |
| iceberg-v3-mor | none            |      2.134 | —                  |                0   | —                |              0   |
| iceberg-v3-mor | every_10_rounds |      1.917 | 8.147              |               40.7 | 1.381            |              6.9 |
| iceberg-v3-mor | every_round     |      1.94  | 7.740              |              387   | 0.798            |             39.9 |

## 5. compaction 정책별 방식 비교 (각 정책 하에서 v2/v3 × COW/MOR)

> CV(변동계수)는 라운드 간 변동성. freshness 는 단일 콜드 측정이라 CV 가 query 보다 큼(정상).

### compaction = `none`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.836 | 0.119          | 20%        | 0.048       | 8%      |
| v2-MOR |   3.259 | 0.134          | 15%        | 0.076       | 17%     |
| v3-COW |   8.013 | 0.150          | 64%        | 0.051       | 11%     |
| v3-MOR |   2.134 | —              | —          | —           | —       |

### compaction = `every_10_rounds`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.826 | 0.121          | 38%        | 0.048       | 9%      |
| v2-MOR |   2.297 | 0.112          | 14%        | 0.056       | 7%      |
| v3-COW |   6.515 | 0.113          | 17%        | 0.049       | 9%      |
| v3-MOR |   1.917 | —              | —          | —           | —       |

### compaction = `every_round`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.667 | 0.113          | 25%        | 0.048       | 9%      |
| v2-MOR |   1.877 | 0.112          | 20%        | 0.045       | 7%      |
| v3-COW |   8.898 | 0.117          | 32%        | 0.049       | 14%     |
| v3-MOR |   1.94  | —              | —          | —           | —       |

## 6. 쌍대 비교 (COW vs MOR · v2 vs v3)

### COW vs MOR (v2) — 비율 = v2-MOR ÷ v2-COW (＜1 이면 v2-MOR 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v2-MOR | 비율    |   fresh v2-COW |   fresh v2-MOR | 비율    |   조회 v2-COW |   조회 v2-MOR | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       6.836 |       3.259 | 0.48× |          0.119 |          0.134 | 1.13× |       0.048 |       0.076 | 1.57× |
| every_10_rounds |       6.826 |       2.297 | 0.34× |          0.121 |          0.112 | 0.93× |       0.048 |       0.056 | 1.18× |
| every_round     |       6.667 |       1.877 | 0.28× |          0.113 |          0.112 | 1.00× |       0.048 |       0.045 | 0.94× |

### COW vs MOR (v3) — 비율 = v3-MOR ÷ v3-COW (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v3-COW |   적재 v3-MOR | 비율    |   fresh v3-COW | fresh v3-MOR   | 비율   |   조회 v3-COW | 조회 v3-MOR   | 비율   |
|-----------------|-------------|-------------|-------|----------------|----------------|------|-------------|-------------|------|
| none            |       8.013 |       2.134 | 0.27× |          0.15  | —              | —    |       0.051 | —           | —    |
| every_10_rounds |       6.515 |       1.917 | 0.29× |          0.113 | —              | —    |       0.049 | —           | —    |
| every_round     |       8.898 |       1.94  | 0.22× |          0.117 | —              | —    |       0.049 | —           | —    |

### v2-MOR vs v3-MOR — 비율 = v3-MOR ÷ v2-MOR (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v2-MOR |   적재 v3-MOR | 비율    |   fresh v2-MOR | fresh v3-MOR   | 비율   |   조회 v2-MOR | 조회 v3-MOR   | 비율   |
|-----------------|-------------|-------------|-------|----------------|----------------|------|-------------|-------------|------|
| none            |       3.259 |       2.134 | 0.65× |          0.134 | —              | —    |       0.076 | —           | —    |
| every_10_rounds |       2.297 |       1.917 | 0.83× |          0.112 | —              | —    |       0.056 | —           | —    |
| every_round     |       1.877 |       1.94  | 1.03× |          0.112 | —              | —    |       0.045 | —           | —    |

### v2-COW vs v3-COW — 비율 = v3-COW ÷ v2-COW (＜1 이면 v3-COW 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v3-COW | 비율    |   fresh v2-COW |   fresh v3-COW | 비율    |   조회 v2-COW |   조회 v3-COW | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       6.836 |       8.013 | 1.17× |          0.119 |          0.15  | 1.26× |       0.048 |       0.051 | 1.06× |
| every_10_rounds |       6.826 |       6.515 | 0.95× |          0.121 |          0.113 | 0.93× |       0.048 |       0.049 | 1.02× |
| every_round     |       6.667 |       8.898 | 1.33× |          0.113 |          0.117 | 1.04× |       0.048 |       0.049 | 1.03× |

## 7. 기술 배경 · 수치 해석 (메커니즘)

> 개념: COW(데이터파일 전체 재작성) · MOR(삭제표식 추가; v2 positional delete / v3 deletion vector) · compaction(small file 병합 + 삭제 흡수) · snapshot(커밋 단위, maintain이 expire) · row-lineage(v3 행 메타데이터).

- **COW vs MOR 쓰기 메커니즘**: COW(copy-on-write)는 `MERGE` 시 갱신 행이 포함된 **데이터 파일을 통째로 다시 씁니다**. 그래서 쓰기 비용이 테이블 크기·파일 수에 비례해 커집니다. MOR(merge-on-read)은 데이터 파일을 안 건드리고 **삭제 표식만 추가**합니다 — v2는 *positional delete 파일*, v3는 *deletion vector*(데이터 파일당 Roaring 비트맵 1개). 그래서 MOR 적재는 평탄·저비용입니다. 실측 적재(none): MOR 2.70s vs COW 7.42s (MOR이 COW의 0.36배).

- **small file·snapshot·compaction**: 매 라운드 커밋은 새 데이터/삭제 파일과 **snapshot 1개**를 만들어 작은 파일이 누적됩니다. `compaction`(`rewrite_data_files`)은 small file을 큰 파일로 병합하고 삭제(deletion vector/positional delete)를 데이터에 **흡수**합니다 → 파일 수↓, 읽기·freshness 개선. 대신 쓰기 비용이 추가됩니다. 실측 compaction 총비용: every_round ≈ 366s vs every_10 ≈ 40s. `maintain`은 snapshot을 1개만 남기고(expire) orphan 파일을 제거해 메타데이터 팽창을 막습니다.

- **주기(compaction cadence)의 양면성**: `none`은 쓰기는 싸지만 파일·삭제가 쌓여 읽기 플래닝이 무거워질 수 있습니다. `every_round`는 매 라운드 파일을 정리해 읽기/freshness가 가장 좋지만, **COW에서는 정리된 소수 대형 파일을 다음 MERGE가 거의 전체 재작성**하게 만들어 후반 적재가 급증합니다. 실측 every_round 적재 최종라운드: v3-COW 25.37s vs v2-COW 13.26s (v3/v2 ≈ 1.91배) — v3 row-lineage 오버헤드가 전체 재작성에서 드러남.

- **MOR 읽기와 compaction**: MOR은 조회 시 삭제 표식을 실시간 병합하므로 compaction 전에는 읽기가 느릴 수 있습니다(특히 v2 positional delete는 여러 작은 삭제 파일을 reconcile). compaction이 삭제를 흡수하면 COW급으로 빨라집니다. 실측 MOR 조회: none 0.076s → every_round 0.045s.

- **v2 vs v3**: v3는 deletion vector로 MOR 읽기가 v2(positional delete)보다 유리하고 삭제가 쌓여도 데이터 파일당 비트맵 1개라 성능이 덜 악화됩니다. 단 v3는 행마다 **row-lineage**(`_row_id`,`_last_updated_sequence_number`) 2개 컬럼을 유지하므로, 전체 재작성이 일어나는 구간(COW every_round 후반)에서 v2 대비 추가 쓰기 비용이 더 드러납니다.

- **freshness 해석**: freshness는 *쓰기→가시성 지연*으로, 본질적으로 새 snapshot의 메타데이터 플래닝 비용에 가깝습니다(데이터 본문 읽기는 제외한 경량 프로브). Spark는 자기 커밋을 즉시 보고, StarRocks는 메타캐시 비활성이라 측정에 `REFRESH EXTERNAL TABLE`이 포함됩니다 — 엔진 간 freshness는 정의가 다소 다릅니다.

## 9. 그래프 (패널=compaction · 선=방식)

### 적재 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)

![적재 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)](fig_load.png)

### 조회 지연 vs 라운드 (compaction 주기별 패널 · 방식 비교)

![조회 지연 vs 라운드 (compaction 주기별 패널 · 방식 비교)](fig_query.png)

### 신선도 write→read vs 라운드 (compaction 주기별 패널 · 방식 비교)

![신선도 write→read vs 라운드 (compaction 주기별 패널 · 방식 비교)](fig_freshness.png)

### compaction 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)

![compaction 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)](fig_compaction.png)

### 스냅샷 expire+orphan 제거 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)

![스냅샷 expire+orphan 제거 시간 vs 라운드 (compaction 주기별 패널 · 방식 비교)](fig_maintain.png)

## 10. 시나리오별 해설

- **iceberg-v2-cow**: 적재 7.6s→12.0s (증가(테이블 성장 비례, COW 특성)). compaction 평균 6.6s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.048s.
- **iceberg-v2-mor**: 적재 4.0s→4.5s (평탄(MOR 특성)). compaction 평균 7.0s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.076s.
- **iceberg-v3-cow**: 적재 5.5s→10.9s (증가(테이블 성장 비례, COW 특성)). compaction 평균 8.0s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.051s.
- **iceberg-v3-mor**: 적재 6.0s→2.2s (평탄(MOR 특성)). compaction 평균 7.7s. StarRocks 호환: none=✗, every_10_rounds=✗, every_round=✗.

## 11. 종합 해설

- **v3-MOR × starrocks-4.1.1** compaction별 호환성: none=✗, every_10_rounds=✗, every_round=✗ → deletion vector를 compaction으로 제거해야 StarRocks가 읽을 수 있음.
- **starrocks-4.1.1** 최저 조회 지연: `iceberg-v2-mor` / every_round (0.045s)

### 결론 — freshness · write/read 확보에 좋은 구성

- **적재(write) 최저**: `v2-MOR` / every_round (1.877s) — MOR 계열이 평탄·저비용.
- **조회(read) 최저 p50**: `v2-MOR` / every_round (0.045s).
- **freshness 최저**: `v2-MOR` / every_10_rounds (0.112s).
- **균형 종합 권장**: `v2-MOR` / `every_round` (정규화 점수 1.00, 1.0=모든 지표 최저) — 적재·freshness·조회를 동일 가중으로 합산한 최적. 실시간·쓰기빈번(적재→조회 지연 최소화) 워크로드 기준.
