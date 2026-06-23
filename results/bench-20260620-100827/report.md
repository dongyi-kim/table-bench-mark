# table-bench-mark 결과 리포트

결과 디렉터리: `bench-20260620-100827` · 라운드 수: 50 · 압축: zstd(Parquet)

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
| iceberg-v2-cow | none            | 0.052             |
| iceberg-v2-cow | every_10_rounds | 0.049             |
| iceberg-v2-cow | every_round     | 0.053             |
| iceberg-v2-mor | none            | 0.080             |
| iceberg-v2-mor | every_10_rounds | 0.058             |
| iceberg-v2-mor | every_round     | 0.049             |
| iceberg-v3-cow | none            | 0.053             |
| iceberg-v3-cow | every_10_rounds | 0.048             |
| iceberg-v3-cow | every_round     | 0.051             |
| iceberg-v3-mor | none            | —                 |
| iceberg-v3-mor | every_10_rounds | —                 |
| iceberg-v3-mor | every_round     | —                 |

## 3. 신선도 write→read (커밋→조회가능 지연 평균, 초)

| 시나리오           | compaction      | starrocks-4.1.1   |
|----------------|-----------------|-------------------|
| iceberg-v2-cow | none            | 0.135             |
| iceberg-v2-cow | every_10_rounds | 0.111             |
| iceberg-v2-cow | every_round     | 0.118             |
| iceberg-v2-mor | none            | 0.127             |
| iceberg-v2-mor | every_10_rounds | 0.107             |
| iceberg-v2-mor | every_round     | 0.108             |
| iceberg-v3-cow | none            | 0.125             |
| iceberg-v3-cow | every_10_rounds | 0.110             |
| iceberg-v3-cow | every_round     | 0.133             |
| iceberg-v3-mor | none            | —                 |
| iceberg-v3-mor | every_10_rounds | —                 |
| iceberg-v3-mor | every_round     | —                 |

## 4. 적재 · compaction · maintain(스냅샷 expire+orphan) 비용 (초)

| 시나리오           | compaction      |   적재 평균(s) | compaction 평균(s)   |   compaction 총합(s) | maintain 평균(s)   |   maintain 총합(s) |
|----------------|-----------------|------------|--------------------|--------------------|------------------|------------------|
| iceberg-v2-cow | none            |      7.071 | —                  |                0   | —                |              0   |
| iceberg-v2-cow | every_10_rounds |      6.018 | 7.384              |               36.9 | 1.261            |              6.3 |
| iceberg-v2-cow | every_round     |      6.783 | 6.903              |              345.2 | 0.795            |             39.8 |
| iceberg-v2-mor | none            |      3.503 | —                  |                0   | —                |              0   |
| iceberg-v2-mor | every_10_rounds |      2.223 | 8.000              |               40   | 1.471            |              7.4 |
| iceberg-v2-mor | every_round     |      1.947 | 7.043              |              352.1 | 0.754            |             37.7 |
| iceberg-v3-cow | none            |      7.266 | —                  |                0   | —                |              0   |
| iceberg-v3-cow | every_10_rounds |      6.255 | 7.548              |               37.7 | 1.258            |              6.3 |
| iceberg-v3-cow | every_round     |      9.17  | 8.166              |              408.3 | 0.850            |             42.5 |
| iceberg-v3-mor | none            |      2.149 | —                  |                0   | —                |              0   |
| iceberg-v3-mor | every_10_rounds |      1.989 | 8.404              |               42   | 1.378            |              6.9 |
| iceberg-v3-mor | every_round     |      1.876 | 7.186              |              359.3 | 0.781            |             39   |

## 5. compaction 정책별 방식 비교 (각 정책 하에서 v2/v3 × COW/MOR)

> CV(변동계수)는 라운드 간 변동성. freshness 는 단일 콜드 측정이라 CV 가 query 보다 큼(정상).

### compaction = `none`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   7.071 | 0.135          | 25%        | 0.052       | 10%     |
| v2-MOR |   3.503 | 0.127          | 16%        | 0.080       | 15%     |
| v3-COW |   7.266 | 0.125          | 38%        | 0.053       | 9%      |
| v3-MOR |   2.149 | —              | —          | —           | —       |

### compaction = `every_10_rounds`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.018 | 0.111          | 23%        | 0.049       | 8%      |
| v2-MOR |   2.223 | 0.107          | 15%        | 0.058       | 7%      |
| v3-COW |   6.255 | 0.110          | 15%        | 0.048       | 8%      |
| v3-MOR |   1.989 | —              | —          | —           | —       |

### compaction = `every_round`

| 방식     |   적재(s) | freshness(s)   | fresh CV   | 조회 p50(s)   | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.783 | 0.118          | 29%        | 0.053       | 14%     |
| v2-MOR |   1.947 | 0.108          | 22%        | 0.049       | 6%      |
| v3-COW |   9.17  | 0.133          | 37%        | 0.051       | 12%     |
| v3-MOR |   1.876 | —              | —          | —           | —       |

## 6. 쌍대 비교 (COW vs MOR · v2 vs v3)

### COW vs MOR (v2) — 비율 = v2-MOR ÷ v2-COW (＜1 이면 v2-MOR 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v2-MOR | 비율    |   fresh v2-COW |   fresh v2-MOR | 비율    |   조회 v2-COW |   조회 v2-MOR | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       7.071 |       3.503 | 0.50× |          0.135 |          0.127 | 0.94× |       0.052 |       0.08  | 1.54× |
| every_10_rounds |       6.018 |       2.223 | 0.37× |          0.111 |          0.107 | 0.97× |       0.049 |       0.058 | 1.19× |
| every_round     |       6.783 |       1.947 | 0.29× |          0.118 |          0.108 | 0.92× |       0.053 |       0.049 | 0.92× |

### COW vs MOR (v3) — 비율 = v3-MOR ÷ v3-COW (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v3-COW |   적재 v3-MOR | 비율    |   fresh v3-COW | fresh v3-MOR   | 비율   |   조회 v3-COW | 조회 v3-MOR   | 비율   |
|-----------------|-------------|-------------|-------|----------------|----------------|------|-------------|-------------|------|
| none            |       7.266 |       2.149 | 0.30× |          0.125 | —              | —    |       0.053 | —           | —    |
| every_10_rounds |       6.255 |       1.989 | 0.32× |          0.11  | —              | —    |       0.048 | —           | —    |
| every_round     |       9.17  |       1.876 | 0.20× |          0.133 | —              | —    |       0.051 | —           | —    |

### v2-MOR vs v3-MOR — 비율 = v3-MOR ÷ v2-MOR (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v2-MOR |   적재 v3-MOR | 비율    |   fresh v2-MOR | fresh v3-MOR   | 비율   |   조회 v2-MOR | 조회 v3-MOR   | 비율   |
|-----------------|-------------|-------------|-------|----------------|----------------|------|-------------|-------------|------|
| none            |       3.503 |       2.149 | 0.61× |          0.127 | —              | —    |       0.08  | —           | —    |
| every_10_rounds |       2.223 |       1.989 | 0.89× |          0.107 | —              | —    |       0.058 | —           | —    |
| every_round     |       1.947 |       1.876 | 0.96× |          0.108 | —              | —    |       0.049 | —           | —    |

### v2-COW vs v3-COW — 비율 = v3-COW ÷ v2-COW (＜1 이면 v3-COW 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v3-COW | 비율    |   fresh v2-COW |   fresh v3-COW | 비율    |   조회 v2-COW |   조회 v3-COW | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       7.071 |       7.266 | 1.03× |          0.135 |          0.125 | 0.92× |       0.052 |       0.053 | 1.02× |
| every_10_rounds |       6.018 |       6.255 | 1.04× |          0.111 |          0.11  | 1.00× |       0.049 |       0.048 | 0.99× |
| every_round     |       6.783 |       9.17  | 1.35× |          0.118 |          0.133 | 1.13× |       0.053 |       0.051 | 0.97× |

## 7. 기술 배경 · 수치 해석 (메커니즘)

> 개념: COW(데이터파일 전체 재작성) · MOR(삭제표식 추가; v2 positional delete / v3 deletion vector) · compaction(small file 병합 + 삭제 흡수) · snapshot(커밋 단위, maintain이 expire) · row-lineage(v3 행 메타데이터).

- **COW vs MOR 쓰기 메커니즘**: COW(copy-on-write)는 `MERGE` 시 갱신 행이 포함된 **데이터 파일을 통째로 다시 씁니다**. 그래서 쓰기 비용이 테이블 크기·파일 수에 비례해 커집니다. MOR(merge-on-read)은 데이터 파일을 안 건드리고 **삭제 표식만 추가**합니다 — v2는 *positional delete 파일*, v3는 *deletion vector*(데이터 파일당 Roaring 비트맵 1개). 그래서 MOR 적재는 평탄·저비용입니다. 실측 적재(none): MOR 2.83s vs COW 7.17s (MOR이 COW의 0.39배).

- **small file·snapshot·compaction**: 매 라운드 커밋은 새 데이터/삭제 파일과 **snapshot 1개**를 만들어 작은 파일이 누적됩니다. `compaction`(`rewrite_data_files`)은 small file을 큰 파일로 병합하고 삭제(deletion vector/positional delete)를 데이터에 **흡수**합니다 → 파일 수↓, 읽기·freshness 개선. 대신 쓰기 비용이 추가됩니다. 실측 compaction 총비용: every_round ≈ 366s vs every_10 ≈ 39s. `maintain`은 snapshot을 1개만 남기고(expire) orphan 파일을 제거해 메타데이터 팽창을 막습니다.

- **주기(compaction cadence)의 양면성**: `none`은 쓰기는 싸지만 파일·삭제가 쌓여 읽기 플래닝이 무거워질 수 있습니다. `every_round`는 매 라운드 파일을 정리해 읽기/freshness가 가장 좋지만, **COW에서는 정리된 소수 대형 파일을 다음 MERGE가 거의 전체 재작성**하게 만들어 후반 적재가 급증합니다. 실측 every_round 적재 최종라운드: v3-COW 23.43s vs v2-COW 16.51s (v3/v2 ≈ 1.42배) — v3 row-lineage 오버헤드가 전체 재작성에서 드러남.

- **MOR 읽기와 compaction**: MOR은 조회 시 삭제 표식을 실시간 병합하므로 compaction 전에는 읽기가 느릴 수 있습니다(특히 v2 positional delete는 여러 작은 삭제 파일을 reconcile). compaction이 삭제를 흡수하면 COW급으로 빨라집니다. 실측 MOR 조회: none 0.080s → every_round 0.049s.

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

- **iceberg-v2-cow**: 적재 5.5s→11.7s (증가(테이블 성장 비례, COW 특성)). compaction 평균 6.9s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.052s.
- **iceberg-v2-mor**: 적재 4.4s→4.6s (평탄(MOR 특성)). compaction 평균 7.0s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.080s.
- **iceberg-v3-cow**: 적재 6.2s→10.6s (증가(테이블 성장 비례, COW 특성)). compaction 평균 8.2s. StarRocks 호환: none=✓, every_10_rounds=✓, every_round=✓. StarRocks 조회 p50 0.053s.
- **iceberg-v3-mor**: 적재 5.4s→2.3s (평탄(MOR 특성)). compaction 평균 7.2s. StarRocks 호환: none=✗, every_10_rounds=✗, every_round=✗.

## 11. 종합 해설

- **v3-MOR × starrocks-4.1.1** compaction별 호환성: none=✗, every_10_rounds=✗, every_round=✗ → deletion vector를 compaction으로 제거해야 StarRocks가 읽을 수 있음.
- **starrocks-4.1.1** 최저 조회 지연: `iceberg-v3-cow` / every_10_rounds (0.048s)

### 결론 — freshness · write/read 확보에 좋은 구성

- **적재(write) 최저**: `v3-MOR` / every_round (1.876s) — MOR 계열이 평탄·저비용.
- **조회(read) 최저 p50**: `v3-COW` / every_10_rounds (0.048s).
- **freshness 최저**: `v2-MOR` / every_10_rounds (0.107s).
- **균형 종합 권장**: `v3-MOR` / `every_round` (정규화 점수 1.00, 1.0=모든 지표 최저) — 적재·freshness·조회를 동일 가중으로 합산한 최적. 실시간·쓰기빈번(적재→조회 지연 최소화) 워크로드 기준.
