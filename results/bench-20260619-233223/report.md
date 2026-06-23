# table-bench-mark 결과 리포트

결과 디렉터리: `bench-20260619-233223` · 라운드 수: 50 · 압축: zstd(Parquet)

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

| 시나리오           | compaction      | spark   |
|----------------|-----------------|---------|
| iceberg-v2-cow | none            | ✓       |
| iceberg-v2-cow | every_10_rounds | ✓       |
| iceberg-v2-cow | every_round     | ✓       |
| iceberg-v2-mor | none            | ✓       |
| iceberg-v2-mor | every_10_rounds | ✓       |
| iceberg-v2-mor | every_round     | ✓       |
| iceberg-v3-cow | none            | ✓       |
| iceberg-v3-cow | every_10_rounds | ✓       |
| iceberg-v3-cow | every_round     | ✓       |
| iceberg-v3-mor | none            | ✓       |
| iceberg-v3-mor | every_10_rounds | ✓       |
| iceberg-v3-mor | every_round     | ✓       |

## 2. 조회 지연 (정상상태 p50 평균, 초)

| 시나리오           | compaction      |   spark |
|----------------|-----------------|---------|
| iceberg-v2-cow | none            |   0.149 |
| iceberg-v2-cow | every_10_rounds |   0.147 |
| iceberg-v2-cow | every_round     |   0.122 |
| iceberg-v2-mor | none            |   0.2   |
| iceberg-v2-mor | every_10_rounds |   0.157 |
| iceberg-v2-mor | every_round     |   0.115 |
| iceberg-v3-cow | none            |   0.148 |
| iceberg-v3-cow | every_10_rounds |   0.152 |
| iceberg-v3-cow | every_round     |   0.132 |
| iceberg-v3-mor | none            |   0.19  |
| iceberg-v3-mor | every_10_rounds |   0.154 |
| iceberg-v3-mor | every_round     |   0.112 |

## 3. 신선도 write→read (커밋→조회가능 지연 평균, 초)

| 시나리오           | compaction      |   spark |
|----------------|-----------------|---------|
| iceberg-v2-cow | none            |   0.235 |
| iceberg-v2-cow | every_10_rounds |   0.196 |
| iceberg-v2-cow | every_round     |   0.159 |
| iceberg-v2-mor | none            |   0.201 |
| iceberg-v2-mor | every_10_rounds |   0.16  |
| iceberg-v2-mor | every_round     |   0.155 |
| iceberg-v3-cow | none            |   0.202 |
| iceberg-v3-cow | every_10_rounds |   0.201 |
| iceberg-v3-cow | every_round     |   0.175 |
| iceberg-v3-mor | none            |   0.202 |
| iceberg-v3-mor | every_10_rounds |   0.173 |
| iceberg-v3-mor | every_round     |   0.152 |

## 4. 적재 · compaction · maintain(스냅샷 expire+orphan) 비용 (초)

| 시나리오           | compaction      |   적재 평균(s) | compaction 평균(s)   |   compaction 총합(s) | maintain 평균(s)   |   maintain 총합(s) |
|----------------|-----------------|------------|--------------------|--------------------|------------------|------------------|
| iceberg-v2-cow | none            |      6.92  | —                  |                0   | —                |              0   |
| iceberg-v2-cow | every_10_rounds |      6.234 | 7.384              |               36.9 | 1.230            |              6.2 |
| iceberg-v2-cow | every_round     |      6.427 | 6.338              |              316.9 | 0.777            |             38.8 |
| iceberg-v2-mor | none            |      3.135 | —                  |                0   | —                |              0   |
| iceberg-v2-mor | every_10_rounds |      2.078 | 7.679              |               38.4 | 1.226            |              6.1 |
| iceberg-v2-mor | every_round     |      2.1   | 6.597              |              329.8 | 0.737            |             36.9 |
| iceberg-v3-cow | none            |      6.677 | —                  |                0   | —                |              0   |
| iceberg-v3-cow | every_10_rounds |      6.869 | 8.167              |               40.8 | 1.319            |              6.6 |
| iceberg-v3-cow | every_round     |      8.597 | 7.469              |              373.4 | 0.797            |             39.8 |
| iceberg-v3-mor | none            |      2.031 | —                  |                0   | —                |              0   |
| iceberg-v3-mor | every_10_rounds |      1.911 | 7.892              |               39.5 | 1.307            |              6.5 |
| iceberg-v3-mor | every_round     |      2.351 | 7.159              |              358   | 0.809            |             40.4 |

## 5. compaction 정책별 방식 비교 (각 정책 하에서 v2/v3 × COW/MOR)

> CV(변동계수)는 라운드 간 변동성. freshness 는 단일 콜드 측정이라 CV 가 query 보다 큼(정상).

### compaction = `none`

| 방식     |   적재(s) |   freshness(s) | fresh CV   |   조회 p50(s) | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.92  |          0.235 | 41%        |       0.149 | 11%     |
| v2-MOR |   3.135 |          0.201 | 16%        |       0.2   | 11%     |
| v3-COW |   6.677 |          0.202 | 23%        |       0.148 | 12%     |
| v3-MOR |   2.031 |          0.202 | 15%        |       0.19  | 11%     |

### compaction = `every_10_rounds`

| 방식     |   적재(s) |   freshness(s) | fresh CV   |   조회 p50(s) | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.234 |          0.196 | 18%        |       0.147 | 13%     |
| v2-MOR |   2.078 |          0.16  | 14%        |       0.157 | 13%     |
| v3-COW |   6.869 |          0.201 | 23%        |       0.152 | 13%     |
| v3-MOR |   1.911 |          0.173 | 27%        |       0.154 | 16%     |

### compaction = `every_round`

| 방식     |   적재(s) |   freshness(s) | fresh CV   |   조회 p50(s) | 조회 CV   |
|--------|---------|----------------|------------|-------------|---------|
| v2-COW |   6.427 |          0.159 | 18%        |       0.122 | 9%      |
| v2-MOR |   2.1   |          0.155 | 18%        |       0.115 | 12%     |
| v3-COW |   8.597 |          0.175 | 29%        |       0.132 | 16%     |
| v3-MOR |   2.351 |          0.152 | 17%        |       0.112 | 11%     |

## 6. 쌍대 비교 (COW vs MOR · v2 vs v3)

### COW vs MOR (v2) — 비율 = v2-MOR ÷ v2-COW (＜1 이면 v2-MOR 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v2-MOR | 비율    |   fresh v2-COW |   fresh v2-MOR | 비율    |   조회 v2-COW |   조회 v2-MOR | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       6.92  |       3.135 | 0.45× |          0.235 |          0.201 | 0.85× |       0.149 |       0.2   | 1.35× |
| every_10_rounds |       6.234 |       2.078 | 0.33× |          0.196 |          0.16  | 0.82× |       0.147 |       0.157 | 1.07× |
| every_round     |       6.427 |       2.1   | 0.33× |          0.159 |          0.155 | 0.97× |       0.122 |       0.115 | 0.94× |

### COW vs MOR (v3) — 비율 = v3-MOR ÷ v3-COW (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v3-COW |   적재 v3-MOR | 비율    |   fresh v3-COW |   fresh v3-MOR | 비율    |   조회 v3-COW |   조회 v3-MOR | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       6.677 |       2.031 | 0.30× |          0.202 |          0.202 | 1.00× |       0.148 |       0.19  | 1.28× |
| every_10_rounds |       6.869 |       1.911 | 0.28× |          0.201 |          0.173 | 0.86× |       0.152 |       0.154 | 1.02× |
| every_round     |       8.597 |       2.351 | 0.27× |          0.175 |          0.152 | 0.87× |       0.132 |       0.112 | 0.85× |

### v2-MOR vs v3-MOR — 비율 = v3-MOR ÷ v2-MOR (＜1 이면 v3-MOR 가 더 낮음)

| compaction      |   적재 v2-MOR |   적재 v3-MOR | 비율    |   fresh v2-MOR |   fresh v3-MOR | 비율    |   조회 v2-MOR |   조회 v3-MOR | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       3.135 |       2.031 | 0.65× |          0.201 |          0.202 | 1.00× |       0.2   |       0.19  | 0.95× |
| every_10_rounds |       2.078 |       1.911 | 0.92× |          0.16  |          0.173 | 1.08× |       0.157 |       0.154 | 0.98× |
| every_round     |       2.1   |       2.351 | 1.12× |          0.155 |          0.152 | 0.98× |       0.115 |       0.112 | 0.98× |

### v2-COW vs v3-COW — 비율 = v3-COW ÷ v2-COW (＜1 이면 v3-COW 가 더 낮음)

| compaction      |   적재 v2-COW |   적재 v3-COW | 비율    |   fresh v2-COW |   fresh v3-COW | 비율    |   조회 v2-COW |   조회 v3-COW | 비율    |
|-----------------|-------------|-------------|-------|----------------|----------------|-------|-------------|-------------|-------|
| none            |       6.92  |       6.677 | 0.96× |          0.235 |          0.202 | 0.86× |       0.149 |       0.148 | 1.00× |
| every_10_rounds |       6.234 |       6.869 | 1.10× |          0.196 |          0.201 | 1.02× |       0.147 |       0.152 | 1.03× |
| every_round     |       6.427 |       8.597 | 1.34× |          0.159 |          0.175 | 1.10× |       0.122 |       0.132 | 1.08× |

## 7. 그래프 (패널=compaction · 선=방식)

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

## 8. 시나리오별 해설

- **iceberg-v2-cow**: 적재 6.4s→11.3s (증가(테이블 성장 비례, COW 특성)). compaction 평균 6.3s. Spark freshness 0.235s.
- **iceberg-v2-mor**: 적재 4.2s→4.3s (평탄(MOR 특성)). compaction 평균 6.6s. Spark freshness 0.201s.
- **iceberg-v3-cow**: 적재 5.7s→15.9s (증가(테이블 성장 비례, COW 특성)). compaction 평균 7.5s. Spark freshness 0.202s.
- **iceberg-v3-mor**: 적재 5.0s→2.3s (평탄(MOR 특성)). compaction 평균 7.2s. Spark freshness 0.202s.

## 9. 종합 해설

- **spark** 최저 조회 지연: `iceberg-v3-mor` / every_round (0.112s)

### 결론 — freshness · write/read 확보에 좋은 구성

- **적재(write) 최저**: `v3-MOR` / every_10_rounds (1.911s) — MOR 계열이 평탄·저비용.
- **조회(read) 최저 p50**: `v3-MOR` / every_round (0.112s).
- **freshness 최저**: `v3-MOR` / every_round (0.152s).
- **균형 종합 권장**: `v2-MOR` / `every_round` (정규화 점수 1.05, 1.0=모든 지표 최저) — 적재·freshness·조회를 동일 가중으로 합산한 최적. 실시간·쓰기빈번(적재→조회 지연 최소화) 워크로드 기준.
