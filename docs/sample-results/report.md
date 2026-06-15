# table-bench-mark 결과 리포트 (compaction × 조회엔진)

결과 디렉터리: `bench-20260615-002125`

적재=**Spark**(Iceberg), 조회=**StarRocks**/**Spark**(같은 테이블을 두 엔진이 조회). compaction은 Spark `rewrite_data_files`(deletion vector/delete 적용). 랜덤 생성은 사전 staging으로 측정 제외, Polaris 캐시 비활성.

## 1. 호환성 매트릭스 (✓ 정상 / △ 부분 / ✗ 불가 / - 없음)

| 시나리오           | compaction     | spark   | starrocks-4.1.1   |
|----------------|----------------|---------|-------------------|
| iceberg-v2-cow | none           | ✓       | ✓                 |
| iceberg-v2-cow | every_round    | ✓       | ✓                 |
| iceberg-v2-cow | every_2_rounds | ✓       | ✓                 |
| iceberg-v2-mor | none           | ✓       | ✓                 |
| iceberg-v2-mor | every_round    | ✓       | ✓                 |
| iceberg-v2-mor | every_2_rounds | ✓       | ✓                 |
| iceberg-v3-cow | none           | ✓       | ✓                 |
| iceberg-v3-cow | every_round    | ✓       | ✓                 |
| iceberg-v3-cow | every_2_rounds | ✓       | ✓                 |
| iceberg-v3-mor | none           | ✓       | ✗                 |
| iceberg-v3-mor | every_round    | ✓       | ✓                 |
| iceberg-v3-mor | every_2_rounds | ✓       | △                 |

## 2. 조회 지연 (p50 평균, 초)

| 시나리오           | compaction     |   spark | starrocks-4.1.1   |
|----------------|----------------|---------|-------------------|
| iceberg-v2-cow | none           |   0.231 | 0.091             |
| iceberg-v2-cow | every_round    |   0.135 | 0.080             |
| iceberg-v2-cow | every_2_rounds |   0.171 | 0.084             |
| iceberg-v2-mor | none           |   0.17  | 0.079             |
| iceberg-v2-mor | every_round    |   0.149 | 0.076             |
| iceberg-v2-mor | every_2_rounds |   0.146 | 0.070             |
| iceberg-v3-cow | none           |   0.162 | 0.075             |
| iceberg-v3-cow | every_round    |   0.127 | 0.061             |
| iceberg-v3-cow | every_2_rounds |   0.145 | 0.070             |
| iceberg-v3-mor | none           |   0.157 | —                 |
| iceberg-v3-mor | every_round    |   0.127 | 0.062             |
| iceberg-v3-mor | every_2_rounds |   0.127 | 0.052             |

## 3. 적재 · compaction 비용 (초)

| 시나리오           | compaction     |   적재 평균(s) | compaction 평균(s)   |   compaction 총합(s) |
|----------------|----------------|------------|--------------------|--------------------|
| iceberg-v2-cow | none           |      9.062 | —                  |                0   |
| iceberg-v2-cow | every_round    |      8.01  | 9.552              |               95.5 |
| iceberg-v2-cow | every_2_rounds |      8.623 | 11.260             |               56.3 |
| iceberg-v2-mor | none           |      4.674 | —                  |                0   |
| iceberg-v2-mor | every_round    |      4.04  | 9.918              |               99.2 |
| iceberg-v2-mor | every_2_rounds |      3.825 | 10.251             |               51.3 |
| iceberg-v3-cow | none           |      8.373 | —                  |                0   |
| iceberg-v3-cow | every_round    |      8.244 | 9.621              |               96.2 |
| iceberg-v3-cow | every_2_rounds |      8.837 | 10.482             |               52.4 |
| iceberg-v3-mor | none           |      4.369 | —                  |                0   |
| iceberg-v3-mor | every_round    |      4.078 | 9.627              |               96.3 |
| iceberg-v3-mor | every_2_rounds |      4.055 | 9.789              |               48.9 |

## 4. 그래프

### 적재 시간 vs 라운드 (compaction 모드별)

![적재 시간 vs 라운드 (compaction 모드별)](fig_load.png)

### 조회 지연 vs 라운드 (엔진/compaction별)

![조회 지연 vs 라운드 (엔진/compaction별)](fig_query.png)

### compaction 시간 vs 라운드

![compaction 시간 vs 라운드](fig_compaction.png)

## 5. 해설

- **v3-MOR × starrocks-4.1.1** 호환성(compaction별): none=✗, every_round=✓, every_2_rounds=△  → compaction으로 deletion vector를 제거하면 StarRocks 조회 가능 여부 확인
- **spark** 최저 조회 지연: `iceberg-v3-mor` / every_round (0.127s)
- **starrocks-4.1.1** 최저 조회 지연: `iceberg-v3-mor` / every_2_rounds (0.052s)
