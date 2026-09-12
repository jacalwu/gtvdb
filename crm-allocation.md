建表語句（gtvdb Columnar + Graph + AsOfTime）
1.1 貸款暴露表（Loan Exposure）
sql
CREATE TABLE loan_exposure (
    loan_id        UINT64,
    ead            FLOAT64,
    pd             FLOAT64,
    lgd            FLOAT64,
    ccr_ead        FLOAT64,
    pv             FLOAT64,
    rating         STRING,
    scenario_id    STRING,
    valid_from     INT64,
    valid_to       INT64
);
1.2 抵押品表（Collateral）
sql
CREATE TABLE collateral (
    col_id         UINT64,
    value          FLOAT64,
    haircut        FLOAT64,
    fx_haircut     FLOAT64,
    maturity_mm    FLOAT64,
    type           STRING,
    valid_from     INT64,
    valid_to       INT64
);
1.3 擔保人表（Guarantee）
sql
CREATE TABLE guarantee (
    guarantor_id   UINT64,
    amount         FLOAT64,
    rating         STRING,
    valid_from     INT64,
    valid_to       INT64
);
1.4 抵押品 → 貸款 Graph（Collateral Edges）
sql
CREATE TABLE collateral_edges (
    col_id          UINT64,
    loan_id         UINT64,
    ratio           FLOAT64,
    allocation_mode STRING,   -- 'specified' / 'optimizable'
    valid_from      INT64,
    valid_to        INT64
);
1.5 擔保人 → 貸款 Graph（Guarantee Edges）
sql
CREATE TABLE guarantee_edges (
    guarantor_id    UINT64,
    loan_id         UINT64,
    amount          FLOAT64,
    allocation_mode STRING,   -- 'specified' / 'optimizable'
    valid_from      INT64,
    valid_to        INT64
);
2. Python Script：首次生成 Testing Data（可直接跑）
python
import csv
import random
import time

# helper
now = int(time.time() * 1_000_000_000)

# 1. loan_exposure.csv
with open("loan_exposure.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["loan_id", "ead", "pd", "lgd", "ccr_ead", "pv", "rating", "scenario_id", "valid_from", "valid_to"])
    for i in range(1, 101):
        w.writerow([
            i,
            random.uniform(5e6, 20e6),
            random.uniform(0.01, 0.05),
            random.uniform(0.3, 0.6),
            random.uniform(5e6, 20e6),
            random.uniform(5e6, 20e6),
            random.choice(["A", "BBB", "BB"]),
            "BASE",
            now - 10_000,
            now + 10_000
        ])

# 2. collateral.csv
with open("collateral.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["col_id", "value", "haircut", "fx_haircut", "maturity_mm", "type", "valid_from", "valid_to"])
    for i in range(1, 21):
        w.writerow([
            i,
            random.uniform(10e6, 50e6),
            random.uniform(0.05, 0.15),
            random.uniform(0.00, 0.05),
            random.uniform(0.00, 0.05),
            random.choice(["CASH", "BOND", "EQUITY"]),
            now - 10_000,
            now + 10_000
        ])

# 3. guarantee.csv
with open("guarantee.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["guarantor_id", "amount", "rating", "valid_from", "valid_to"])
    for i in range(1, 11):
        w.writerow([
            i,
            random.uniform(20e6, 80e6),
            random.choice(["AAA", "AA", "A"]),
            now - 10_000,
            now + 10_000
        ])

# 4. collateral_edges.csv
with open("collateral_edges.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["col_id", "loan_id", "ratio", "allocation_mode", "valid_from", "valid_to"])
    for col in range(1, 21):
        for loan in random.sample(range(1, 101), 10):
            w.writerow([
                col,
                loan,
                random.uniform(0.05, 0.3),
                random.choice(["specified", "optimizable"]),
                now - 10_000,
                now + 10_000
            ])

# 5. guarantee_edges.csv
with open("guarantee_edges.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["guarantor_id", "loan_id", "amount", "allocation_mode", "valid_from", "valid_to"])
    for g in range(1, 11):
        for loan in random.sample(range(1, 101), 15):
            w.writerow([
                g,
                loan,
                random.uniform(1e6, 10e6),
                random.choice(["specified", "optimizable"]),
                now - 10_000,
                now + 10_000
            ])
3. Load CSV 語句（gtvdb）
sql
LOAD CSV 'loan_exposure.csv' INTO loan_exposure;
LOAD CSV 'collateral.csv' INTO collateral;
LOAD CSV 'guarantee.csv' INTO guarantee;
LOAD CSV 'collateral_edges.csv' INTO collateral_edges;
LOAD CSV 'guarantee_edges.csv' INTO guarantee_edges;
4. CRM Allocation 的要求（HKMA + 實務 + gtvdb）
4.1 HKMA 原文要求（引用）
“Credit risk mitigation (CRM) techniques may be used to reduce credit risk exposures.”
“Banks must meet the eligibility criteria and operational requirements for CRM techniques.”
（來源：20241225-15-EN.pdf）

“Grouping approach is acceptable as long as the allocation reflects correctly the reality (including differences in loan maturities/repayment structures and the contractual or legal rights).”
（來源：20241213e1a3.pdf）

4.2 gtvdb CRM allocation 必須滿足的要求
✔ 1. 支援多抵押品、多擔保人、多貸款
一筆貸款可有多抵押品

一筆貸款可有多擔保人

一個抵押品可覆蓋多筆貸款

一個擔保人可覆蓋多筆貸款

✔ 2. 支援「指定配對」
某些抵押品 / 擔保是合約鎖死的：

allocation_mode = 'specified'

必須先分配

不可進入優化範圍

✔ 3. 支援「可優化配對」
剩餘的：

allocation_mode = 'optimizable'

進入 greedy / priority allocator

之後可接 LP solver 做全局最優

✔ 4. 支援 haircut / FX haircut / maturity mismatch
抵押品可認列價值：

𝐶𝑎𝑑𝑗=𝐶×(1−𝐻𝑐−𝐻𝑓𝑥−𝐻𝑚𝑚)✔
 5. 支援擔保覆蓋剩餘暴露
擔保覆蓋：

𝐶𝑅𝑀𝑔=min⁡(𝐺,𝐸−𝐶𝑅𝑀𝑐)

✔ 6. 支援 AsOfTime
所有資料必須：

Code
valid_from <= T < valid_to
✔ 7. 支援 scenario（BASE / STRESS / DOWN）
所有資料必須：

Code
scenario_id = :S
✔ 8. 支援 greedy / priority allocator
可審計

可解釋

可重跑

可高速運行（Rust + Columnar）

✔ 9. 支援 LP solver（未來擴展）
gtvdb 提供資料切片

LP solver 做全局最優

結果寫回 gtvdb

🎯 最後一句話
這份 crm-allocation.md 是你可以直接放進 gtvdb 專案的正式文件：

有 schema

有測試資料生成

有 CSV 載入

有 HKMA 要求

有 CRM allocation 的完整規範

如果你願意，我可以下一步幫你：

寫出完整 greedy allocator 的 Rust KernelPlan

寫出 crm mode 的 README / user menu

寫出 LP solver 的資料輸出格式

寫出 greedy allocator Rust KernelPlan
下面我給你一個 可以直接放進 gtvdb 的 greedy CRM allocator Rust KernelPlan——
它支持：

抵押品 + 擔保人

指定配對（specified）+ 可優化配對（optimizable）

先鎖死指定，再對剩餘做 greedy / priority 分配

完全 Columnar、可審計、可解釋、可重跑

核心設計思路
Phase 1：指定配對（specified edges）

先按合約鎖死的邊分配覆蓋

更新貸款剩餘暴露、抵押品 / 擔保剩餘容量

Phase 2：Greedy / Priority allocator（optimizable edges）

按 CRM source 優先級排序（抵押品 / 擔保人）

按貸款風險排序

逐一分配 min(source_remaining, loan_remaining)

輸出：

每筆貸款的：collateral_cover, guarantee_cover, net_exposure

1. 資料結構定義
rust
pub struct Loan {
    pub id: u32,
    pub exposure: f64,      // 原始 EAD / PV / etc.
    pub remaining: f64,     // 剩餘暴露（動態更新）
    pub priority: f64,      // 風險優先級
}

pub struct Collateral {
    pub id: u32,
    pub capacity: f64,      // haircut 後可用容量
    pub remaining: f64,     // 剩餘容量（動態更新）
    pub priority: f64,      // 覆蓋優先級
}

pub struct Guarantor {
    pub id: u32,
    pub capacity: f64,      // 保證金額
    pub remaining: f64,     // 剩餘容量
    pub priority: f64,      // 覆蓋優先級
}

pub enum AllocationMode {
    Specified,
    Optimizable,
}

pub struct CollateralEdge {
    pub col_id: u32,
    pub loan_id: u32,
    pub ratio: f64,             // 可選，用於 proration / weighting
    pub mode: AllocationMode,
}

pub struct GuaranteeEdge {
    pub guarantor_id: u32,
    pub loan_id: u32,
    pub amount: f64,
    pub mode: AllocationMode,
}
2. 輸出結構
rust
pub struct CrmResult {
    pub collateral_cover: Vec<f64>,  // per loan_id
    pub guarantee_cover: Vec<f64>,   // per loan_id
    pub net_exposure: Vec<f64>,      // per loan_id
}
3. 核心入口：crm_alloc_greedy_kernel
rust
pub fn crm_alloc_greedy_kernel(
    loans: &mut [Loan],
    collaterals: &mut [Collateral],
    guarantors: &mut [Guarantor],
    coll_edges: &[CollateralEdge],
    guar_edges: &[GuaranteeEdge],
) -> CrmResult {
    let n_loans = loans.len();
    let mut collateral_cover = vec![0.0f64; n_loans];
    let mut guarantee_cover = vec![0.0f64; n_loans];

    // Phase 1: 指定配對（specified）
    apply_specified_collateral(
        loans,
        collaterals,
        coll_edges,
        &mut collateral_cover,
    );
    apply_specified_guarantee(
        loans,
        guarantors,
        guar_edges,
        &mut guarantee_cover,
    );

    // Phase 2: greedy / priority allocator（optimizable）
    apply_greedy_collateral(
        loans,
        collaterals,
        coll_edges,
        &mut collateral_cover,
    );
    apply_greedy_guarantee(
        loans,
        guarantors,
        guar_edges,
        &mut guarantee_cover,
    );

    // 計算 net exposure
    let mut net_exposure = vec![0.0f64; n_loans];
    for (i, loan) in loans.iter().enumerate() {
        net_exposure[i] = loan.exposure - collateral_cover[i] - guarantee_cover[i];
    }

    CrmResult {
        collateral_cover,
        guarantee_cover,
        net_exposure,
    }
}
4. Phase 1：指定配對分配
rust
fn apply_specified_collateral(
    loans: &mut [Loan],
    collaterals: &mut [Collateral],
    edges: &[CollateralEdge],
    out_collateral: &mut [f64],
) {
    for edge in edges.iter().filter(|e| matches!(e.mode, AllocationMode::Specified)) {
        let l = edge.loan_id as usize;
        let c = edge.col_id as usize;

        let loan = &mut loans[l];
        let col = &mut collaterals[c];

        if loan.remaining <= 0.0 || col.remaining <= 0.0 {
            continue;
        }

        let cover = col.remaining.min(loan.remaining);
        if cover > 0.0 {
            out_collateral[l] += cover;
            col.remaining -= cover;
            loan.remaining -= cover;
        }
    }
}

fn apply_specified_guarantee(
    loans: &mut [Loan],
    guarantors: &mut [Guarantor],
    edges: &[GuaranteeEdge],
    out_guarantee: &mut [f64],
) {
    for edge in edges.iter().filter(|e| matches!(e.mode, AllocationMode::Specified)) {
        let l = edge.loan_id as usize;
        let g = edge.guarantor_id as usize;

        let loan = &mut loans[l];
        let guar = &mut guarantors[g];

        if loan.remaining <= 0.0 || guar.remaining <= 0.0 {
            continue;
        }

        let cover = guar.remaining.min(loan.remaining);
        if cover > 0.0 {
            out_guarantee[l] += cover;
            guar.remaining -= cover;
            loan.remaining -= cover;
        }
    }
}
5. Phase 2：Greedy / Priority 分配（optimizable）
rust
fn apply_greedy_collateral(
    loans: &mut [Loan],
    collaterals: &mut [Collateral],
    edges: &[CollateralEdge],
    out_collateral: &mut [f64],
) {
    // 1. 抵押品按 priority 排序
    let mut coll_idx: Vec<usize> = (0..collaterals.len()).collect();
    coll_idx.sort_by(|a, b| collaterals[b].priority.partial_cmp(&collaterals[a].priority).unwrap());

    // 2. 貸款按 priority 排序
    let mut loan_idx: Vec<usize> = (0..loans.len()).collect();
    loan_idx.sort_by(|a, b| loans[b].priority.partial_cmp(&loans[a].priority).unwrap());

    // 3. greedy 分配
    for &c in &coll_idx {
        let col = &mut collaterals[c];
        if col.remaining <= 0.0 {
            continue;
        }

        for &l in &loan_idx {
            let loan = &mut loans[l];
            if loan.remaining <= 0.0 {
                continue;
            }

            // 只使用 optimizable 邊
            let has_edge = edges.iter().any(|e|
                matches!(e.mode, AllocationMode::Optimizable)
                    && e.col_id as usize == c
                    && e.loan_id as usize == l
            );
            if !has_edge {
                continue;
            }

            let cover = col.remaining.min(loan.remaining);
            if cover > 0.0 {
                out_collateral[l] += cover;
                col.remaining -= cover;
                loan.remaining -= cover;

                if col.remaining <= 0.0 {
                    break;
                }
            }
        }
    }
}

fn apply_greedy_guarantee(
    loans: &mut [Loan],
    guarantors: &mut [Guarantor],
    edges: &[GuaranteeEdge],
    out_guarantee: &mut [f64],
) {
    let mut guar_idx: Vec<usize> = (0..guarantors.len()).collect();
    guar_idx.sort_by(|a, b| guarantors[b].priority.partial_cmp(&guarantors[a].priority).unwrap());

    let mut loan_idx: Vec<usize> = (0..loans.len()).collect();
    loan_idx.sort_by(|a, b| loans[b].priority.partial_cmp(&loans[a].priority).unwrap());

    for &g in &guar_idx {
        let guar = &mut guarantors[g];
        if guar.remaining <= 0.0 {
            continue;
        }

        for &l in &loan_idx {
            let loan = &mut loans[l];
            if loan.remaining <= 0.0 {
                continue;
            }

            let has_edge = edges.iter().any(|e|
                matches!(e.mode, AllocationMode::Optimizable)
                    && e.guarantor_id as usize == g
                    && e.loan_id as usize == l
            );
            if !has_edge {
                continue;
            }

            let cover = guar.remaining.min(loan.remaining);
            if cover > 0.0 {
                out_guarantee[l] += cover;
                guar.remaining -= cover;
                loan.remaining -= cover;

                if guar.remaining <= 0.0 {
                    break;
                }
            }
        }
    }