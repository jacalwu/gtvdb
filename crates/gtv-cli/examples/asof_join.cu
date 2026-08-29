// asof_join.cu — per-thread binary-search as-of join kernel (gtvdb TC1).
//
// Each CUDA thread handles one left row: it binary-searches the right table for
// the largest `right_ts <= left_ts`, then applies the tolerance window. Compiled
// at runtime via NVRTC by the `--features cuda` path; falls back to the CPU
// L2-bucket sweep if no GPU is present.
extern "C" __global__ void asof_join_cuda_kernel(
    const long long* __restrict__ left_ts,
    const long long* __restrict__ right_ts,
    const double* __restrict__ right_price,
    const double* __restrict__ right_spread,
    double* __restrict__ out_price,
    double* __restrict__ out_spread,
    int left_len,
    int right_len,
    long long tolerance_ns
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= left_len) return;

    const long long l_ts = left_ts[idx];

    // Binary search: largest r_idx with right_ts[r_idx] <= l_ts (or -1).
    int low = 0;
    int high = right_len - 1;
    int r_idx = -1;
    while (low <= high) {
        int mid = low + ((high - low) >> 1);
        if (right_ts[mid] <= l_ts) {
            r_idx = mid;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }

    if (r_idx >= 0) {
        const long long diff = l_ts - right_ts[r_idx];
        if (diff >= 0 && diff <= tolerance_ns) {
            out_price[idx] = right_price[r_idx];
            out_spread[idx] = right_spread[r_idx];
            return;
        }
    }

    // Quiet NaN (0x7ff8000000000000) marks a miss — matches the CPU path.
    out_price[idx] = __longlong_as_double(0x7ff8000000000000ULL);
    out_spread[idx] = __longlong_as_double(0x7ff8000000000000ULL);
}

// asof_merge_fused_kernel — resident-data two-pointer merge join, fused feature.
//
// Replaces the per-thread binary search (O(L·log R)) with the CPU's O(L) chunked
// sweep: each thread owns `chunk` consecutive left rows and advances a *local*
// right pointer monotonically (both sides are sorted), so the total work is the
// merge's O(L + R) instead of the binary search's O(L·log R). The O(1) time-bucket
// index (`bucket_offsets`) gives each thread its starting right row without a
// log-R search.
//
// Zero-copy fusion: instead of materializing (price, spread), the feature
// `rel_spread = spread / price` is computed inline and a single f64 per row is
// written — 8 MB of output instead of 16 MB, and the query downloads only 8 MB.
//
// All inputs are resident on device (uploaded once at build); a query only
// launches this kernel and downloads the 8 MB output.
extern "C" __global__ void asof_merge_fused_kernel(
    const long long* __restrict__ left_ts,
    const long long* __restrict__ right_ts,
    const double* __restrict__ right_price,
    const double* __restrict__ right_spread,
    const int* __restrict__ bucket_offsets,   // length num_buckets + 1
    double* __restrict__ out_feature,
    int left_len,
    int right_len,
    int num_buckets,
    long long min_r_ts,
    long long bucket_ms,
    long long tolerance_ns,
    int chunk)
{
    long long tid = (long long)blockIdx.x * blockDim.x + threadIdx.x;
    long long start = tid * chunk;
    if (start >= left_len) return;

    // O(1) bucket lookup for this chunk's starting predecessor, matching the CPU
    // path: bucket_offsets[b] = first right row in bucket b; start one before it.
    long long l_start = left_ts[start];
    int b = 0;
    if (l_start >= min_r_ts) {
        long long bll = (l_start - min_r_ts) / bucket_ms;
        b = (int)(bll < (long long)num_buckets ? bll : (long long)num_buckets);
    }
    int r_idx = bucket_offsets[b];
    if (r_idx > 0) r_idx -= 1;

    long long end = start + chunk;
    if (end > left_len) end = left_len;

    const double nan = __longlong_as_double(0x7ff8000000000000ULL);
    for (long long i = start; i < end; ++i) {
        long long l = left_ts[i];
        // Two-pointer: advance the right pointer while the next right ts <= l.
        while (r_idx + 1 < right_len && right_ts[r_idx + 1] <= l) {
            r_idx += 1;
        }
        double val = nan;
        long long diff = l - right_ts[r_idx];
        if (diff >= 0 && diff <= tolerance_ns) {
            // Fused feature — the intermediate (price, spread) is never stored.
            val = right_spread[r_idx] / right_price[r_idx];
        }
        out_feature[i] = val;
    }
}
