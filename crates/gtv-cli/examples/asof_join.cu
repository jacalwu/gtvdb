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
