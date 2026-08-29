// knn_top10.cu — CUDA exact top-10 K-NN over a 512-dim f32 corpus (gtvdb TC4).
//
// memory0copy.md guidance followed:
//   * Operator fusion: squared-L2 distance AND the top-K reduction are fused in
//     one kernel — the N intermediate distances are never materialized; only
//     `num_blocks * K` candidates ever leave the device.
//   * Index-only / resident payload: the (transposed) corpus + query are
//     resident on device, uploaded once at build. A query only launches the
//     kernel and downloads a tiny candidate buffer (~num_blocks*10*8 bytes).
//   * Cache tiling / zero-allocation: each thread's top-K lives in registers,
//     and the block merge uses a small fixed-size shared-memory staging area
//     (no malloc, no dynamic growth).
//
// Coalescing: the corpus is stored *column-major* (`data_t[d*n + i]`). For a
// fixed dimension d, the 32 lanes of a warp read 32 consecutive floats — a fully
// coalesced 128-byte transaction — instead of striding `dim*4` bytes apart as a
// row-major layout would. This is what keeps the 2 GB (1M x 512 x 4 B) scan at
// the memory controller's peak bandwidth rather than ~1/8 of it.
//
// NVRTC-compiled at runtime by the `--features cuda` path (same as asof_join.cu).

#define K 10
#define THREADS 256
#define F_INF 3.402823466e38F  // f32 max, used as the empty-slot sentinel

// Insert (d, id) into an ascending top-K array (best = index 0, worst = K-1).
// Strict `<` keeps the first-seen candidate on ties, mirroring the CPU tie-break
// on this deterministic corpus (which has no exact ties anyway).
__device__ __forceinline__ void insert_topk(float d, int id, float* d_ar, int* i_ar) {
    if (d < d_ar[K - 1]) {
        int j = K - 1;
        while (j > 0 && d_ar[j - 1] > d) {
            d_ar[j] = d_ar[j - 1];
            i_ar[j] = i_ar[j - 1];
            --j;
        }
        d_ar[j] = d;
        i_ar[j] = id;
    }
}

extern "C" __global__ void knn_top10_kernel(
    const float* __restrict__ data_t,   // column-major: data_t[d*n + i]
    const float* __restrict__ query,    // dim floats, broadcast to all threads
    int n,
    int dim,
    int* __restrict__ out_id,           // num_blocks * K (ascending per block)
    float* __restrict__ out_dist)       // num_blocks * K
{
    __shared__ float s_d[THREADS * K];
    __shared__ int   s_i[THREADS * K];

    // Per-thread register-resident top-K.
    float ld[K];
    int   li[K];
    #pragma unroll
    for (int j = 0; j < K; ++j) { ld[j] = F_INF; li[j] = -1; }

    const int stride = gridDim.x * blockDim.x;
    for (int idx = blockIdx.x * blockDim.x + threadIdx.x; idx < n; idx += stride) {
        float acc = 0.0f;
        const float* col = data_t + idx; // vector idx, dimension 0
        #pragma unroll 4
        for (int d = 0; d < dim; ++d) {
            float diff = *col - query[d];
            acc += diff * diff;
            col += n; // strength-reduced stride to the next dimension
        }
        insert_topk(acc, idx, ld, li);
    }

    // Block merge: stage every thread's top-K in shared memory, then let lane 0
    // reduce the whole block and write the block's top-K to global.
    #pragma unroll
    for (int j = 0; j < K; ++j) {
        s_d[threadIdx.x * K + j] = ld[j];
        s_i[threadIdx.x * K + j] = li[j];
    }
    __syncthreads();

    if (threadIdx.x == 0) {
        float bd[K];
        int   bi[K];
        #pragma unroll
        for (int j = 0; j < K; ++j) { bd[j] = F_INF; bi[j] = -1; }
        for (int t = 0; t < THREADS; ++t) {
            #pragma unroll
            for (int j = 0; j < K; ++j) {
                insert_topk(s_d[t * K + j], s_i[t * K + j], bd, bi);
            }
        }
        for (int j = 0; j < K; ++j) {
            out_id[blockIdx.x * K + j] = bi[j];
            out_dist[blockIdx.x * K + j] = bd[j];
        }
    }
}
