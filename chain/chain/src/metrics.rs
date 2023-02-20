use near_o11y::metrics::{
    exponential_buckets, try_create_histogram, try_create_histogram_vec, try_create_int_counter,
    try_create_int_gauge, try_create_int_gauge_vec, Histogram, HistogramVec, IntCounter, IntGauge,
    IntGaugeVec,
};
use once_cell::sync::Lazy;

pub static BLOCK_PROCESSING_ATTEMPTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_block_processing_attempts_total",
        "Total number of block processing attempts. The most common reason for aborting block processing is missing chunks",
    )
    .unwrap()
});
pub static BLOCK_PROCESSED_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter("near_block_processed_total", "Total number of blocks processed")
        .unwrap()
});
pub static BLOCK_PROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_processing_time", "Time taken to process blocks successfully, from when a block is ready to be processed till when the processing is finished. Measures only the time taken by the successful attempts of block processing")
        .unwrap()
});
pub static APPLYING_CHUNKS_TIME: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_applying_chunks_time",
        "Time taken to apply chunks per shard",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static BLOCK_PREPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_preprocessing_time", "Time taken to preprocess blocks, only include the time when the preprocessing is successful")
        .unwrap()
});
pub static BLOCK_POSTPROCESSING_TIME: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_postprocessing_time", "Time taken to postprocess blocks")
        .unwrap()
});
pub static BLOCK_HEIGHT_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_height_head", "Height of the current head of the blockchain")
        .unwrap()
});
pub static BLOCK_ORDINAL_HEAD: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_block_ordinal_head", "Ordinal of the current head of the blockchain")
        .unwrap()
});
pub static VALIDATOR_AMOUNT_STAKED: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validators_stake_total",
        "The total stake of all active validators during the last block",
    )
    .unwrap()
});
pub static VALIDATOR_ACTIVE_TOTAL: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_validator_active_total",
        "The total number of validators active after last block",
    )
    .unwrap()
});
pub static NUM_ORPHANS: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_num_orphans", "Number of orphan blocks.").unwrap());
pub static HEADER_HEAD_HEIGHT: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_header_head_height", "Height of the header head").unwrap()
});
pub static BOOT_TIME_SECONDS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge(
        "near_boot_time_seconds",
        "Unix timestamp in seconds of the moment the client was started",
    )
    .unwrap()
});
pub static TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_tail_height", "Height of tail").unwrap());
pub static CHUNK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_chunk_tail_height", "Height of chunk tail").unwrap());
pub static FORK_TAIL_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_fork_tail_height", "Height of fork tail").unwrap());
pub static GC_STOP_HEIGHT: Lazy<IntGauge> =
    Lazy::new(|| try_create_int_gauge("near_gc_stop_height", "Target height of gc").unwrap());
pub static CHUNK_RECEIVED_DELAY: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_chunk_receive_delay_seconds",
        "Delay between requesting and receiving a chunk.",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static BLOCK_ORPHANED_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram("near_block_orphaned_delay", "How long blocks stay in the orphan pool")
        .unwrap()
});
pub static BLOCK_MISSING_CHUNKS_DELAY: Lazy<Histogram> = Lazy::new(|| {
    try_create_histogram(
        "near_block_missing_chunks_delay",
        "How long blocks stay in the missing chunks pool",
    )
    .unwrap()
});
pub static STATE_PART_ELAPSED: Lazy<HistogramVec> = Lazy::new(|| {
    try_create_histogram_vec(
        "near_state_part_elapsed_sec",
        "Time needed to create a state part",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});
pub static NUM_INVALID_BLOCKS: Lazy<IntGauge> = Lazy::new(|| {
    try_create_int_gauge("near_num_invalid_blocks", "Number of invalid blocks").unwrap()
});
pub static GET_REF_SUM: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_sum", "near_get_ref_sum", &["shard_id"]).unwrap()
});
pub static GET_REF_AVG: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_avg", "near_get_ref_avg", &["shard_id"]).unwrap()
});
pub static GET_REF_CNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_cnt", "near_get_ref_cnt", &["shard_id"]).unwrap()
});
pub static GET_REF_NUM_BLOCKS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_blocks", "near_get_ref_blocks", &["shard_id"]).unwrap()
});
pub static GET_REF_GET_BLOCKS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_get_blocks", "near_get_ref_get_blocks", &["shard_id"])
        .unwrap()
});
pub static GET_REF_GET_DELTAS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_get_deltas", "near_get_ref_get_deltas", &["shard_id"])
        .unwrap()
});
pub static GET_REF_CACHED: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_cached", "near_get_ref_cached", &["shard_id"]).unwrap()
});
pub static GET_REF_DISK: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec("near_get_ref_disk", "near_get_ref_disk", &["shard_id"]).unwrap()
});
pub static GET_REF_DELTA_CACHE_HITS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_get_ref_delta_cache_hits",
        "near_get_ref_delta_cache_hits",
        &["shard_id"],
    )
    .unwrap()
});
pub static GET_REF_DELTA_CACHE_MISSES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_get_ref_delta_cache_misses",
        "near_get_ref_delta_cache_misses",
        &["shard_id"],
    )
    .unwrap()
});
pub static GET_REF_DELTA_DISK_HITS: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_get_ref_delta_disk_hits",
        "near_get_ref_delta_disk_hits",
        &["shard_id"],
    )
        .unwrap()
});
pub static GET_REF_DELTA_DISK_MISSES: Lazy<IntGaugeVec> = Lazy::new(|| {
    try_create_int_gauge_vec(
        "near_get_ref_delta_disk_misses",
        "near_get_ref_delta_disk_misses",
        &["shard_id"],
    )
        .unwrap()
});
