/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2026 Simplyblock.
 *
 *   Unit tests for the transfer dispatch path (xfer_replication +
 *   helper_xfer_poller). Written for the fix that removed the one-item-per-
 *   poller-tick serialization: the dispatcher must fill the whole in-flight
 *   window in one pass, and the helper must drain its ready ring.
 *
 *   Reuses lvol_ut.c wholesale (its stub set is what makes lvol.c link);
 *   its main() is renamed away and only the xfer suite is registered here,
 *   so this binary is independent of the state of the legacy suites.
 */
#define main lvol_ut_main_disabled
#include "../lvol.c/lvol_ut.c"
#undef main

static struct spdk_lvol g_xfer_lvol;
static struct spdk_lvol_store g_xfer_lvs;

static struct spdk_lvs_xfer *
make_xfer(int cluster_batch, uint32_t num_clusters, const int *allocated)
{
	struct spdk_lvs_xfer *xfer = calloc(1, sizeof(*xfer));
	SPDK_CU_ASSERT_FATAL(xfer != NULL);

	memset(&g_xfer_lvol, 0, sizeof(g_xfer_lvol));
	memset(&g_xfer_lvs, 0, sizeof(g_xfer_lvs));
	g_xfer_lvol.lvol_store = &g_xfer_lvs;
	snprintf(g_xfer_lvol.name, sizeof(g_xfer_lvol.name), "xfer_ut_lvol");

	xfer->lvol = &g_xfer_lvol;
	xfer->type = XFER_REPLICATE_SNAPSHOT;
	xfer->state = XFER_STATE_TRANSFER_CLUSTERS;
	xfer->final_step = false;
	xfer->cluster_batch = cluster_batch;
	xfer->page_size = 16;
	xfer->page_per_cluster = 4;
	xfer->num_clusters = num_clusters;
	xfer->clusters = calloc(num_clusters, sizeof(uint64_t));
	SPDK_CU_ASSERT_FATAL(xfer->clusters != NULL);
	for (uint32_t i = 0; i < num_clusters; i++) {
		xfer->clusters[i] = allocated[i] ? 0xABCD0000 + i : 0;
	}

	xfer->free_ring = spdk_ring_create(SPDK_RING_TYPE_MP_MC, cluster_batch,
					   SPDK_ENV_NUMA_ID_ANY);
	xfer->ready_ring = spdk_ring_create(SPDK_RING_TYPE_MP_MC, cluster_batch,
					    SPDK_ENV_NUMA_ID_ANY);
	SPDK_CU_ASSERT_FATAL(xfer->free_ring != NULL && xfer->ready_ring != NULL);

	xfer->reqs = calloc(cluster_batch, sizeof(*xfer->reqs));
	SPDK_CU_ASSERT_FATAL(xfer->reqs != NULL);
	for (int i = 0; i < cluster_batch; i++) {
		xfer->reqs[i].payload = calloc(1, xfer->page_size * xfer->page_per_cluster);
		SPDK_CU_ASSERT_FATAL(xfer->reqs[i].payload != NULL);
		xfer->reqs[i].xfer = xfer;
		xfer->reqs[i].type = xfer->type;
	}
	xfer_fill_queue(xfer, cluster_batch);
	xfer->timeout = spdk_get_ticks();
	return xfer;
}

static void
free_xfer(struct spdk_lvs_xfer *xfer)
{
	for (int i = 0; i < xfer->cluster_batch; i++) {
		free(xfer->reqs[i].payload);
	}
	free(xfer->reqs);
	spdk_ring_free(xfer->free_ring);
	spdk_ring_free(xfer->ready_ring);
	free(xfer->clusters);
	free(xfer);
}

static uint32_t
drain_ready(struct spdk_lvs_xfer *xfer, uint64_t *offsets, uint32_t max)
{
	struct spdk_lvs_xfer_req *req;
	uint32_t n = 0;

	while (n < max && spdk_ring_dequeue(xfer->ready_ring, (void **)&req, 1) == 1) {
		if (offsets != NULL) {
			offsets[n] = req->offset;
		}
		n++;
	}
	return n;
}

/* The fix itself: one call must fill the whole window, not one cluster. */
static void
xfer_fills_whole_window(void)
{
	const int alloc[20] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			       1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
	struct spdk_lvs_xfer *xfer = make_xfer(8, 20, alloc);

	int count = xfer_replication(xfer);

	CU_ASSERT(count == 8);
	CU_ASSERT(xfer->outstanding_io == 8);
	CU_ASSERT(xfer->idx == 8);
	CU_ASSERT(xfer->hold_idx == 8);
	CU_ASSERT(drain_ready(xfer, NULL, 16) == 8);

	free_xfer(xfer);
}

/* A window larger than the remaining work stops at the work, not the window. */
static void
xfer_stops_at_last_cluster(void)
{
	const int alloc[5] = {1, 1, 1, 0, 0};
	struct spdk_lvs_xfer *xfer = make_xfer(8, 5, alloc);

	int count = xfer_replication(xfer);

	CU_ASSERT(count == 3);
	CU_ASSERT(xfer->outstanding_io == 3);
	CU_ASSERT(drain_ready(xfer, NULL, 16) == 3);

	free_xfer(xfer);
}

/* Sparse maps: unallocated clusters are skipped, offsets match the map. */
static void
xfer_skips_unallocated_clusters(void)
{
	const int alloc[10] = {1, 0, 0, 1, 0, 1, 0, 0, 0, 1};
	struct spdk_lvs_xfer *xfer = make_xfer(8, 10, alloc);
	uint64_t offsets[8] = {0};

	int count = xfer_replication(xfer);

	CU_ASSERT(count == 4);
	CU_ASSERT(drain_ready(xfer, offsets, 8) == 4);
	CU_ASSERT(offsets[0] == 0 * xfer->page_per_cluster);
	CU_ASSERT(offsets[1] == 3 * xfer->page_per_cluster);
	CU_ASSERT(offsets[2] == 5 * xfer->page_per_cluster);
	CU_ASSERT(offsets[3] == 9 * xfer->page_per_cluster);

	free_xfer(xfer);
}

/* Completions recycle requests: the NEXT pass continues where the map left
 * off, and the batch transition still fires when everything succeeded. */
static void
xfer_second_pass_continues_and_completes(void)
{
	const int alloc[6] = {1, 1, 1, 1, 1, 1};
	struct spdk_lvs_xfer *xfer = make_xfer(4, 6, alloc);
	struct spdk_lvs_xfer_req *req;

	CU_ASSERT(xfer_replication(xfer) == 4);
	CU_ASSERT(xfer->hold_idx == 4);

	/* complete the four in-flight requests the way local/remote completion
	 * does: status DONE, back on the free ring */
	for (int i = 0; i < 4; i++) {
		CU_ASSERT(spdk_ring_dequeue(xfer->ready_ring, (void **)&req, 1) == 1);
		req->status = XFER_REQ_STATUS_DONE;
		xfer->success_cnt++;
		CU_ASSERT(spdk_ring_enqueue(xfer->free_ring, (void **)&req, 1, NULL) == 1);
	}

	CU_ASSERT(xfer_replication(xfer) == 2);
	CU_ASSERT(xfer->idx == 6);
	CU_ASSERT(xfer->hold_idx == 6);

	/* complete the tail, then the state machine must leave the transfer */
	for (int i = 0; i < 2; i++) {
		CU_ASSERT(spdk_ring_dequeue(xfer->ready_ring, (void **)&req, 1) == 1);
		req->status = XFER_REQ_STATUS_DONE;
		xfer->success_cnt++;
		CU_ASSERT(spdk_ring_enqueue(xfer->free_ring, (void **)&req, 1, NULL) == 1);
	}
	xfer_replication(xfer);
	CU_ASSERT(xfer->state == XFER_STATE_DONE);

	free_xfer(xfer);
}

/* The helper must drain its ready ring, not take one request per tick. */
static void
helper_drains_ready_ring(void)
{
	const int alloc[8] = {1, 1, 1, 1, 1, 1, 1, 1};
	struct spdk_lvs_xfer *xfer = make_xfer(8, 8, alloc);
	struct spdk_lvs_poll_group lpg;
	struct remote_lvol_info rmt;

	CU_ASSERT(xfer_replication(xfer) == 8);

	memset(&lpg, 0, sizeof(lpg));
	TAILQ_INIT(&lpg.rmt_lvols);
	memset(&rmt, 0, sizeof(rmt));
	rmt.status = true;
	rmt.type = XFER_REPLICATE_SNAPSHOT;
	rmt.desc = (struct spdk_bdev_desc *)0x1;      /* only null-checked */
	rmt.channel = (struct spdk_io_channel *)0x1;  /* only null-checked */
	rmt.md_channel = (struct spdk_io_channel *)0x1;
	rmt.ready_ring = xfer->ready_ring;
	rmt.free_ring = xfer->free_ring;
	TAILQ_INSERT_TAIL(&lpg.rmt_lvols, &rmt, entry);

	helper_xfer_poller(&lpg);

	CU_ASSERT(rmt.outstanding_io == 8);
	CU_ASSERT(drain_ready(xfer, NULL, 8) == 0);   /* ring fully drained */

	free_xfer(xfer);
}

int
main(int argc, char **argv)
{
	CU_pSuite suite = NULL;
	unsigned int num_failures;

	CU_initialize_registry();

	suite = CU_add_suite("lvol_xfer", NULL, NULL);
	CU_ADD_TEST(suite, xfer_fills_whole_window);
	CU_ADD_TEST(suite, xfer_stops_at_last_cluster);
	CU_ADD_TEST(suite, xfer_skips_unallocated_clusters);
	CU_ADD_TEST(suite, xfer_second_pass_continues_and_completes);
	CU_ADD_TEST(suite, helper_drains_ready_ring);

	allocate_threads(1);
	set_thread(0);

	num_failures = spdk_ut_run_tests(argc, argv, NULL);

	free_threads();
	CU_cleanup_registry();
	return num_failures;
}
