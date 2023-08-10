use crate::ClientActor;
use borsh::BorshSerialize;
use near_chain::chain::{
    do_apply_chunks, ApplyStatePartsRequest, ApplyStatePartsResponse, BlockCatchUpRequest,
    BlockCatchUpResponse,
};
use near_chain::resharding::StateSplitRequest;
use near_chain::Chain;
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_primitives::state_part::PartId;
use near_primitives::syncing::StatePartKey;
use near_primitives::types::ShardId;
use near_store::DBCol;

pub(crate) struct SyncJobsActor {
    pub(crate) client_addr: actix::Addr<ClientActor>,
}

pub(crate) fn create_sync_job_scheduler<M>(address: actix::Addr<SyncJobsActor>) -> Box<dyn Fn(M)>
where
    M: actix::Message + Send + 'static,
    M::Result: Send,
    SyncJobsActor: actix::Handler<WithSpanContext<M>>,
{
    Box::new(move |msg: M| {
        if let Err(err) = address.try_send(msg.with_span_context()) {
            match err {
                actix::dev::SendError::Full(request) => {
                    address.do_send(request);
                }
                actix::dev::SendError::Closed(_) => {
                    tracing::error!("Can't send message to SyncJobsActor, mailbox is closed");
                }
            }
        }
    })
}

impl SyncJobsActor {
    pub(crate) const MAILBOX_CAPACITY: usize = 100;

    fn apply_parts(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<(), near_chain_primitives::error::Error> {
        let shard_id = msg.shard_uid.shard_id as ShardId;
        let _span =
            tracing::debug_span!(target: "sync_jobs_actor", ?shard_id, "apply_parts").entered();
        tracing::debug!(target: "sync_jobs_actor", ?shard_id, "Applying all state parts for shard");
        let store = msg.runtime_adapter.store();

        for part_id in 0..msg.num_parts {
            let key = StatePartKey(msg.sync_hash, shard_id, part_id).try_to_vec()?;
            let part = store.get(DBCol::StateParts, &key)?.unwrap();

            if let Err(err) = msg.runtime_adapter.apply_state_part(
                shard_id,
                &msg.state_root,
                PartId::new(part_id, msg.num_parts),
                &part,
                &msg.epoch_id,
            ) {
                tracing::error!(target: "sync_jobs_actor", ?err, ?part_id, shard_id, "Failed to apply state part");
                return Err(err);
            }
        }

        Ok(())
    }

    /// Clears flat storage before applying state parts.
    fn clear_flat_state(
        &mut self,
        msg: &ApplyStatePartsRequest,
    ) -> Result<(), near_chain_primitives::error::Error> {
        let _span = tracing::debug_span!(target: "sync_jobs_actor", "clear_flat_state").entered();
        if let Some(flat_storage_manager) = msg.runtime_adapter.get_flat_storage_manager() {
            flat_storage_manager.remove_flat_storage_for_shard(msg.shard_uid)?
        }
        Ok(())
    }
}

impl actix::Actor for SyncJobsActor {
    type Context = actix::Context<Self>;
}

impl actix::Handler<WithSpanContext<ApplyStatePartsRequest>> for SyncJobsActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<ApplyStatePartsRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync_jobs_actor", msg);
        let shard_id = msg.shard_uid.shard_id as ShardId;
        let sync_hash = msg.sync_hash;
        if let Err(err) = self.clear_flat_state(&msg) {
            self.client_addr.do_send(
                ApplyStatePartsResponse { apply_result: Err(err), shard_id, sync_hash }
                    .with_span_context(),
            );
            return;
        }

        let apply_result = self.apply_parts(&msg);
        self.client_addr.do_send(
            ApplyStatePartsResponse { apply_result, shard_id, sync_hash }.with_span_context(),
        );
    }
}

impl actix::Handler<WithSpanContext<BlockCatchUpRequest>> for SyncJobsActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<BlockCatchUpRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync_jobs_actor", msg);
        let results = do_apply_chunks(msg.block_hash, msg.block_height, msg.work);

        self.client_addr.do_send(
            BlockCatchUpResponse { sync_hash: msg.sync_hash, block_hash: msg.block_hash, results }
                .with_span_context(),
        );
    }
}

impl actix::Handler<WithSpanContext<StateSplitRequest>> for SyncJobsActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: WithSpanContext<StateSplitRequest>,
        _: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "sync_jobs_actor", msg);
        let response = Chain::build_state_for_split_shards(msg);
        self.client_addr.do_send(response.with_span_context());
    }
}
