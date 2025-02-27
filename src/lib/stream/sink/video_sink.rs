use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use gst::prelude::*;
use tracing::*;

use super::SinkInterface;
use crate::stream::pipeline::runner::PipelineRunner;

#[derive(Debug)]
pub struct VideoSink {
    sink_id: uuid::Uuid,
    pipeline: gst::Pipeline,
    queue: gst::Element,
    proxysink: gst::Element,
    muxer: gst::Element,
    filesink: gst::Element,
    tee_src_pad: Option<gst::Pad>,
    pad_blocker: Arc<Mutex<Option<gst::PadProbeId>>>,
    pipeline_runner: PipelineRunner,
}

impl VideoSink {
    #[instrument(level = "debug")]
    pub fn try_new(sink_id: uuid::Uuid) -> Result<Self> {
        let queue = gst::ElementFactory::make("queue")
            .property_from_str("leaky", "downstream") // 丢弃旧数据
            .property("silent", true)
            .property("flush-on-eos", true)
            .property("max-size-buffers", 0u32) // 禁用缓冲
            .build()?;

        let proxysink = gst::ElementFactory::make("proxysink").build()?;
        let _proxysrc = gst::ElementFactory::make("proxysrc")
            .property("proxysink", &proxysink)
            .build()?;

        // Configure proxysrc's queue, skips if fails
        match _proxysrc.downcast_ref::<gst::Bin>() {
            Some(bin) => {
                let elements = bin.children();
                match elements
                    .iter()
                    .find(|element| element.name().starts_with("queue"))
                {
                    Some(element) => {
                        element.set_property_from_str("leaky", "downstream"); // Throw away any data
                        element.set_property("silent", true);
                        element.set_property("flush-on-eos", true);
                        element.set_property("max-size-buffers", 0u32); // Disable buffers
                    }
                    None => {
                        warn!("Failed to customize proxysrc's queue: Failed to find queue in proxysrc");
                    }
                }
            }
            None => {
                warn!("Failed to customize proxysrc's queue: Failed to downcast element to bin")
            }
        }

        // 选择 muxer (Matroska 格式)
        let muxer = gst::ElementFactory::make("matroskamux").build()?;

        // 文件输出
        let filesink = gst::ElementFactory::make("filesink")
            .property("location", "/home/pi/video/output.mkv") // 改为 .mkv
            .property("sync", false)
            .property("async", false)
            .build()?;

        let pad_blocker: Arc<Mutex<Option<gst::PadProbeId>>> = Default::default();
        let pad_blocker_clone = pad_blocker.clone();
        let queue_src_pad = queue.static_pad("src").expect("No src pad found on Queue");

        // To get data out of the callback, we'll be using this arc mutex
        // let (sender, _) = tokio::sync::broadcast::channel(1);
        // let flat_samples_sender = sender.clone();
        // let mut pending = false;

        // 创建 Pipeline
        let pipeline = gst::Pipeline::builder()
            .name(format!("pipeline-sink-{sink_id}"))
            .build();

        // 添加元素到 Pipeline
        let mut elements = vec![&_proxysrc];
        elements.push(muxer.upcast_ref());
        elements.push(filesink.upcast_ref());
        let elements = &elements;
        if let Err(add_err) = pipeline.add_many(elements) {
            return Err(anyhow!(
                "Failed adding VideoSink's elements to Sink Pipeline: {add_err:?}"
            ));
        }

        // 连接 pipeline
        if let Err(link_err) = gst::Element::link_many(elements) {
            if let Err(remove_err) = pipeline.remove_many(elements) {
                warn!("Failed removing elements from VideoSink Pipeline: {remove_err:?}")
            };
            return Err(anyhow!("Failed linking VideoSink's elements: {link_err:?}"));
        }

        let pipeline_runner = PipelineRunner::try_new(&pipeline, &sink_id, true)?;

        // Start the pipeline in Pause, because we want to wait the snapshot
        if let Err(state_err) = pipeline.set_state(gst::State::Null) {
            return Err(anyhow!(
                "Failed pausing VideoSink's pipeline: {state_err:#?}"
            ));
        }

        // Got a valid frame, block any further frame until next request
        // if let Some(old_blocker) = queue_src_pad
        //     .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, |_pad, _info| {
        //         gst::PadProbeReturn::Ok
        //     })
        //     .and_then(|blocker| pad_blocker_clone.lock().unwrap().replace(blocker))
        // {
        //     queue_src_pad.remove_probe(old_blocker);
        // }

        Ok(Self {
            sink_id,
            pipeline,
            queue,
            proxysink,
            muxer,
            filesink,
            tee_src_pad: Default::default(),
            pad_blocker,
            pipeline_runner,
        })
    }

    #[instrument(level = "debug", skip(self))]
    pub fn start_recording(&self) -> Result<()> {
        //// Play the pipeline if it's not playing yet.
        if self.pipeline.current_state() != gst::State::Playing {
            let _ = self.pipeline.set_state(gst::State::Playing);
        }
        // Unblock the data from entering the ProxySink
        // if let Some(blocker) = self.pad_blocker.lock().unwrap().take() {
        //     self.queue
        //         .static_pad("src")
        //         .expect("No src pad found on Queue")
        //         .remove_probe(blocker);
        // }
        info!("Recording started");
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    pub fn start_recording(&self) -> Result<()> {
        //// Play the pipeline if it's not playing yet.
        if self.pipeline.current_state() != gst::State::Playing {
            let _ = self.pipeline.set_state(gst::State::Playing);
        }
        // Unblock the data from entering the ProxySink
        // if let Some(blocker) = self.pad_blocker.lock().unwrap().take() {
        //     self.queue
        //         .static_pad("src")
        //         .expect("No src pad found on Queue")
        //         .remove_probe(blocker);
        // }
        info!("Recording started");
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    pub fn stop_recording(&self) -> Result<()> {
        //// Play the pipeline if it's not playing yet.
        if self.pipeline.current_state() != gst::State::Null {
            let _ = self.pipeline.set_state(gst::State::Null);
        }
        // Unblock the data from entering the ProxySink
        // if let Some(blocker) = self.pad_blocker.lock().unwrap().take() {
        //     self.queue
        //         .static_pad("src")
        //         .expect("No src pad found on Queue")
        //         .remove_probe(blocker);
        // }
        info!("Recording stopped");
        Ok(())
    }
}

impl SinkInterface for VideoSink {
    #[instrument(level = "debug", skip(self, pipeline))]
    fn link(
        &mut self,
        pipeline: &gst::Pipeline,
        pipeline_id: &uuid::Uuid,
        tee_src_pad: gst::Pad,
    ) -> Result<()> {
        let sink_id = &self.get_id();

        if self.tee_src_pad.is_some() {
            return Err(anyhow!(
                "Tee's src pad from VideoSink {sink_id} already configured"
            ));
        }

        self.tee_src_pad.replace(tee_src_pad);
        let Some(tee_src_pad) = &self.tee_src_pad else {
            unreachable!()
        };

        // Block data flow to prevent any data before set Playing, which would cause an error
        let Some(tee_src_pad_data_blocker) = tee_src_pad
            .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, |_pad, _info| {
                gst::PadProbeReturn::Ok
            })
        else {
            let msg =
                "Failed adding probe to Tee's src pad to block data before going to playing state"
                    .to_string();
            error!(msg);

            if let Some(parent) = tee_src_pad.parent_element() {
                parent.release_request_pad(tee_src_pad)
            }

            return Err(anyhow!(msg));
        };

        // Add the ProxySink element to the source's pipeline
        let elements = &[&self.queue, &self.proxysink];
        if let Err(add_err) = pipeline.add_many(elements) {
            let msg = format!("Failed to add ProxySink to Pipeline {pipeline_id}: {add_err:#?}");

            if let Some(parent) = tee_src_pad.parent_element() {
                parent.release_request_pad(tee_src_pad)
            }

            return Err(anyhow!(msg));
        }

        // Link the queue's src pad to the ProxySink's sink pad
        let queue_src_pad = &self
            .queue
            .static_pad("src")
            .expect("No src pad found on Queue");
        let proxysink_sink_pad = &self
            .proxysink
            .static_pad("sink")
            .expect("No sink pad found on ProxySink");
        if let Err(link_err) = queue_src_pad.link(proxysink_sink_pad) {
            let msg =
                format!("Failed to link Queue's src pad with WebRTCBin's sink pad: {link_err:?}");
            error!(msg);

            if let Some(parent) = tee_src_pad.parent_element() {
                parent.release_request_pad(tee_src_pad)
            }

            if let Err(remove_err) = pipeline.remove_many(elements) {
                error!("Failed to remove elements from pipeline: {remove_err:?}");
            }

            return Err(anyhow!(msg));
        }

        // Link the new Tee's src pad to the ProxySink's sink pad
        let queue_sink_pad = &self
            .queue
            .static_pad("sink")
            .expect("No sink pad found on Queue");
        if let Err(link_err) = tee_src_pad.link(queue_sink_pad) {
            let msg = format!("Failed to link Tee's src pad with Queue's sink pad: {link_err:?}");
            error!(msg);

            if let Err(unlink_err) = queue_src_pad.unlink(proxysink_sink_pad) {
                error!("Failed to unlink Queue's src pad and ProxySink's sink pad: {unlink_err:?}");
            }

            if let Some(parent) = tee_src_pad.parent_element() {
                parent.release_request_pad(tee_src_pad)
            }

            if let Err(remove_err) = pipeline.remove_many(elements) {
                error!("Failed to remove elements from pipeline: {remove_err:?}");
            }

            return Err(anyhow!(msg));
        }

        // Syncronize added and linked elements
        if let Err(sync_err) = pipeline.sync_children_states() {
            let msg = format!("Failed to synchronize children states: {sync_err:?}");
            error!(msg);

            if let Err(unlink_err) = queue_src_pad.unlink(proxysink_sink_pad) {
                error!("Failed to unlink Queue's src pad and ProxySink's sink pad: {unlink_err:?}");
            }

            if let Err(unlink_err) = queue_src_pad.unlink(proxysink_sink_pad) {
                error!("Failed to unlink Queue's src pad and ProxySink's sink pad: {unlink_err:?}");
            }

            if let Some(parent) = tee_src_pad.parent_element() {
                parent.release_request_pad(tee_src_pad)
            }

            if let Err(remove_err) = pipeline.remove_many(elements) {
                error!("Failed to remove elements from pipeline: {remove_err:?}");
            }

            return Err(anyhow!(msg));
        }

        // Unblock data to go through this added Tee src pad
        tee_src_pad.remove_probe(tee_src_pad_data_blocker);

        Ok(())
    }

    #[instrument(level = "debug", skip(self, pipeline))]
    fn unlink(&self, pipeline: &gst::Pipeline, pipeline_id: &uuid::Uuid) -> Result<()> {
        let Some(tee_src_pad) = &self.tee_src_pad else {
            warn!("Tried to unlink Sink from a pipeline without a Tee src pad.");
            return Ok(());
        };

        // Block data flow to prevent any data from holding the Pipeline elements alive
        if tee_src_pad
            .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, |_pad, _info| {
                gst::PadProbeReturn::Ok
            })
            .is_none()
        {
            warn!(
                "Failed adding probe to Tee's src pad to block data before going to playing state"
            );
        }

        // Unlink the Queue element from the source's pipeline Tee's src pad
        let queue_sink_pad = self
            .queue
            .static_pad("sink")
            .expect("No sink pad found on Queue");
        if let Err(unlink_err) = tee_src_pad.unlink(&queue_sink_pad) {
            warn!("Failed unlinking VideoSink's Queue element from Tee's src pad: {unlink_err:?}");
        }
        drop(queue_sink_pad);

        // Release Tee's src pad
        if let Some(parent) = tee_src_pad.parent_element() {
            parent.release_request_pad(tee_src_pad)
        }

        // Remove the Sink's elements from the Source's pipeline
        let elements = &[&self.queue, &self.proxysink];
        if let Err(remove_err) = pipeline.remove_many(elements) {
            warn!("Failed removing VideoSink's elements from pipeline: {remove_err:?}");
        }

        // Set Sink's pipeline to null
        if let Err(state_err) = self.pipeline.set_state(gst::State::Null) {
            warn!("Failed to set Pipeline's state from VideoSink to NULL: {state_err:#?}");
        }

        // Set Queue to null
        if let Err(state_err) = self.queue.set_state(gst::State::Null) {
            warn!("Failed to set Queue's state to NULL: {state_err:#?}");
        }

        // Set ProxySink to null
        if let Err(state_err) = self.proxysink.set_state(gst::State::Null) {
            warn!("Failed to set ProxySink's state to NULL: {state_err:#?}");
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn get_id(&self) -> uuid::Uuid {
        self.sink_id
    }

    #[instrument(level = "trace", skip(self))]
    fn get_sdp(&self) -> Result<gst_sdp::SDPMessage> {
        Err(anyhow!(
            "Not available. Reason: Video Sink doesn't provide endpoints"
        ))
    }

    #[instrument(level = "debug", skip(self))]
    fn start(&self) -> Result<()> {
        // self.pipeline_runner.start()
        Ok(())
    }

    #[instrument(level = "debug", skip(self))]
    fn eos(&self) {
        let pipeline_weak = self.pipeline.downgrade();
        if let Err(error) = std::thread::Builder::new()
            .name("EOS".to_string())
            .spawn(move || {
                let pipeline = pipeline_weak.upgrade().unwrap();
                if let Err(error) = pipeline.post_message(gst::message::Eos::new()) {
                    error!("Failed posting Eos message into Sink bus. Reason: {error:?}");
                }
            })
            .expect("Failed spawning EOS thread")
            .join()
        {
            error!(
                "EOS Thread Panicked with: {:?}",
                error.downcast_ref::<String>()
            );
        }
    }
}
