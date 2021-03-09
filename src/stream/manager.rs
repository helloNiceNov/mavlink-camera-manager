use super::types::*;
use super::{stream_backend, stream_backend::StreamBackend};
use crate::settings;
use crate::video_stream::types::VideoAndStreamInformation;
use log::*;
use simple_error::SimpleError;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Manager {
    pub streams: Vec<(StreamType, VideoAndStreamInformation)>,
}

lazy_static! {
    static ref MANAGER: Arc<Mutex<Manager>> = Arc::new(Mutex::new(Manager::default()));
}

pub fn init() {
    debug!("Starting video stream service.");
}

// Start all streams that are not running
pub fn start() {
    let mut manager = MANAGER.as_ref().lock().unwrap();
    for (stream, _) in &mut manager.streams {
        match stream {
            StreamType::UDP(stream) => {
                stream.start();
            }
        }
    }
}

pub fn streams() -> Vec<StreamStatus> {
    let manager = MANAGER.as_ref().lock().unwrap();
    let status: Vec<StreamStatus> = manager
        .streams
        .iter()
        .map(|(stream, video_and_stream)| StreamStatus {
            running: stream.inner().is_running(),
            video_and_stream: video_and_stream.clone(),
        })
        .collect();

    return status;
}

pub fn add_stream_and_start(
    video_and_stream_information: VideoAndStreamInformation,
) -> Result<(), SimpleError> {
    //TODO: Check if stream can handle caps
    let mut manager = MANAGER.as_ref().lock().unwrap();

    for (our_stream_type, our_video_and_stream_information) in manager.streams.iter() {
        our_video_and_stream_information.conflicts_with(&video_and_stream_information)?
    }

    let mut stream = stream_backend::create_stream(&video_and_stream_information)?;
    stream.mut_inner().start();
    manager.streams.push((stream, video_and_stream_information));

    let video_and_stream_informations = manager
        .streams
        .iter()
        .map(|(stream, video_and_stream_information)| video_and_stream_information.clone())
        .collect();
    settings::manager::set_streams(&video_and_stream_informations);
    return Ok(());
}

pub fn remove_stream(stream_name: &str) -> Result<(), SimpleError> {
    let find_stream = |(stream_type, information): &(StreamType, VideoAndStreamInformation)| {
        information.name == *stream_name
    };

    let mut manager = MANAGER.as_ref().lock().unwrap();
    match manager.streams.iter().position(find_stream) {
        Some(index) => {
            manager.streams.remove(index);
            Ok(())
        }
        None => Err(SimpleError::new(
            "Identification does not match any stream.",
        )),
    }
}

//TODO: rework to use UML definition
// Add a new pipeline string to run
/*
pub fn add(description: &'static str) {
    let mut stream = VideoStreamUdp::default();
    stream.set_pipeline_description(description);
    let mut manager = MANAGER.as_ref().lock().unwrap();
    manager.streams.push(StreamType::UDP(stream));
}
*/
