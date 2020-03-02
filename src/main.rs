use libc;
use std::{thread, time};
use std::io::prelude::*;
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

use crossbeam_utils;
use parking_lot::{RwLock, Mutex, Condvar};

mod buf4k;
use buf4k::Buf4K;

const NUM_CHUNK: usize = 4;
const BLOCKS_PER_CHUNK: usize = 256;

struct Chunk {
    buf: RwLock<Buf4K>,
    pending_writes: Mutex<u8>,
    cvar: Condvar,
}

impl Chunk {
    fn new(size: usize) -> Self {
        Self{
            buf: RwLock::new(Buf4K::new(size)),
            pending_writes: Mutex::new(0),
            cvar: Condvar::new(),
        }
    }
}

fn main() {
    let bytes_written = AtomicUsize::new(0);

    let mut chunks = Vec::with_capacity(NUM_CHUNK);
    for _ in 0..NUM_CHUNK {
        chunks.push(Chunk::new(BLOCKS_PER_CHUNK));
    }

    let input_path = "/dev/sda";

    let outputs = [
        "/dev/sdb",
        "/dev/sdc",
        "/dev/sdd",
        "/dev/sde",
        "/dev/sdf",
        "/dev/sdg",
        "/dev/sdh",
        "/dev/sdi",
        "/dev/sdj",
        "/dev/sdk",
        "/dev/sdl",
        "/dev/sdm",
        "/dev/sdn",
        "/dev/sdo",
        "/dev/sdp",
    ];

    let finished = AtomicBool::new(false);

    crossbeam_utils::thread::scope(|s| {
        for output_path in &outputs {
            let chunks = &chunks;
            s.spawn(move |_| {
                let mut output = OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_DIRECT | libc::O_SYNC)
                    .open(output_path)
                    .expect("Can't open");

                for chunk in chunks.iter().cycle() {
                    let mut pending_writes = chunk.pending_writes.lock();
                    if *pending_writes == 0 {
                        chunk.cvar.wait(&mut pending_writes);
                    }

                    *pending_writes -= 1;
                    let last_write = *pending_writes == 0;

                    let buf = chunk.buf.read();
                    drop(pending_writes);

                    output.write_all(&buf).unwrap();

                    if last_write {
                        chunk.cvar.notify_one();
                    }
                }
            });
        }

        s.spawn(|_| {
            while !finished.load(Ordering::Relaxed) {
                thread::sleep(time::Duration::new(10, 0));
                let bytes = bytes_written.swap(0, Ordering::Relaxed);
                println!("Speed: {:.2}MB/s", bytes as f64 / 10.0 / 1024.0 / 1024.0);
            }
        });

        let mut input = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
            .open(input_path)
            .expect("Can't open");

        for chunk in chunks.iter().cycle() {
            let mut pending_writes = chunk.pending_writes.lock();
            if *pending_writes > 0 {
                chunk.cvar.wait(&mut pending_writes);
            }

            let mut buf = chunk.buf.write();
            input.read_exact(&mut buf).unwrap();

            bytes_written.fetch_add(buf.len(), Ordering::Relaxed);

            *pending_writes = outputs.len() as u8;
            chunk.cvar.notify_all();
        }

        finished.store(true, Ordering::Relaxed);
    }).unwrap();
}
