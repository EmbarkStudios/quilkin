mod shared;

use divan::Bencher;
use shared::*;

fn main() {
    divan::main();
}

#[inline]
fn counter(psize: usize) -> impl divan::counter::Counter {
    divan::counter::BytesCount::new(psize * NUMBER_OF_PACKETS as usize)
}

#[divan::bench_group(sample_count = 10)]
mod read {
    use super::*;

    #[divan::bench(consts = PACKET_SIZES)]
    fn direct<const N: usize>(b: Bencher<'_, '_>) {
        let (writer, reader) = socket_pair(None, None);
        let (tx, rx) = channel();
        let writer = Writer::<N>::new(writer, reader.local_addr().unwrap(), rx);

        spawn(format!("direct_writer_{N}"), move || {
            loop {
                if !writer.write_all(NUMBER_OF_PACKETS) {
                    break;
                }
            }
        });

        b.counter(counter(N)).bench_local(|| {
            read_to_end::<N>(&reader, &tx, NUMBER_OF_PACKETS);
        });
    }

    #[divan::bench(consts = PACKET_SIZES)]
    fn quilkin<const N: usize>(b: Bencher<'_, '_>) {
        let (writer, reader) = socket_pair(None, None);
        let (tx, rx) = channel();

        let quilkin_loop = QuilkinLoop::spinup(READ_QUILKIN_PORT, reader.local_addr().unwrap());
        let writer = Writer::<N>::new(writer, (Ipv4Addr::LOCALHOST, READ_QUILKIN_PORT).into(), rx);
        let _quilkin_loop = writer.wait_ready(quilkin_loop, &reader);

        spawn(format!("quilkin_writer_{N}"), move || {
            loop {
                if !writer.write_all(NUMBER_OF_PACKETS) {
                    break;
                }
            }
        });

        b.counter(counter(N)).bench_local(|| {
            read_to_end::<N>(&reader, &tx, NUMBER_OF_PACKETS);
        });
    }
}

#[divan::bench_group(sample_count = 10)]
mod write {
    use super::*;

    #[divan::bench(consts = PACKET_SIZES)]
    fn direct<const N: usize>(b: Bencher<'_, '_>) {
        let (writer, reader) = socket_pair(None, None);
        let (tx, rx) = channel();

        let writer = Writer::<N>::new(writer, reader.local_addr().unwrap(), rx);

        let (loop_tx, loop_rx) = mpsc::sync_channel(1);

        spawn(format!("direct_reader_{N}"), move || {
            while let Ok((num, _size)) = loop_rx.recv() {
                read_to_end::<N>(&reader, &tx, num);
            }
        });

        b.counter(counter(N)).bench_local(|| {
            // Signal the read loop to run
            loop_tx.send((NUMBER_OF_PACKETS, N)).unwrap();

            writer.write_all(NUMBER_OF_PACKETS);
        });
    }

    #[divan::bench(consts = PACKET_SIZES)]
    fn quilkin<const N: usize>(b: Bencher<'_, '_>) {
        let (writer, reader) = socket_pair(None, None);
        let (tx, rx) = channel();

        //quilkin::test::enable_log("quilkin=debug");

        let quilkin_loop = QuilkinLoop::spinup(WRITE_QUILKIN_PORT, reader.local_addr().unwrap());
        let writer = Writer::<N>::new(writer, (Ipv4Addr::LOCALHOST, WRITE_QUILKIN_PORT).into(), rx);
        let _quilkin_loop = writer.wait_ready(quilkin_loop, &reader);

        let thread = {
            let (loop_tx, loop_rx) = mpsc::sync_channel(1);

            let thread = spawn(format!("quilkin_reader_{}", N), move || {
                while let Ok((num, _size)) = loop_rx.recv() {
                    read_to_end::<N>(&reader, &tx, num);
                }
            });

            b.counter(counter(N)).bench_local(|| {
                // Signal the read loop to run
                loop_tx.send((NUMBER_OF_PACKETS, N)).unwrap();

                writer.write_all(NUMBER_OF_PACKETS);
            });

            thread
        };

        thread.join().unwrap();
    }
}
