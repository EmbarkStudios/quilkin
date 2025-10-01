/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#![cfg(target_os = "linux")]

use qt::xdp_util;
use quilkin::filters::{self, StaticFilter, r#match as mf};

#[tokio::test]
async fn match_() {
    let server_addr = "10.1.1.100:9001".parse().unwrap();

    let (proc_state, cfg_state) = xdp_util::default_xdp_state(xdp_util::make_config(
        qt::filter_chain!([
            Capture => filters::capture::Config::with_strategy(filters::capture::Suffix {
                size: 3,
                remove: true,
            }),
            Match => mf::Config {
                on_read: Some(mf::DirectionalConfig {
                    metadata_key: "quilkin.dev/capture".into(),
                    branches: vec![
                        mf::Branch {
                            value: b"abc".into(),
                            filter: filters::Concatenate::as_filter_config(
                                filters::concatenate::Config {
                                    on_read: filters::concatenate::Strategy::Append,
                                    on_write: filters::concatenate::Strategy::DoNothing,
                                    bytes: b"xyz".into(),
                                },
                            )
                            .unwrap(),
                        },
                        mf::Branch {
                            value: b"xyz".into(),
                            filter: filters::Concatenate::as_filter_config(
                                filters::concatenate::Config {
                                    on_read: filters::concatenate::Strategy::Append,
                                    on_write: filters::concatenate::Strategy::DoNothing,
                                    bytes: b"abc".into(),
                                },
                            )
                            .unwrap(),
                        },
                    ],
                    fallthrough: mf::Fallthrough(
                        filters::Concatenate::as_filter_config(filters::concatenate::Config {
                            on_read: filters::concatenate::Strategy::Append,
                            on_write: filters::concatenate::Strategy::DoNothing,
                            bytes: b"fallthrough".into(),
                        })
                        .unwrap(),
                    ),
                }),
                on_write: None,
            },
        ]),
        xdp_util::endpoints(&[(server_addr, &[])]),
    ));

    let mut simple_loop = xdp_util::SimpleLoop::new(1, proc_state, cfg_state);

    let client = "9.8.7.6".parse().unwrap();

    assert_eq!(
        std::str::from_utf8(simple_loop.echo(client, 89, b"helloabc").unwrap().payload()),
        std::str::from_utf8(b"helloxyz")
    );
    assert_eq!(
        simple_loop.echo(client, 89, b"helloxyz").unwrap().payload(),
        b"helloabc"
    );
    assert_eq!(
        simple_loop
            .echo(client, 89, b"hellofallthrough")
            .unwrap()
            .payload(),
        b"hellofallthrofallthrough"
    );
    assert_eq!(
        simple_loop.echo(client, 89, b"hellofgh").unwrap().payload(),
        b"hellofallthrough"
    );
}
