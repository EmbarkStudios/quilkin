# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

FROM rust:latest
WORKDIR /app
# How to install quilkin
ARG install_args="--git https://github.com/EmbarkStudios/quilkin quilkin"
RUN apt update && apt install -y iperf3 socat
RUN cargo install $install_args
COPY ./run.sh /app
COPY ./proxy.yaml /etc/quilkin/quilkin.yaml
CMD ["/app/run.sh"]
