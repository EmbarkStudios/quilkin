/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::io::{Error, ErrorKind, Result as IoResult};

/// Embed the git commit hash into the binary
fn embed_commit_hash() -> IoResult<()> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .map_err(|e| Error::new(e.kind(), "failed to run `git`"))?;

    if !output.status.success() {
        return Err(Error::other(format!(
            "`git` failed with status {}: {}",
            output.status,
            std::str::from_utf8(&output.stderr).unwrap_or("stderr output was not utf-8")
        )));
    }

    let commit = String::from_utf8(output.stdout)
        .map_err(|_e| Error::new(ErrorKind::InvalidData, "stdout was not utf-8"))?;

    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", commit.trim());

    Ok(())
}

// This build script is used to generate the rust source files that
// we need for XDS GRPC communication.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We could use an env var etc to make this fatal if needed
    if let Err(err) = embed_commit_hash() {
        println!("cargo:warning={err}");
    }

    Ok(())
}
