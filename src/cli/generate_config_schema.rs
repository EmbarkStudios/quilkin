/*
 * Copyright 2022 Google LLC
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

/// Generates JSON schema files for known filters.
#[derive(clap::Args, Clone, Debug)]
pub struct GenerateConfigSchema {
    /// The directory to write configuration files.
    #[clap(short, long, default_value = ".")]
    pub output_directory: std::path::PathBuf,
    /// A list of one or more filter IDs to generate or 'all' to generate all
    /// available filter schemas.
    #[clap(num_args = 1.., default_value = "all")]
    pub filter_ids: Vec<String>,
}

impl GenerateConfigSchema {
    pub fn generate_config_schema(&self) -> crate::Result<()> {
        let set = crate::filters::FilterSet::default();
        type SchemaIterator<'r> = Box<dyn Iterator<Item = (&'static str, schemars::Schema)> + 'r>;

        let schemas = if self.filter_ids.len() == 1 && self.filter_ids[0].to_lowercase() == "all" {
            {
                Box::new(
                    set.iter()
                        .map(|factory| (factory.name(), factory.config_schema())),
                ) as SchemaIterator<'_>
            }
        } else {
            {
                Box::new(self.filter_ids.iter().filter_map(|id| {
                    let item = set.get(id);

                    if item.is_none() {
                        tracing::error!("{id} not found in filter set.");
                    }

                    item.map(|item| (item.name(), item.config_schema()))
                })) as SchemaIterator<'_>
            }
        };

        for (id, schema) in schemas {
            let mut path = self.output_directory.join(id);
            path.set_extension("yaml");

            tracing::info!("Writing {id} schema to {}", path.display());

            std::fs::write(path, serde_yaml::to_string(&schema)?)?;
        }

        Ok(())
    }
}
