/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      version = "~> 4.70"
    }
  }
}

variable "name" {
  description = "The name of the GKE cluster"
  type        = string
  default     = "agones"
}

variable "zone" {
  description = "The Google Cloud Zone to place the GKE cluster"
  type        = string
  default     = "us-west1-c"
}

variable "project" {
  description = "The Google Cloud project name"
  type        = string
}

variable "machine_type" {
  description = "The GCE machine type to use for the nodes"
  type        = string
  default     = "e2-standard-4"
}

variable "node_count" {
  default     = "4"
  description = "This is the number of gameserver nodes. The Agones module will automatically create an additional two node pools with 1 node each for 'agones-system' and 'agones-metrics'"
  type        = number
}

// Create a GKE cluster with the appropriate structure
module "agones_cluster" {
  source = "git::https://github.com/googleforgames/agones.git//install/terraform/modules/gke/?ref=release-1.40.0"

  cluster = {
    "name"             = var.name
    "zone"             = var.zone
    "initialNodeCount" = var.node_count
    "project"          = var.project
  }
}

// Install Agones via Helm
module "helm_agones" {
  source = "git::https://github.com/googleforgames/agones.git//install/terraform/modules/helm3/?ref=release-1.40.0"

  agones_version         = "1.40.0"
  values_file            = "./helm.yaml"
  chart                  = "agones"
  host                   = module.agones_cluster.host
  token                  = module.agones_cluster.token
  cluster_ca_certificate = module.agones_cluster.cluster_ca_certificate

}
