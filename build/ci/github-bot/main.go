/*
 * Copyright 2021 Google LLC
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

package main

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"text/template"

	"github.com/GoogleCloudPlatform/cloud-build-notifiers/lib/notifiers"
	log "github.com/golang/glog"
	"github.com/google/go-github/v57/github"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/devtools/cloudbuild/v1"
)

const (
	tokenSecretName = "gh-token"
	owner           = "googleforgames"
	repo            = "quilkin"
	quilkinBotId    = 84364394
)

func main() {
	if err := notifiers.Main(new(githubNotifier)); err != nil {
		log.Fatalf("fatal error: %v", err)
	}
}

type githubNotifier struct {
	client          *github.Client
	successTemplate *template.Template
	failureTemplate *template.Template
}

func (g *githubNotifier) SetUp(ctx context.Context, config *notifiers.Config, sg notifiers.SecretGetter, resolver notifiers.BindingResolver) error {
	tokenResource, err := notifiers.FindSecretResourceName(config.Spec.Secrets, tokenSecretName)
	if err != nil {
		return fmt.Errorf("failed to find Secret for ref %q: %w", tokenSecretName, err)
	}
	token, err := sg.GetSecret(ctx, tokenResource)
	if err != nil {
		return fmt.Errorf("failed to get token secret: %w", err)
	}

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	tc := oauth2.NewClient(ctx, ts)
	g.client = github.NewClient(tc)

	g.successTemplate, err = template.New("success").Parse(successTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse success template: %w", err)
	}

	g.failureTemplate, err = template.New("failure").Parse(failureTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse failure template: %w", err)
	}

	return nil
}

func (g *githubNotifier) SendNotification(ctx context.Context, build *cloudbuild.Build) error {
	statusName := cloudbuild.Build_Status_name[int32(build.Status)]
	log.Infof("Build Status: %s, Detail: %s", statusName, build.StatusDetail)
	if build.Status == cloudbuild.Build_QUEUED || build.Status == cloudbuild.Build_WORKING {
		log.Info("Skipping notification")
		return nil
	}

	if build.StatusDetail == "" {
		build.StatusDetail = statusName
	}

	// if this isn't a pull request, ignore it.
	prNumber, ok := build.Substitutions["_PR_NUMBER"]
	if !ok {
		log.Errorf("Could not find PR number in build %s", build.Id)
		return nil
	}
	log.Infof("Serving Pull Request #%s", prNumber)

	// if we get weird data, ignore it.
	pr, err := strconv.Atoi(prNumber)
	if err != nil {
		log.Errorf("Could not convert the PR number (%s) to an integer in build %s", prNumber, build.Id)
		return nil
	}

	// if builds fail, exit, as it's our fault, and there is no point in retrying.
	body := new(bytes.Buffer)

	// hack to allow backticks in multiline strings
	build.Substitutions["_DELIM"] = "`"

	if build.Status == cloudbuild.Build_SUCCESS {
		if err := g.successTemplate.Execute(body, build); err != nil {
			log.Errorf("Error executing success template: %v, in build %s", err, build.Id)
			return nil
		}
	} else {
		if err := g.failureTemplate.Execute(body, build); err != nil {
			log.Errorf("Error executing failure template: %v, in build %s", err, build.Id)
			return nil
		}
	}

	if err := g.clearBotComments(ctx, pr); err != nil {
		log.Warningf("Error clearing bot comments: %v for PR: %d", err, pr)
	}

	commentBody := body.String()
	comment := &github.IssueComment{
		Body: &commentBody,
	}
	if _, _, err := g.client.Issues.CreateComment(ctx, owner, repo, pr, comment); err != nil {
		log.Errorf("Error creating a comment: %v on PR %d", err, pr)
		return err
	}

	return nil
}

func (g *githubNotifier) clearBotComments(ctx context.Context, pr int) error {
	comments, _, err := g.client.Issues.ListComments(ctx, owner, repo, pr, &github.IssueListCommentsOptions{})
	if err != nil {
		log.Errorf("Error retrieving comment history: %v on PR with id: %d", err, pr)
		return nil
	}

	for _, comment := range comments {
		if *comment.User.ID == quilkinBotId {
			if _, err := g.client.Issues.DeleteComment(ctx, owner, repo, *comment.ID); err != nil {
				log.Errorf("Error deleting comment: %v on PR with id %d", err, *comment.ID)
			}
		}
	}

	return nil
}

const successTemplate = `
{{ $pr := .Substitutions._PR_NUMBER }}
**Build Succeeded :partying_face:**

_Build Id: {{ .Id }}_

The following development images have been built, and will exist for the next 30 days:
{{ range .Results.Images }}
* [{{ .Name }}](https://{{ .Name }})
{{ end }}

To build this version:
` + "```" + `
git fetch git@github.com:googleforgames/quilkin.git pull/{{ $pr }}/head:pr_{{ $pr }} && git checkout pr_{{ $pr }}
cargo build
` + "```"

const failureTemplate = `
{{ $sha := .Substitutions.SHORT_SHA }}
{{ $delim := .Substitutions._DELIM }}
{{ $repo := .Substitutions._REPOSITORY }}

**Build Failed :sob:**

_Build Id: {{ .Id }}_

Status: {{ .StatusDetail }}

 - [Cloud Build view]({{.LogUrl}})
 - [Cloud Build log download](https://storage.googleapis.com/quilkin-build-logs/log-{{ .Id }}.txt)

To get permission to view the Cloud Build view, join the [quilkin-discuss](https://groups.google.com/forum/#!forum/quilkin-discuss) Google Group.

Filter with the Git Commit {{ $delim }}{{ $sha }}{{ $delim }} within [{{ $repo }}/quilkin](https://{{ $repo }}/quilkin) to see if a development image is available from this build, and can be used for debugging purposes.

Development images are retained for at least 30 days.
`
