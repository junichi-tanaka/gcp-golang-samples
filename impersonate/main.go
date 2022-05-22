package main

import (
	"context"
	"io"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

var (
	impersonateServiceAccount = os.Getenv("IMPERSONATE_SERVICE_ACCOUNT")

	bucketName = os.Getenv("BUCKET_NAME")
	objectName = os.Getenv("OBJECT_NAME")

	scopes = []string{"https://www.googleapis.com/auth/cloud-platform"}
)

type Service struct {
	cli *storage.Client
}

func (s *Service) Copy(ctx context.Context, w io.Writer, bucket, object string) error {
	r, err := s.cli.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := io.Copy(w, r); err != nil {
		return err
	}

	return nil
}

func impersonateTokenSource(ctx context.Context, serviceAccount string, scopes []string) (oauth2.TokenSource, error) {
	// Base credentials sourced from ADC or provided client options.
	return impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
		TargetPrincipal: serviceAccount,
		Scopes:          scopes,
		// Optionally supply delegates.
		Delegates: []string{},
	})
}

func run() error {
	ctx := context.Background()

	// Base credentials sourced from ADC or provided client options.
	ts, err := impersonateTokenSource(ctx, impersonateServiceAccount, scopes)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return err
	}

	s := &Service{cli: client}
	if err := s.Copy(ctx, os.Stdout, bucketName, objectName); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
