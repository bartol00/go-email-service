package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

func SendTestEmail(req EmailRequest) error {
	ctx := context.TODO()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	client := ses.NewFromConfig(cfg)

	from := os.Getenv("SENDER_EMAIL")

	input := &ses.SendEmailInput{
		Source: &from,
		Destination: &types.Destination{
			ToAddresses: []string{req.To},
		},
		Message: &types.Message{
			Subject: &types.Content{
				Data: ptr(req.Subject),
			},
			Body: &types.Body{
				Text: &types.Content{
					Data: ptr(req.Text),
				},
				Html: &types.Content{
					Data: ptr(req.HTML),
				},
			},
		},
	}

	_, err = client.SendEmail(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func ptr(s string) *string {
	return &s
}
