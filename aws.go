package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

func SendEmail(ctx context.Context, req EmailRequest, redisSvc *RedisService) error {
	// 1. Check limit first
	ok, _, err := redisSvc.CanSend(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("Monthly email limit reached!")
	}

	// 2. Send email via SES
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

	// 3. Increment ONLY after success
	_, err = redisSvc.Increment(ctx)
	if err != nil {
		// optional: log but don't fail email
		fmt.Println("WARN Redis increment failed: ", err)
	}

	return nil
}

func ptr(s string) *string {
	return &s
}
