package nosql

import (
	"context"
	"errors"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type cosmosClient struct {
	client *azcosmos.Client
}

func NewCosmosClient(endpoint string, key string) (*cosmosClient, error) {
	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		return nil, err
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		return nil, err
	}

	return &cosmosClient{
		client: client,
	}, nil
}

func (a *cosmosClient) CreateDatabase(dbName string) error {
	databaseProperties := azcosmos.DatabaseProperties{ID: dbName}

	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}
	ctx := context.TODO()
	databaseResp, err := a.client.CreateDatabase(ctx, databaseProperties, nil)

	switch {
	case errorIs409(err):
		log.Printf("Database [%s] already exists\n", dbName)
	case err != nil:
		return err
	default:
		log.Printf("Database [%v] created. ActivityId %s\n", dbName, databaseResp.ActivityID)
	}
	return nil
}

func (a *cosmosClient) CreateContainer(dbName, containerName string, partitionKey []string) error {
	databaseClient, err := a.client.NewDatabase(dbName)
	if err != nil {
		return err
	}

	containerProperties := azcosmos.ContainerProperties{
		ID: containerName,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: partitionKey,
		},
	}

	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}

	throughputProperties := azcosmos.NewManualThroughputProperties(400)
	options := &azcosmos.CreateContainerOptions{
		ThroughputProperties: &throughputProperties,
	}
	ctx := context.TODO()
	containerResponse, err := databaseClient.CreateContainer(ctx, containerProperties, options)

	switch {
	case errorIs409(err):
		log.Printf("Container [%s] already exists\n", containerName)
	case err != nil:
		return err
	default:
		log.Printf("Container [%s] created. ActivityId %s\n", containerName, containerResponse.ActivityID)
	}
	return nil
}
