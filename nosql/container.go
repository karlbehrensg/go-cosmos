package nosql

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type containerClient struct {
	client *azcosmos.ContainerClient
}

func (a *cosmosClient) NewContainerClient(dbName, containerName string) (*containerClient, error) {
	client, err := a.client.NewContainer(dbName, containerName)
	if err != nil {
		return nil, err
	}

	return &containerClient{
		client: client,
	}, nil
}

func (c *containerClient) CreateItem(item interface{}, id string) error {
	// specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(id)

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}

	// setting the item options upon creating ie. consistency level
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
	}

	// this is a helper function that swallows 409 errors
	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}

	ctx := context.TODO()
	itemResponse, err := c.client.CreateItem(ctx, pk, b, &itemOptions)

	switch {
	case errorIs409(err):
		log.Printf("Item with partitionkey value %s already exists\n", pk)
	case err != nil:
		return err
	default:
		log.Printf("Status %d. Item %v created. ActivityId %s. Consuming %v Request Units.\n", itemResponse.RawResponse.StatusCode, pk, itemResponse.ActivityID, itemResponse.RequestCharge)
	}

	return nil
}

func (c *containerClient) ReadItem(partitionKey, id string) (map[string]interface{}, error) {
	// Specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	itemResponse, err := c.client.ReadItem(context.Background(), pk, id, nil)
	if err != nil {
		return nil, err
	}

	var item map[string]interface{}
	if err := json.Unmarshal(itemResponse.Value, &item); err != nil {
		return nil, err
	}

	log.Printf("Status %d. Item %v read. ActivityId %s. Consuming %v Request Units.\n", itemResponse.RawResponse.StatusCode, pk, itemResponse.ActivityID, itemResponse.RequestCharge)

	return item, nil
}
