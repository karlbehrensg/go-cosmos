package main

import (
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/karlbehrensg/go-cosmos/nosql"
)

func main() {
	client, err := nosql.NewCosmosClient(os.Getenv("COSMOS_PATH"), os.Getenv("COSMOS_KEY"))
	if err != nil {
		log.Fatal(err)
	}

	if err := client.CreateDatabase("testdb"); err != nil {
		log.Fatal(err)
	}

	partitionKeys := []string{"/email"}

	if err := client.CreateContainer("testdb", "testcontainer", partitionKeys); err != nil {
		log.Fatal(err)
	}

	containerClient, err := client.NewContainerClient("testdb", "testcontainer")
	if err != nil {
		log.Fatal(err)
	}

	type Item struct {
		ID    string `json:"id"`
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	id := uuid.New().String()

	item := Item{
		ID:    id,
		Email: "john@doe.com",
		Name:  "John Doe",
	}

	if err := containerClient.CreateItem(item, item.Email); err != nil {
		log.Fatal(err)
	}

	_, err = containerClient.ReadItem(item.Email, id)
	if err != nil {
		log.Fatal(err)
	}

	item.Name = "Jane Doe"

	if err := containerClient.ReplaceItem(item, item.Email, id); err != nil {
		log.Fatal(err)
	}

	if err := containerClient.DeleteItem(item.Email, item.ID); err != nil {
		log.Fatal(err)
	}
}
