package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"net/http"
	"os"
	"time"
)

type options struct {
	ListenAddr string
	DryRun     bool
}

func (o *options) Run() error {
	stopCh := wait.NeverStop

	klog.Infof("Starting...")

	if len(o.ListenAddr) > 0 {
		http.DefaultServeMux.Handle("/metrics", promhttp.Handler())
		go func() {
			klog.Infof("Listening on %s for UI and metrics", o.ListenAddr)
			if err := http.ListenAndServe(o.ListenAddr, nil); err != nil {
				klog.Exitf("Server exited: %v", err)
			}
		}()
	}

	var ok bool
	var databaseHost, databasePort, databaseUserName, databaseUserPassword, databaseAdminPassword, databaseName string

	if databaseHost, ok = os.LookupEnv("MONGODB_HOST"); !ok || len(databaseHost) == 0 {
		klog.Fatal("MONGODB_HOST is not defined")
	}

	if databasePort, ok = os.LookupEnv("MONGODB_PORT"); !ok || len(databasePort) == 0 {
		klog.Fatal("MONGODB_PORT is not defined")
	}

	if databaseUserName, ok = os.LookupEnv("MONGODB_USER"); !ok || len(databaseUserName) == 0 {
		klog.Fatal("MONGODB_USER is not defined")
	}

	if databaseUserPassword, ok = os.LookupEnv("MONGODB_PASSWORD"); !ok || len(databaseUserPassword) == 0 {
		klog.Fatal("MONGODB_PASSWORD is not defined")
	}

	if databaseAdminPassword, ok = os.LookupEnv("MONGODB_ADMIN_PASSWORD"); !ok || len(databaseAdminPassword) == 0 {
		klog.Fatal("MONGODB_ADMIN_PASSWORD is not defined")
	}

	if databaseName, ok = os.LookupEnv("MONGODB_DATABASE"); !ok || len(databaseName) == 0 {
		klog.Fatal("MONGODB_DATABASE is not defined")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// mongodb://[username:password@]host1[:port1][,...hostN[:portN]][/[defaultauthdb][?options]]
	connectString := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", databaseUserName, databaseUserPassword, databaseHost, databasePort, databaseName)
	client, err := mongo.Connect(ctx, mongoOptions.Client().ApplyURI(connectString))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	adminConnectString := fmt.Sprintf("mongodb://admin:%s@%s:%s/admin", databaseAdminPassword, databaseHost, databasePort)
	adminClient, err := mongo.Connect(ctx, mongoOptions.Client().ApplyURI(adminConnectString))
	defer func() {
		if err = adminClient.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	klog.Infof("Checking access to database...")
	err = wait.PollImmediate(15*time.Second, 60*time.Second, func() (done bool, err error) {
		err = client.Ping(ctx, readpref.Primary())
		if err != nil {
			klog.Warningf("Unable to ping database: %v", err)
			return false, nil
		}
		klog.Infof("Ping successful")
		return true, nil
	})
	if err != nil {
		klog.Fatal(err)
	}

	initializeDatabase(adminClient)

	create(client)
	structures(client)
	read(client)
	update(client)
	delete(client)

	go mainProcessLoop(stopCh)

	<-stopCh
	klog.Infof("Exit...")
	return nil
}

func initializeDatabase(client *mongo.Client) {
	klog.Infof("Initializing database...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Println(databases)
}

func create(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quickstartDatabase := client.Database("sampledb")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	podcastResult, err := podcastsCollection.InsertOne(ctx, bson.D{
		{"title", "The Polyglot Developer Podcast"},
		{"author", "Nic Raboy"},
		{"tags", bson.A{"development", "programming", "coding"}},
	})
	if err != nil {
		klog.Fatal(err)
	}

	episodeResult, err := episodesCollection.InsertMany(ctx, []interface{}{
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "GraphQL for API Development"},
			{"description", "Learn about GraphQL from the co-creator of GraphQL, Lee Byron."},
			{"duration", 25},
		},
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "Progressive Web Application Development"},
			{"description", "Learn about PWA development with Tara Manicsic."},
			{"duration", 32},
		},
	})
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Printf("Inserted %v documents into episode collection!\n", len(episodeResult.InsertedIDs))
}

func read(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quickstartDatabase := client.Database("sampledb")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	// ALL
	fmt.Println("Getting episodes via ALL()")
	cursor, err := episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		klog.Fatal(err)
	}
	var episodes []bson.M
	if err = cursor.All(ctx, &episodes); err != nil {
		klog.Fatal(err)
	}
	fmt.Println(episodes)

	// Iterate
	fmt.Println("Iterating over episodes")
	cursor, err = episodesCollection.Find(ctx, bson.M{})
	if err != nil {
		klog.Fatal(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var episode bson.M
		if err = cursor.Decode(&episode); err != nil {
			klog.Fatal(err)
		}
		fmt.Println(episode)
	}

	// FindOne
	fmt.Println("FindOne")
	var podcast bson.M
	if err = podcastsCollection.FindOne(ctx, bson.M{}).Decode(&podcast); err != nil {
		klog.Fatal(err)
	}
	fmt.Println(podcast)

	// Filters
	fmt.Println("Filtering (duration of 25)")
	filterCursor, err := episodesCollection.Find(ctx, bson.M{"duration": 25})
	if err != nil {
		klog.Fatal(err)
	}
	var episodesFiltered []bson.M
	if err = filterCursor.All(ctx, &episodesFiltered); err != nil {
		klog.Fatal(err)
	}
	fmt.Println(episodesFiltered)

	// Sorting
	fmt.Println("Sorting, descending by duration > 24")
	opts := mongoOptions.Find()
	opts.SetSort(bson.D{{"duration", -1}})
	sortCursor, err := episodesCollection.Find(ctx, bson.D{{"duration", bson.D{{"$gt", 24}}}}, opts)
	if err != nil {
		klog.Fatal(err)
	}
	var episodesSorted []bson.M
	if err = sortCursor.All(ctx, &episodesSorted); err != nil {
		klog.Fatal(err)
	}
	fmt.Println(episodesSorted)
}

func update(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quickstartDatabase := client.Database("sampledb")
	podcastsCollection := quickstartDatabase.Collection("podcasts")

	// UpdateOne()
	fmt.Println("Updating by ID (610414778b0a99f9bc7f248b)")
	id, _ := primitive.ObjectIDFromHex("610414778b0a99f9bc7f248b")
	result, err := podcastsCollection.UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.D{
			{"$set", bson.D{{"author", "Nic Raboy"}}},
		},
	)
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Printf("Updated %v Documents!\n", result.ModifiedCount)

	// UpdateMany()
	fmt.Println("Updating by filter")
	result, err = podcastsCollection.UpdateMany(
		ctx,
		bson.M{"title": "The Polyglot Developer Podcast"},
		bson.D{
			{"$set", bson.D{{"author", "Nicolas Raboy"}}},
		},
	)
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Printf("Updated %v Documents!\n", result.ModifiedCount)

	// ReplaceOne()
	fmt.Println("Replacing document by filter")
	result, err = podcastsCollection.ReplaceOne(
		ctx,
		bson.M{"author": "Nic Raboy"},
		bson.M{
			"title":  "The Nic Raboy Show",
			"author": "Nicolas Raboy",
		},
	)
	fmt.Printf("Replaced %v Documents!\n", result.ModifiedCount)
}

func delete(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quickstartDatabase := client.Database("sampledb")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	// DeleteOne
	fmt.Println("Deleting Document by filter")
	result, err := podcastsCollection.DeleteOne(ctx, bson.M{"title": "The Polyglot Developer Podcast"})
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Printf("DeleteOne removed %v document(s)\n", result.DeletedCount)

	// DeleteMany
	fmt.Println("Deleting Multiple Documents by filter")
	result, err = episodesCollection.DeleteMany(ctx, bson.M{"duration": 25})
	if err != nil {
		klog.Fatal(err)
	}
	fmt.Printf("DeleteMany removed %v document(s)\n", result.DeletedCount)

	// Drop
	fmt.Println("Dropping entire collection")
	if err = podcastsCollection.Drop(ctx); err != nil {
		klog.Fatal(err)
	}
}

type Podcast struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Title  string             `bson:"title,omitempty"`
	Author string             `bson:"author,omitempty"`
	Tags   []string           `bson:"tags,omitempty"`
}

type Episode struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Podcast     primitive.ObjectID `bson:"podcast,omitempty"`
	Title       string             `bson:"title,omitempty"`
	Description string             `bson:"description,omitempty"`
	Duration    int32              `bson:"duration,omitempty"`
}

func structures(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	quickstartDatabase := client.Database("sampledb")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	// Reading into GO Types
	fmt.Println("Reading into Go Types")
	var episodes []Episode
	cursor, err := episodesCollection.Find(ctx, bson.M{"duration": bson.D{{"$gt", 25}}})
	if err != nil {
		panic(err)
	}
	if err = cursor.All(ctx, &episodes); err != nil {
		panic(err)
	}
	fmt.Println(episodes)

	// Creating using GO Types
	fmt.Println("Creating using Go Types")
	podcast := Podcast{
		Title:  "The Polyglot Developer",
		Author: "Nic Raboy",
		Tags:   []string{"development", "programming", "coding"},
	}
	insertResult, err := podcastsCollection.InsertOne(ctx, podcast)
	if err != nil {
		panic(err)
	}
	fmt.Println(insertResult.InsertedID)
}

func mainProcessLoop(stopCh <-chan struct{}) {
	// Loop, every 5 minutes, forever...
	wait.Until(func() {
		start := time.Now()
		_, err := processLoop()
		duration := time.Since(start)

		if err != nil {
			klog.Errorf("processLoop failed: %v", err)
			return
		}

		klog.Infof("processLoop finished in: %d ms", duration.Milliseconds())
	}, 5*time.Minute, stopCh)
}

func processLoop() (bool, error) {
	return true, nil
}

func main() {
	original := flag.CommandLine
	klog.InitFlags(original)
	original.Set("alsologtostderr", "true")
	original.Set("v", "2")

	opt := &options{
		ListenAddr: ":8080",
	}

	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, arguments []string) {
			if err := opt.Run(); err != nil {
				klog.Exitf("Run error: %v", err)
			}
		},
	}

	flagset := cmd.Flags()
	flagset.BoolVar(&opt.DryRun, "dry-run", opt.DryRun, "Perform no actions")
	flagset.StringVar(&opt.ListenAddr, "listen", opt.ListenAddr, "The address to serve information on")

	flagset.AddGoFlag(original.Lookup("v"))

	if err := cmd.Execute(); err != nil {
		klog.Exitf("Execute error: %v", err)
	}
}
