[![Build Status](https://travis-ci.com/OGKevin/go-sonic.svg?branch=master)](https://travis-ci.com/OGKevin/go-sonic)

Golang bindings for https://github.com/valeriansaliou/sonic

```golang
	c, err := NewClientWithPassword("localhost:1491", "SecretPassword", context.Background())
	if err != nil {
		panic(err)
	}

	// Ingest
	_, err = c.IngestService.Push(NewDataBuilder().Text("my string").Bucket("my bucket").Collection("my collection").Object("my object").Build())
	if err != nil {
		panic(err)
	}

	// Search
	ch, err := c.SearchService.Query(NewDataBuilder().Collection("my collection").Bucket("my bucket").Text("my string").Build(), 0, 0)
	for e := range ch {
		log.Println(e)
	}
```

For protocol info, see https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md
