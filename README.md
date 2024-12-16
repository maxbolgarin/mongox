# MongoX

[![Go Version][version-img]][doc] [![GoDoc][doc-img]][doc] [![Build][ci-img]][ci] [![GoReport][report-img]][report]


<picture>
  <img src=".github/logo.png" width="500" alt="gorder logo">
</picture>


## A Handy MongoDB Interface for Go

`mongox` is a high-level MongoDB interface for Go that simplifies database operations and provides rich error handling. It wraps the official MongoDB Go driver to reduce boilerplate code and make MongoDB operations more intuitive.

Whether you are building small applications or large-scale systems, `mongox` streamlines your database operations, allowing you to focus on what truly matters—developing your application.

## Table of Contents
- [Features](#features)
- [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Initializing](#initializing)
    - [CRUD Operations](#crud-operations)
        - [Insert](#insert)
        - [Find](#find)
        - [Update](#update)
        - [Delete](#delete)
    - [Advanced Usage](#advanced-usage)
        - [Index Management](#index-management)
        - [Error Handling](#error-handling)
        - [Async Operations](#async-operations)
    - [Best Practices](#best-practices)
    - [Limitations](#limitations)
- [Contributing](#contributing)
- [License](#license)


## Features

- **Simplified Interface:** Reduce boilerplate code with an intuitive API.
- **Rich Error Handling:** Error types for all possible error codes
- **Concurrent Safety:** Designed for safe use across multiple goroutines.
- **Well tested code**: Built on top of official MongoDB Go driver, `mongox` has a 80% test coverage with integration tests using real MongoDB instance.


## Getting Started


### Installation

```bash
go get -u github.com/maxbolgarin/mongox
```

### Initializing

Before performing any operations, initialize a `Collection` instance:

```go
cfg := mongox.Config{
    AppName: "MyApp",
    Address: "localhost:27017",
    Auth: &mongox.AuthConfig{
        Username: "username",
        Password: "password",
    },
    // TODO: other settings
}

client, err := mongox.Connect(ctx, cfg)
if err != nil {
    return err
}
defer client.Disconnect(ctx) // TODO: handle error

db := client.Database("mydb")
collection := db.Collection("users")
```

### CRUD Operations

#### Insert

Insert single or multiple documents into the collection:

```go
type User struct {
    Name string `bson:"name"`
    Age  int    `bson:"age"`
}
```

```go
user := User{Name: "Alice", Age: 30}

// Insert a single document
_, err := collection.Insert(ctx, user)
if err != nil {
    return err
}

// Insert multiple documents
_, err = collection.Insert(ctx,
    User{Name: "Bob", Age: 30},
    User{Name: "Charlie", Age: 35},
)
if err != nil {
    return err
}

// Insert with generic method
_, err = mongox.Insert(ctx, collection, User{Name: "Mike", Age: 20})
if err != nil {
    return err
}
```

### Find

Get documents from the collection:

```go
// Define a filter
filter := mongox.M{"age": mongox.Gt(20)}

// Find multiple documents
var users []mongox.User
err = collection.Find(ctx, &users, filter)
if err != nil {
    return err
}

// Find a one document with a generic method and applied sort
result, err := mongox.FindOne[User](ctx, collection, filter, mongox.FindOptions{
    Sort: mongox.M{"age": 1},
})
if err != nil {
    return err
}
```

#### Update

Modify existing documents in the collection. You can redefine `mongox.M` to make code cleaner:

```go
type M = mongox.M
```

```go
update := M{
    mongox.Inc: M{
        "age": 1,
    },
}

// Update a single document
err := collection.UpdateOne(ctx, filter, update)
if err != nil {
    return err
}

// Set new fields
err := collection.SetFields(ctx, filter, M{"new_field": "value"})
if err != nil {
    return err
}

// Upsert a document (update or insert)
record := User{
    Name: "Diana",
    Age:  28,
}
_, err = collection.Upsert(ctx, record, M{"name": "Alice"})
if err != nil {
    return err
}
```

#### Delete

Remove documents from the collection:

```go
// Delete a single document
err := collection.DeleteOne(ctx, M{"name": "Diana"})
if err != nil {
    return err
}

// Delete multiple documents
n, err = collection.DeleteMany(ctx, M{"age": mongox.Lt(30)})
if err != nil {
    return err
}
```

### Advanced Usage

#### Index Management

Create indexes to optimize query performance.

```go
// Create a unique index on the 'email' field
err := collection.CreateIndex(ctx, true, "email")
if err != nil {
    return err
}

// Create a text index for text search on 'name' and 'bio' fields
err = collection.CreateTextIndex(ctx, "en", "name", "bio")
if err != nil {
    return err
}
```

#### Error Handling

`mongox` provides error handling:

```go
err := collection.Insert(ctx, User{
    Name: eveName,
    Age:  eveAge,
})
if err != nil {
    if errors.Is(err, mongox.ErrNotFound) {
        // Handle not found
    } else if errors.Is(err, mongox.ErrInvalidArgument) {
        // Handle invalid argument
    } else {
        // Handle other errors
    }
}
```

#### Async Operations

`mongox` supports asynchronous operations using `AsyncCollection`:

```go
asyncCollection := client.AsyncDatabase("mydb").AsyncCollection("users")

// Insert a document asynchronously
asyncCollection.Insert("users_queue", "insert_task", User{
    Name: "Alice",
    Age:  30,
})
```

- Operations with the same queue name (first argument, `users_queue` in example) will be executed sequentially in strict order of calling
- Operations with different queue names will be executed in parallel
- Name is using for logging.


## Contributing
Contributions are welcome! Please feel free to submit a Pull Request or an Issue.

## License

This project is licensed under the terms of the [MIT License](LICENSE).

[MIT License]: LICENSE.txt
[version-img]: https://img.shields.io/badge/Go-%3E%3D%201.22-%23007d9c
[doc-img]: https://pkg.go.dev/badge/github.com/maxbolgarin/mongox
[doc]: https://pkg.go.dev/github.com/maxbolgarin/mongox
[ci-img]: https://github.com/maxbolgarin/mongox/actions/workflows/go.yaml/badge.svg
[ci]: https://github.com/maxbolgarin/mongox/actions
[report-img]: https://goreportcard.com/badge/github.com/maxbolgarin/mongox
[report]: https://goreportcard.com/report/github.com/maxbolgarin/mongox
