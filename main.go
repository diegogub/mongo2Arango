package main

import (
  ara "github.com/diegogub/aranGO"
  "log"
  mgo "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
  "flag"
)


var (
  arangoServer = flag.String("arangoServer","http://localhost:8529","ArangoDb server")
  arangoUser   = flag.String("arangoUser","","ArangoDB user")
  arangoPassword = flag.String("arangoPass","","ArangoDB password")
  arangoDB    = flag.String("arangoDB","","ArangoDB database to create collection")
  mongoServer = flag.String("mongoServer", "","MongoDB server")
  mongoDB     = flag.String("mongoDB", "","MongoDB database")
  collections = flag.String("col","","Collections to migrate")
  arangoCol   = flag.String("arangoCol","","collection in arango")
  newIds      = flag.Bool("preserve-ids",false,"Preserve or not unique ids")
  batchSize   = flag.Int("batch-size",1000,"Batch size to fetch from servers")
  sync        = flag.Bool("sync",false,"Sync to arangodb, much slower than normal")
)

func ConnectArango() *ara.Session{
  s ,err := ara.Connect(*arangoServer,*arangoUser,*arangoPassword,false)
  if err != nil {
    panic(err)
  }

  if s == nil {
    panic("Invalid arangodb session")
  }
  return s
}

func ConnectMongo() *mgo.Session {
  s, err := mgo.Dial(*mongoServer)
  if err != nil{
    panic(err)
  }

  return s
}

func FetchMongo(m *mgo.Session,col string) chan interface{}{
  // buffered channel
  out := make(chan interface{},*batchSize)
  // fetch from MongoDB
  num, err := m.DB(*mongoDB).C(col).Count()
  log.Println("Found:",num)
  if err != nil {
    panic(err)
  }

  if num == 0 {
    close(out)
  }

  pages := int(num / *batchSize) + 1
  log.Println("Pages:",pages)

  go func(pages,batch int){
    for i:= 0 ; i < pages ; i++ {
      var result []interface{}
      m.DB(*mongoDB).C(col).Find(nil).Skip(i*batch).Limit(*batchSize).All(&result)
      for j := 0 ; j < len(result) ;j++ {
        parsed := make(map[string]interface{})
        for k,v := range result[j].(bson.M) {
          if k == "_id" {
            parsed["_key"] = v
          }else{
            parsed[k] = v
          }
        }
        out <- parsed
      }
    }
    close(out)
  }(pages,*batchSize)

  return out
}

func PushToArango(a *ara.Session,col string,ch chan interface{}) {
  db := a.DB(*arangoDB)
  if db == nil {
    panic("invalid db")
  }

  var err error
  for i := range ch {
    if *arangoCol == "" {
      err = db.Col(col).Save(i)
    }else{
      err = db.Col(*arangoCol).Save(i)
    }
    if err != nil {
      panic(err)
    }
  }
}

func main() {
  flag.Parse()
  a := ConnectArango()
  m := ConnectMongo()
  ch := FetchMongo(m,*collections)
  PushToArango(a,*collections,ch)
}
