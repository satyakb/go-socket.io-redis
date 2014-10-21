package redis

import (
    "fmt"
    "log"
    "reflect"
    "strings"
    "github.com/googollee/go-socket.io"
    "github.com/garyburd/redigo/redis"
    "github.com/nu7hatch/gouuid"
    // "github.com/vmihailenco/msgpack"  // screwed up types after decoded
    "encoding/json"
)

type broadcast struct {
  host string
  port string
  pub redis.PubSubConn
  sub redis.PubSubConn
  prefix string
  uid string
  key string
  remote bool
  rooms map[string]map[string]socketio.Socket
}

//
// opts: {
//   "host": "127.0.0.1",
//   "port": "6379"
//   "prefix": "socket.io"
// }
func Redis(opts map[string]string) socketio.BroadcastAdaptor {
  b := broadcast {
    rooms: make(map[string]map[string]socketio.Socket),
  }

  var ok bool
  b.host, ok = opts["host"]
  if !ok {
    b.host = "127.0.0.1"
  }
  b.port, ok = opts["port"]
  if !ok {
    b.port = "6379"
  }
  b.prefix, ok = opts["prefix"]
  if !ok {
    b.prefix = "socket.io"
  }

  pub, err := redis.Dial("tcp", ":" + b.port)
  if err != nil {
      panic(err)
  }
  // defer pub.Close()
  sub, err := redis.Dial("tcp", ":" + b.port)
  if err != nil {
      panic(err)
  }
  // defer sub.Close()
  // var wg sync.WaitGroup
  // wg.Add(1)
  // psc := redis.PubSubConn{Conn: c}

  b.pub = redis.PubSubConn{Conn: pub}
  b.sub = redis.PubSubConn{Conn: sub}

  uid, err := uuid.NewV4();
  if err != nil {
    fmt.Println("error:", err)
    return nil
  }
  b.uid = uid.String()
  b.key = b.prefix + "#" + b.uid

  b.remote = false

  fmt.Println("Key:", b.key)

  b.sub.PSubscribe(b.prefix + "#*")
  // This goroutine receives and prints pushed notifications from the server.
  // The goroutine exits when the connection is unsubscribed from all
  // channels or there is an error.
  go func() {
      // defer wg.Done()
      for {
          switch n := b.sub.Receive().(type) {
          case redis.Message:
              fmt.Printf("Message: %s %s\n", n.Channel, n.Data)
          case redis.PMessage:
              fmt.Println(n)
              b.onmessage(n.Channel, n.Data)
              fmt.Printf("PMessage: %s %s %s\n", n.Pattern, n.Channel, n.Data)
          case redis.Subscription:
              fmt.Printf("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count)
              if n.Count == 0 {
                  return
              }
          case error:
              fmt.Printf("error: %v\n", n)
              return
          }
      }
  }()

  return b
}

func (b broadcast) onmessage(channel string, data []byte) error {
  pieces := strings.Split(channel, "#");
  uid := pieces[len(pieces) - 1]
  if b.uid == uid {
    // some kind of error here
    fmt.Println("same server")
    return nil
  }
  fmt.Println("onmessage")
  fmt.Println("DATA:", data)
  var out map[string][]interface{}
  err := json.Unmarshal(data, &out)
  fmt.Println(err, out)

  fmt.Println(reflect.TypeOf(out))

  // out, ok := args[len(args)-1].(map[string]interface{})
  // fmt.Println("OUT:", out)
  // if !ok {
  //   return nil
  // }

  // m, ok := out.(map[string][]interface{})
  // if !ok {
  //   fmt.Println("map not okay")
  //   return nil
  // }

  args := out["args"]
  fmt.Println("args:", reflect.TypeOf(args))
  opts := out["opts"]
  room, ok := opts[0].(string)
  if !ok {
    fmt.Println("room didn't work")
    room = ""
  }
  message, ok := opts[1].(string)
  if !ok {
    fmt.Println("message didn't work")
    message = ""
  }

  // args, ok := out["args"].([]interface{})
  // if !ok {
  //   fmt.Println("args not ok")
  //   args = nil
  // }

  // room, ok := out["opts"].(string)
  // if !ok {
  //   fmt.Println("room not ok")
  //   room = "" 
  // }
  // str, ok := out["message"].(string)
  // if !ok {
  //   fmt.Println("message not ok")
  //   return nil
  // }
  // ignore, ok := out["ignore"].(socketio.Socket)
  // if !ok {
  //   fmt.Println("ignore not ok")
  //   ignore = nil
  // }

  // rem := map[string]bool{
  //   "remote": true,
  // }
  // args[len(args)-1] = rem
  fmt.Println("ARGS2:", out)
  
  b.remote = true;
  b.Send(nil, room, message, args)
  return nil
}

func (b broadcast) Join(room string, socket socketio.Socket) error {
  sockets, ok := b.rooms[room]
  if !ok {
    sockets = make(map[string]socketio.Socket)
  }
  sockets[socket.Id()] = socket
  b.rooms[room] = sockets
  return nil
}

func (b broadcast) Leave(room string, socket socketio.Socket) error {
  sockets, ok := b.rooms[room]
  if !ok {
    return nil
  }
  delete(sockets, socket.Id())
  if len(sockets) == 0 {
    delete(b.rooms, room)
    return nil
  }
  b.rooms[room] = sockets
  return nil
}

// Same as Broadcast
func (b broadcast) Send(ignore socketio.Socket, room, message string, args []interface{}) error {
  fmt.Println("lol here")
  fmt.Println(args, reflect.TypeOf(args[0]))
  sockets := b.rooms[room]
  for id, s := range sockets {
    fmt.Println("looping")
    if ignore != nil && ignore.Id() == id {
      fmt.Println("skipped")
      continue
    }
    err := (s.Emit(message, args...))
    if err != nil {
      log.Fatal(err)
    }
  }

  // fmt.Println("ARGS:", args...);

  // buf := &bytes.Buffer{}
  // enc := msgpack.NewEncoder(buf)
  opts := make([]interface{}, 2)
  opts[0] = room
  opts[1] = message
  in := map[string][]interface{}{
    "args": args,
    "opts": opts,
  }

  fmt.Println("IN: ", in)

  // append := make([]interface{}, len(args) + 1)
  // for i := range args {
  //   append[i] = args[i]
  // }
  // append[len(args)] = in

  buf, err := json.Marshal(in)
  _ = err
  
  // remote := false
  // rem, ok := args[len(args) - 1].(map[string]bool)
  // if !ok {
  //   remote = false
  // }
  // remote = rem["remote"]

  if !b.remote {
    b.pub.Conn.Do("PUBLISH", b.key, buf)
    b.remote = false
  }
  return nil
}