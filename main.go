package main

import (
	"L0/cache"
	"L0/controller"
	"L0/model"
	"L0/service"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"log"
	"net/http"
	"strings"
	"time"
)

var db *sqlx.DB

var dbcfg=model.DBConfig{
	"localhost",
	"5432",
	"root",
	"root",
	"l0db",
}

func natsReceiver(){
	timestamp,_:=time.Parse(time.RFC822Z, "15 Apr 22 19:00 +0300")
	service.NATSinit(timestamp)
	stantime:=service.NATSread()
	log.Println("Бэкап NATS загружен, дата и время последнего прочтенного сообщения:",stantime.Format(time.RFC822Z))
	sc, err := stan.Connect("test-cluster", "go-nats-streaming-json-receiver")
	if err!=nil{
		log.Fatal("NATS не загружен",err)
	}
	sub, _ := sc.Subscribe("orders", func(m *stan.Msg) {
		service.NATSwrite(time.Now())
		var order model.Order
		err:=json.Unmarshal(m.Data,&order)
		if err!=nil{
			log.Println("Невозможно запарсить сообщение из NATS :(")
		}else{
			order.PrepareIn()
			err:=service.OrderAdd(order)
			if err!=nil{
				if strings.Contains(err.Error(),"повторяющееся значение ключа")==false{
					log.Println(err)
				}
			}
		}
	},stan.StartAtTime(stantime))
	time.Sleep(10*time.Minute)
	sub.Unsubscribe()
}

func main(){
	Cache:=cache.CacheNew()
	service.Cache=Cache
	mR:=mux.NewRouter()
	mR.StrictSlash(true)

	db=service.DBInitialize(dbcfg)
	db.SetMaxOpenConns(100)
	service.Db=db
	cache.Db=db
	service.DBInitialize(dbcfg)

	Cache.Load()

	go natsReceiver()

	getOrder:=http.HandlerFunc(controller.OrderGet)
	getOrderAll:=http.HandlerFunc(controller.OrderGetAll)

	mR.Handle("/api/v1/orders/{orderuid}", controller.MiddlewarePackData(getOrder))
	mR.Handle("/api/v1/orders/", controller.MiddlewarePackData(getOrderAll)).Methods("GET")
	log.Fatal(http.ListenAndServe("localhost:8000",mR))
}
