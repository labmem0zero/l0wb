package service

import (
	"L0/cache"
	"L0/model"
	"errors"
	"fmt"
	gorillactx "github.com/gorilla/context"
	"github.com/jmoiron/sqlx"
	"log"
	"net/http"
	"time"
)
var Db *sqlx.DB
var Cache *cache.Cache

var dbInitQuery=`
CREATE TABLE IF NOT EXISTS orders
	(order_uid TEXT UNIQUE,
	track_number TEXT,
	entry TEXT,
	locale TEXT,
	internal_signature TEXT,
	customer_id TEXT,
	delivery_service TEXT,
	shardkey TEXT,
	sm_id INT,
	date_created TEXT,
	oof_shard TEXT
);

CREATE TABLE IF NOT EXISTS deliveries
	(order_uid TEXT UNIQUE,
	name TEXT,
	phone TEXT,
	zip TEXT,
	city TEXT,
	address TEXT,
	region TEXT,
	email TEXT
);

CREATE TABLE IF NOT EXISTS payments
	(order_uid TEXT UNIQUE,
	request_id TEXT,
	currency TEXT,
	provider TEXT,
	amount INT,
	payment_dt INT,
	bank TEXT,
	delivery_cost INT,
	goods_total INT,
	custom_fee INT
);

CREATE TABLE IF NOT EXISTS items
	(order_uid TEXT,
	chrt_id INT,
	track_number TEXT,
	price TEXT,
	rid TEXT,
	name TEXT,
	sale INT,
	size TEXT,
	total_price INT,
	nm_id INT,
	brand TEXT,
	status INT
);

CREATE TABLE IF NOT EXISTS nats
	(
	    host TEXT UNIQUE,
	    last TEXT
);
`

func DBInitialize(config model.DBConfig) *sqlx.DB {
	initString:=fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, config.DBName)
	db, err:=sqlx.Open("postgres",initString)
	if err!=nil{
		log.Fatal("Ошибка при открытии бд: ",err)
	}
	err=db.Ping()
	if err!=nil{
		log.Fatal("Ошибка при пинге бд: ",err)
	}
	_,err=db.Exec(dbInitQuery)
	if err!=nil{
		log.Fatal(err)
	}
	return db
}

func OrderAdd(o model.Order)error{
	insertOrderSchema:=`INSERT INTO orders 
	VALUES (:order_uid, :track_number, :entry,
:locale, :internal_signature, :customer_id, :delivery_service,:shardkey,
:sm_id, :date_created, :oof_shard)`
	insertDeliverySchema:=`INSERT INTO deliveries 
	VALUES (:order_uid, :name, :phone, :zip, :city, :address, :region, :email)
`
	insertPaymentSchema:=`INSERT INTO payments 
	VALUES (:order_uid, :request_id, :currency, :provider, :amount,
:payment_dt, :bank, :delivery_cost, :goods_total, :custom_fee)
`
	insertItemSchema:=`INSERT INTO items 
	VALUES (:order_uid, :chrt_id, :track_number, :price, :rid,
:name, :sale, :size, :total_price, :nm_id, :brand, :status)
`
	_, err:=Db.NamedExec(insertOrderSchema, o)
	if err!=nil{
		return errors.New(fmt.Sprintf("При добавлении %s в таблицу orders произошла ошибка:%s\n",o.OrderUid,err))
	}
	_, err=Db.NamedExec(insertDeliverySchema, o.Delivery)
	if err!=nil{
		OrderRem(o.OrderUid)
		return errors.New(fmt.Sprintf("При добавлении %s в таблицу orders произошла ошибка:%s\n",o.OrderUid,err))
	}
	_, err=Db.NamedExec(insertPaymentSchema, o.Payment)
	if err!=nil{
		OrderRem(o.OrderUid)
		return errors.New(fmt.Sprintf("При добавлении %s в таблицу orders произошла ошибка:%s\n",o.OrderUid,err))
	}
	for _,item:=range o.Items{
		_, err:=Db.NamedExec(insertItemSchema, item)
		if err!=nil{
			OrderRem(o.OrderUid)
			return errors.New(fmt.Sprintf("При добавлении %s в таблицу orders произошла ошибка:%s\n",o.OrderUid,err))
		}
	}
	Cache.Append(o)
	return nil
}

func OrderGet(r *http.Request,orderUid string){
	order,ok:=Cache.Get(orderUid)
	if ok==false{
		gorillactx.Set(r,"error",fmt.Sprintf("Заказ с OrderUid=%s не найден",orderUid))
		return
	}
	gorillactx.Set(r,"data",order)
}

func OrderGetAll(r *http.Request){
	gorillactx.Set(r,"data",Cache.GetAll())
}

func OrderRem(orderUid string){
	orderDeleteSchema:=`
	DELETE FROM orders WHERE order_uid=($1)
`
	paymentDeleteSchema:=`
	DELETE FROM payments WHERE order_uid=($1)
`
	deliveryDeleteSchema:=`
	DELETE FROM deliveries WHERE order_uid=($1)
`
	itemsDeleteSchema:=`
	DELETE FROM items WHERE order_uid=($1)
`
	_,err:=Db.Exec(orderDeleteSchema,orderUid)
	if err!=nil{
		log.Printf("Ошибка при удалении заказа с order_uid=%s: %s",orderUid,err)
		return
	}
	_,err=Db.Exec(paymentDeleteSchema,orderUid)
	if err!=nil{
		log.Printf("Ошибка при удалении оплаты с order_uid=%s: %s",orderUid,err)
		return
	}
	_,err=Db.Exec(deliveryDeleteSchema,orderUid)
	if err!=nil{
		log.Printf("Ошибка при удалении доставки с order_uid=%s: %s",orderUid,err)
		return
	}
	_,err=Db.Exec(itemsDeleteSchema,orderUid)
	if err!=nil{
		log.Printf("Ошибка при удалении товаров с order_uid=%s: %s",orderUid,err)
	}
	Cache.Remove(orderUid)
}

func NATSinit(msgTime time.Time){
	row:=Db.QueryRow("SELECT COUNT(*) FROM nats WHERE host = 'localhost' ")
	var x int
	row.Scan(&x)
	if x>0{
		return
	}
	time:= msgTime.Format(time.RFC822Z)
	_,err:=Db.Exec(`
	INSERT INTO nats VALUES ($1, $2)
`,"localhost",time)
	if err!=nil{
		log.Printf("%s: ошибка при создании бэкапа NATS: %s",time,err)
	}
}

func NATSwrite(msgTime time.Time){
	time:= msgTime.Format(time.RFC822Z)
	_,err:=Db.Exec(`
	UPDATE nats SET last = ($1)
	WHERE host='localhost'
`,time)
	if err!=nil{
		log.Printf("%s: ошибка при создании бэкапа NATS: %s",time,err)
	}
}

func NATSread() time.Time{
	loaded:=""
	row:=Db.QueryRow(`
	SELECT last FROM nats
`)
	err:=row.Scan(&loaded)
	if err!=nil{
		log.Println("Невозможно закгрузить бэкап NATS, пропущенные сообщения будут проигнорированны")
	}
	restoredTime,err:=time.Parse(time.RFC822Z, loaded)
	if err!=nil{
		log.Println("Невозможно закгрузить бэкап NATS, пропущенные сообщения будут проигнорированны")
	}
	return restoredTime
}
