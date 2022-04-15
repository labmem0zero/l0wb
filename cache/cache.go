package cache

import (
	"L0/model"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
)

var Db *sqlx.DB

type Cache struct {
	orders map[string]model.Order
	natsLast time.Time
}

func CacheNew()*Cache {
	var c Cache
	c.orders=make(map[string]model.Order)
	return &c
}

func (c *Cache)Append(order model.Order){
	c.orders[order.OrderUid]=order
}

func (c *Cache)Load(){
	orderQuerySchema:=`SELECT * FROM orders
`
	paymentQuerySchema:=`SELECT * FROM payments
WHERE order_uid=($1)`
	deliveryQuerySchema:=`SELECT * FROM deliveries
WHERE order_uid=($1)`
	itemQuerySchema:=`SELECT * FROM items
WHERE order_uid=($1)`
	rows,err:= Db.Queryx(orderQuerySchema)
	if err!=nil{
		log.Printf("При поиске заказов возникла ошибка:%s",err)
		return
	}
	orders:=make(map[string]model.Order)
	for rows.Next(){
		var order model.Order
		err:=rows.StructScan(&order)
		if err!=nil{
			log.Printf("При загрузке в кэш заказа с uid=%s возникла ошибка:%s",order.OrderUid,err)
			continue
		}
		row:= Db.QueryRowx(paymentQuerySchema,order.OrderUid)
		err=row.StructScan(&order.Payment)
		if err!=nil{
			log.Printf("При загрузке в кэш оплаты заказа с uid=%s возникла ошибка:%s",order.OrderUid,err)
			continue
		}
		row= Db.QueryRowx(deliveryQuerySchema,order.OrderUid)
		err=row.StructScan(&order.Delivery)
		if err!=nil{
			log.Printf("При загрузке в кэш заказа с uid=%s возникла ошибка:%s",order.OrderUid,err)
			continue
		}
		rows,err:= Db.Queryx(itemQuerySchema,order.OrderUid)
		if err!=nil{
			log.Printf("При загрузке в кэш заказа с uid=%s возникла ошибка:%s",order.OrderUid,err)
			continue
		}
		var item model.Item
		for rows.Next(){
			err:=rows.StructScan(&item)
			if err!=nil{
				log.Printf("При сзагрузке в кэш товара заказа с uid=%s возникла ошибка:%s",order.OrderUid,err)
				continue
			}
			order.Items=append(order.Items,item)
		}
		orders[order.OrderUid]=order
	}
	c.orders=orders
}

func (c Cache) GetAll()[]string{
	var orders []string
	for o,_:=range c.orders{
		orders=append(orders,o)
	}
	return orders
}

func (c Cache) Get(orderuid string)(order model.Order, ok bool){
	order,ok=c.orders[orderuid]
	if ok==true{
		order.PrepareOut()
	}
	return order,ok
}

func (c *Cache) Remove(orderuid string){
	delete(c.orders,orderuid)
}
