package controller

import (
	"encoding/json"
	gorillactx "github.com/gorilla/context"
	"log"
	"net/http"
)

func MiddlewarePackData(handler http.Handler)http.Handler{
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w,r)
		data:=gorillactx.Get(r, "data")
		gorillactx.Delete(r,"data")
		if gorillactx.Get(r, "error")!=nil{
			w.WriteHeader(404)
			w.Write([]byte(gorillactx.Get(r, "error").(string)))
			gorillactx.Delete(r,"error")
			return
		}
		if data==nil{
			w.WriteHeader(404)
			w.Write([]byte("Информация не найдена"))
			return
		}
		responseBody,_:=json.MarshalIndent(data, " ", "\t")
		w.Header().Set("Content-Type","application/json")
		w.WriteHeader(http.StatusOK)
		_,err:=w.Write(responseBody)
		if err!=nil{
			log.Println("Ошибка при отправке ответа:",err)
		}
	})
}