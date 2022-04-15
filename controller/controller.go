package controller

import (
	"L0/service"
	"github.com/gorilla/mux"
	"net/http"
)

func OrderGet(w http.ResponseWriter, r *http.Request){
	vars:=mux.Vars(r)
	service.OrderGet(r,vars["orderuid"])
}


func OrderGetAll(w http.ResponseWriter, r *http.Request){
	service.OrderGetAll(r)
}
