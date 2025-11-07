/*
 * AUTOR: Claudia Pavón Calcerrada, Paula Melero Laviña
 * ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
 *			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
 * FECHA: noviembre de 2025
 * FICHERO: main.go
 * EJECUTAR: go run main.go 0-2 192.168.3.5:29260 192.168.3.6:29260 192.168.3.7:29260
 */
package main

import (
	//"errors"
	"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
	//"time"
)

func main() {
	// obtener entero de indice de este nodo
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []rpctimeout.HostPort
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	rpc.Register(nr)

	fmt.Println("Replica escucha en :", me, " de ", os.Args[2:])

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")

	rpc.Accept(l)
}
