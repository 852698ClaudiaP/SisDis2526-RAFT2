/*
 * AUTOR: Claudia Pavón Calcerrada, Paula Melero Laviña
 * ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
 *			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
 * FECHA: noviembre de 2025
 * FICHERO: testintegracionraft1_test.go
 */
package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"

	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const (
	//hosts
	//MAQUINA1 = "192.168.3.6"
	MAQUINA1 = "localhost"
	MAQUINA2 = "localhost"
	MAQUINA3 = "localhost"

	//puertos
	PUERTOREPLICA1 = "29260"
	PUERTOREPLICA2 = "29261"
	PUERTOREPLICA3 = "29262"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	//PRIVKEYFILE = "id_ed25519"
	PRIVKEYFILE = "id_rsa"
)

// PATH de los ejecutables de modulo golang de servicio Raft
//var PATH string = filepath.Join(os.Getenv("HOME"), "tmp", "p3", "raft")
//var PATH string = filepath.Join("/misc/alumnos/sd/sd2526/a852698/practica4", "raft")
var PATH string = filepath.Join("$HOME/sisdislasttry/practica4/practica4", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se ponen en marcha las replicas - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Espera para que hayan elegido lider
	time.Sleep(4000 * time.Millisecond)

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Espera para que hayan elegido lider
	time.Sleep(4000 * time.Millisecond)

	fmt.Printf("Lider inicial\n")
	lider := cfg.pruebaUnLider(3)

	fmt.Printf("Encontrado lider %d\n", lider)

	cfg.stopDistributedProcess(lider)
	cfg.conectados[lider] = false

	time.Sleep(16000 * time.Millisecond)

	fmt.Printf("Comprobar nuevo lider\n")
	newlider := cfg.pruebaUnLider(3)
	fmt.Printf("Encontrado lider %d\n", newlider)

	// Parar réplicas almacenamiento en remoto
	for i := 0; i < 3; i++ {
		if i != lider {
			cfg.stopDistributedProcess(i)
		}
	}

	cfg.conectados[lider] = true

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	cfg.startDistributedProcesses()

	// Espera para que hayan elegido lider
	time.Sleep(4000 * time.Millisecond)

	IdLider := cfg.pruebaUnLider(3)

	fmt.Printf("Encontrado lider %d\n", IdLider)

	cfg.someterOperacionRaft(IdLider)
	cfg.someterOperacionRaft(IdLider)
	cfg.someterOperacionRaft(IdLider)

	cfg.stopDistributedProcesses()
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	//t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	cfg.startDistributedProcesses()

	// Espera para que hayan elegido lider
	time.Sleep(4000 * time.Millisecond)

	lider := cfg.pruebaUnLider(3)

	// Comprometer una entrada
	cfg.someterOperacionRaft(lider)

	// Obtener un lider y, a continuación desconectar una de los nodos Raft
	seguidor := 0
	if lider == seguidor {
		seguidor = 1
	}

	cfg.stopDistributedProcess(seguidor)
	cfg.conectados[seguidor] = false

	// Comprobar varios acuerdos con una réplica desconectada
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
	cfg.startDistributedProcess(seguidor)
	cfg.conectados[seguidor] = true

	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	time.Sleep(200 * time.Millisecond)

	cfg.stopDistributedProcesses()
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	//t.Skip("SKIPPED SinAcuerdoPorFallos")

	cfg.startDistributedProcesses()

	// Espera para que hayan elegido lider
	time.Sleep(4000 * time.Millisecond)

	lider := cfg.pruebaUnLider(3)

	// Comprometer una entrada
	cfg.someterOperacionRaft(lider)

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft
	for i := 0; i <= 2; i++ {
		if i != lider {
			cfg.stopDistributedProcess(i)
			cfg.conectados[i] = false
		}
	}

	// Comprobar varios acuerdos con 2 réplicas desconectada
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)

	// reconectar los 2 nodos Raft desconectados y probar varios acuerdos
	for i := 0; i <= 2; i++ {
		if i != lider {
			cfg.startDistributedProcess(i)
			cfg.conectados[i] = true
		}
	}

	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	cfg.someterOperacionRaft(lider)
	time.Sleep(200 * time.Millisecond)

	cfg.stopDistributedProcesses()
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	// A completar ???

	// un bucle para estabilizar la ejecucion

	// Obtener un lider y, a continuación someter una operacion

	// Someter 5  operaciones concurrentes

	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 20; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 100*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

func (cfg *configDespliegue) someterOperacionRaft(indiceNodo int) (
	int, int, bool, int, string) {

	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{Operacion: "a", Clave: "b", Valor: "c"}, &reply, 200*time.Millisecond)

	check.CheckError(err, "Error en llamada RPC SometerOperacionRaft")

	return reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2000 * time.Millisecond)
}

func (cfg *configDespliegue) startDistributedProcess(node int) {
	//cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	despliegue.ExecMutipleHosts(EXECREPLICACMD+
		" "+strconv.Itoa(node)+" "+
		rpctimeout.HostPortArrayToString(cfg.nodosRaft),
		[]string{cfg.nodosRaft[node].Host()}, cfg.cr, PRIVKEYFILE)

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(2000 * time.Millisecond)
}

//
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for _, endPoint := range cfg.nodosRaft {
		err := endPoint.CallTimeout("NodoRaft.ParaNodo",
			raft.Vacio{}, &reply, 10*time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
	time.Sleep(2000 * time.Millisecond)
}

func (cfg *configDespliegue) stopDistributedProcess(node int) {
	var reply raft.Vacio

	fmt.Printf("Parado nodo %d\n", node)

	err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 10*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}
