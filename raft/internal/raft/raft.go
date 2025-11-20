/*
 * AUTOR: Claudia Pavón Calcerrada, Paula Melero Laviña
 * ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
 *			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
 * FECHA: noviembre de 2025
 * FICHERO: raft.go
 */

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"raft/internal/comun/rpctimeout"
	"sync"
	"time"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = true

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"

	// Cuanto tiempo tiene la replica para responder a CallTimeout
	tRespCall = 20 * time.Millisecond
	// Cada cuanto se envía un latido
	tLatido = 1 * time.Second
	// Cuanto espera un seguidor a recibir un latido antes de
	// dar al líder como caído e iniciar una elección
	tEspLatidoMin = 6000
	tEspLatidoMax = 10000
	// Tiempo minimo hasta iniciar nueva elección (en segundos)
	tEleccMin = 2000
	// Tiempo máximo hasta iniciar nueva elección (en segundos)
	tEleccMax = 6000
)

type Estado string

const (
	seguidor  Estado = "seguidor"
	candidato Estado = "candidato"
	lider     Estado = "lider"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Vuestros datos aqui.
	VotosRecibidos   int       //si es candidato, los votos que ha recibido
	Latido           chan bool //si el nodo recibe un latido del corazon
	Estado           Estado    //papel que desempena el nodo
	cambiarASeguidor chan bool //si el nodo vuelve a ser seguidor
	cambiarALider    chan bool //si el nodo es o sigue siendo lider
	MandatoActual    int       //ultimo mandato visto por el servidor
	CandiVotado      int       //candidato que ha recibido el voto de este nodo

	Log []Entrada //

	// Indice de la entrada mas alta que ha sido comprometida
	CommitIndice int
	// Indice de la entrada mas alta que ha sido aplicada
	AppliedIndice int
	// Por cada nodo, indice de la siguiente entrada que enviar a ese nodo
	nextIndice []int
	// Por cada nodo, indice de la entrada mas alta que se sabe esta replicada
	lastIndice []int
}

// Entrada que guardar en el registro
type Entrada struct {
	Mandato   int
	Operacion AplicaOperacion
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO

	// Estado del nodo
	nr.Estado = seguidor
	// Canal por el que llegan latidos
	nr.Latido = make(chan bool)
	// Canal que indica si hay que cambiar a Seguidor
	nr.cambiarASeguidor = make(chan bool)
	// Canal que indica si hay que cambiar a Lider
	nr.cambiarALider = make(chan bool)
	// Mandato actual
	nr.MandatoActual = 0
	// Candidato que ha votado este nodo
	nr.CandiVotado = IntNOINICIALIZADO
	// Votos que ha recibido este nodo
	nr.VotosRecibidos = 0

	nr.CommitIndice = 0
	nr.AppliedIndice = 0

	for i := 0; i < len(nr.Nodos); i++ {
		nr.nextIndice = append(nr.nextIndice, 0)
		nr.lastIndice = append(nr.lastIndice, 0)
	}

	if kEnableDebugLogs {
		nr.Logger = initLogger(nodos[yo])
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(io.Discard, "", 0)
	}

	// Añadir codigo de inicialización
	go seleccionarEstado(nr)
	go aplicarEntradas(nr, canalAplicarOperacion)
	go enviarEntradas(nr)

	return nr
}

// Envia las entradas comprometidas por el canal canalAplicarOperacion al main
func aplicarEntradas(nr *NodoRaft, canalAplicarOperacion chan AplicaOperacion) {
	for {
		if nr.CommitIndice > nr.AppliedIndice {
			// Bloqueo
			nr.Mux.Lock()
			nr.AppliedIndice++
			entrada := nr.Log[nr.AppliedIndice].Operacion
			nr.Mux.Unlock()
			canalAplicarOperacion <- entrada
		}
	}
}

// Envia entradas nuevas al resto de nodo si es lider
func enviarEntradas(nr *NodoRaft) {
	for {
		if nr.Estado == lider {
			// Mandar entrada a nodos
			for nodo := 0; nodo < len(nr.Nodos); nodo++ {
				if nodo != nr.Yo {

					if nr.getUltimoIndice() >= nr.nextIndice[nodo] {
						var resultados Results
						go nr.enviarOperacion(nodo,
							&ArgAppendEntries{
								nr.MandatoActual,
								nr.Yo,
								nr.getUltimoIndice(),
								nr.getUltimoMandato(),
								nr.Log[nr.nextIndice[nodo]],
								nr.CommitIndice,
							},
							&resultados)
					}

				}
			}
		}
	}
}

func initLogger(nodo rpctimeout.HostPort) *log.Logger {
	nombreNodo := nodo.Host() + "_" + nodo.Port()
	fmt.Println("nombreNodo: ", nombreNodo)

	if kLogToStdout {
		return log.New(os.Stdout, nombreNodo+" -->> ",
			log.Lmicroseconds|log.Lshortfile)
	} else {
		err := os.MkdirAll(kLogOutputDir, os.ModePerm)
		if err != nil {
			panic(err.Error())
		}
		logOutputFile, err := os.OpenFile(
			fmt.Sprintf("%s/%s.txt", kLogOutputDir, nombreNodo),
			os.O_RDWR|os.O_CREATE|os.O_TRUNC,
			0755)
		if err != nil {
			panic(err.Error())
		}
		return log.New(logOutputFile,
			nombreNodo+" -> ", log.Lmicroseconds|log.Lshortfile)
	}
}

// Selecciona el codigo a ejecutar dependiendo del estado
func seleccionarEstado(nr *NodoRaft) {
	for {
		for nr.Estado == seguidor {
			soySeguidor(nr)
		}
		for nr.Estado == candidato {
			soyCandidato(nr)
		}
		for nr.Estado == lider {
			soyLider(nr)
		}
	}
}

// Codigo a ejecutar si es seguidor
func soySeguidor(nr *NodoRaft) {

	// Tiempo el seguidor acepta sin latidos
	// antes de dar al Líder como desaparecido
	espLatido := time.NewTimer(
		time.Duration(genRandom(tEspLatidoMin, tEspLatidoMax)) * time.Millisecond)

	select {
	case <-nr.Latido:
		nr.Logger.Printf("Mandato %d. He recibido latido, mi lider es %d\n",
			nr.MandatoActual, nr.IdLider)
	case <-espLatido.C:
		nr.Estado = candidato

		nr.Logger.Printf(
			"Mandato %d. No he recibido latidos, me vuelvo candidato.\n",
			nr.MandatoActual,
		)
	}
}

// Codigo a ejecutar si es candidato
func soyCandidato(nr *NodoRaft) {
	iniciarEleccion(nr)
	// Tiempo hasta iniciar una nueva elección
	tempElec := time.NewTimer(
		time.Duration(genRandom(tEleccMin, tEleccMax)) * time.Millisecond)

	select {
	case <-nr.cambiarALider:
		// Si recibe mayoria de votos entre todos los nodos
		nr.Estado = lider
		nr.Logger.Printf(
			"Mandato %d. He recibido mayoria, me convierto en lider.\n",
			nr.MandatoActual,
		)
	case <-nr.Latido:
		// Si recibe latidos de lider pasa a seguidor (otro ha sido elegido)
		nr.Estado = seguidor
		nr.Logger.Printf("Mandato %d. Era candidato y me ha llegado latido.\n",
			nr.MandatoActual,
		)
	case <-nr.cambiarASeguidor:
		// Si hay otro nodo con mayor mandato que el local, este pasa a seguidor
		nr.Estado = seguidor
		nr.Logger.Printf(
			"Mandato %d. Era candidato y me ha llegado un mandato superior.\n",
			nr.MandatoActual,
		)
	case <-tempElec.C:
		// Si el temporizador expira, inicia otra elección
		nr.Logger.Printf("Mandato %d. Ha expirado timer.\n", nr.MandatoActual)
	}
}

// Inicia una eleccion, actualiza mandato y envía peticiones de voto
func iniciarEleccion(nr *NodoRaft) {

	nr.Mux.Lock()
	nr.IdLider = IntNOINICIALIZADO

	// Se incrementa el mandato actual para garantizar que cada elección
	// de líder sea única y que no se superponga con elecciones anteriores.
	nr.MandatoActual++
	nr.Logger.Printf("Inicio elección en mandato %d\n", nr.MandatoActual)

	// El nodo se vota a si mismo
	nr.CandiVotado = nr.Yo
	nr.VotosRecibidos = 1

	nr.Mux.Unlock()

	// Se envian solicitudes de votos al resto de nodos
	pedirVotosNodos(nr)
}

// Pide votos al resto de procesos
func pedirVotosNodos(nr *NodoRaft) {
	var respuesta RespuestaPeticionVoto
	for idNodo := 0; idNodo < len(nr.Nodos); idNodo++ {
		if idNodo != nr.Yo {
			go nr.enviarPeticionVoto(idNodo,
				&ArgsPeticionVoto{nr.MandatoActual, nr.Yo}, &respuesta)
		}
	}
}

// Codigo a ejecutar si es líder
func soyLider(nr *NodoRaft) {
	// Tiempo entre envio de latidos
	tempLatido := time.NewTimer(tLatido)

	enviarLatidosNodos(nr)

	select {
	case <-nr.cambiarASeguidor:
		nr.Logger.Printf(
			"Mandato %d. Hay otro lider con mandato superior, me vuelvo seguidor.\n",
			nr.MandatoActual,
		)
		nr.Estado = seguidor
	case <-tempLatido.C:
	}
}

// Envía latido a todos los nodos que no sean él mismo
func enviarLatidosNodos(nr *NodoRaft) {
	var resultados Results
	for nodo := 0; nodo < len(nr.Nodos); nodo++ {
		if nodo != nr.Yo {
			go nr.enviarLatido(nodo,
				&ArgAppendEntries{
					nr.MandatoActual,
					nr.Yo,
					nr.getUltimoIndice(),
					nr.getUltimoMandato(),
					Entrada{},
					nr.CommitIndice,
				},
				&resultados)
		}
	}
}

// Devuelve el indice de la ultima entrada de este nodo
func (nr *NodoRaft) getUltimoIndice() int {
	return (len(nr.Log) - 1)
}

// Devuelve el mandato de la ultima entrada de este nodo
func (nr *NodoRaft) getUltimoMandato() int {
	if nr.getUltimoIndice() < 0 {
		return -1 // Log vacio
	}
	return nr.Log[nr.getUltimoIndice()].Mandato
}

// Se encarga de enviar un latido a un nodo específico y gestionar la respuesta.
func (nr *NodoRaft) enviarLatido(idNodo int, args *ArgAppendEntries,
	resultados *Results) bool {

	err := nr.Nodos[idNodo].CallTimeout("NodoRaft.AppendEntries", args,
		resultados, tRespCall)
	if err != nil {
		return false
	} else {
		if resultados.MandatoActual > nr.MandatoActual {
			// Si el nodo remoto (el que recibió el latido) tiene un mandato
			// mayor, esto indica que el nodo local ya no es el líder.
			nr.actualizarMandato(resultados.MandatoActual)
			nr.IdLider = IntNOINICIALIZADO
			nr.cambiarASeguidor <- true
		}
		return true
	}
}

// Mira si hay que actualizar el mandato
func (nr *NodoRaft) actualizarMandato(nuevoMandato int) {

	if nuevoMandato > nr.MandatoActual {
		nr.Logger.Printf(
			"Mandato %d. He actualizado a mandato %d\n",
			nr.MandatoActual, nuevoMandato,
		)
		nr.MandatoActual = nuevoMandato
	}
}

func genRandom(min int, max int) int {
	return ((rand.Intn(max - min)) + min)
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.MandatoActual
	var esLider bool = (nr.IdLider == nr.Yo)
	var idLider int = nr.IdLider

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantía que esta operación consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Resultado de este método :
// - Primer valor devuelto es el indice del registro donde se va a colocar
// - la operacion si consigue comprometerse.
// - El segundo valor es el mandato en curso
// - El tercer valor es true si el nodo cree ser el lider
// - Cuarto valor es el lider, es el indice del líder si no es él
// - Quinto valor es el resultado de aplicar esta operación en máquina de estados
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := nr.lastIndice[nr.Yo] + 1
	mandato := nr.MandatoActual
	EsLider := (nr.IdLider == nr.Yo)
	idLider := nr.IdLider
	valorADevolver := ""

	if EsLider {
		nr.Logger.Println("lider tratando de someter operacion")
		// Añadir entrada a mi log
		nr.addEntrada(Entrada{
			nr.MandatoActual,
			AplicaOperacion{indice, operacion},
		})

		// Esperar a que la mitad hayan añadido la entrada
		done := make(chan bool) //necesita mas espacio si se puede estar comprometiendo multiples entradas al mismo tiempo
		go nr.esperarComprometido(indice, done)
		<-done

		nr.Logger.Printf(
			"Mandato %d. Entrada comprometida\n",
			nr.MandatoActual,
		)
	}

	return indice, mandato, EsLider, idLider, valorADevolver
}

// Espera a que la entrada en el indice especificado este comprometida
// (presente en la mayoria de servidores)
func (nr *NodoRaft) esperarComprometido(indice int, done chan bool) {
	for {
		nodosComprometidos := 0
		for idNodo := 0; idNodo < len(nr.Nodos); idNodo++ {
			if nr.lastIndice[idNodo] >= indice {
				nodosComprometidos++
			}
		}
		nr.Logger.Printf(
			"HEREEEE. Comprometidos: %d. lastIndice: %d\n",
			nodosComprometidos, nr.lastIndice[nr.Yo],
		)
		if nodosComprometidos >= ((len(nr.Nodos) / 2) + 1) {
			done <- true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Se encarga de enviar un latido a un nodo específico y gestionar la respuesta.
func (nr *NodoRaft) enviarOperacion(idNodo int, args *ArgAppendEntries,
	resultados *Results) bool {

	err := nr.Nodos[idNodo].CallTimeout("NodoRaft.AppendEntries", args,
		&resultados, tRespCall)
	if err != nil {
		return false
	} else {
		return true
	}
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	nr.Logger.Println("nodo detenido")
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(args TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(args)

	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	// Vuestros datos aqui
	MandSolicitante int //mandato del candidato que pide voto
	IdSolicitante   int //id del candidato que pide el voto
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	// Vuestros datos aqui
	MandatoActual int  //mandato actual
	HaDadoSuVoto  bool //si el candidato que hizo la
	//solicitud ha recibido el voto
}

// Metodo para RPC PedirVoto
// respuesta del nodo tras recibir una petición de voto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {

	nr.Mux.Lock()
	if peticion.MandSolicitante <= nr.MandatoActual {

		negarVoto(&reply.MandatoActual, &nr.MandatoActual, &reply.HaDadoSuVoto)

		nr.Logger.Printf("Mandato %d. Voto negado a %d\n",
			nr.MandatoActual, peticion.IdSolicitante)

	} else {

		nr.actualizarMandato(peticion.MandSolicitante)
		darVoto(&nr.CandiVotado, peticion.IdSolicitante, &reply.MandatoActual,
			&nr.MandatoActual, &reply.HaDadoSuVoto)

		// Si no es ya seguidor, debe cambiar a seguidor
		if nr.Estado == lider || nr.Estado == candidato {
			nr.cambiarASeguidor <- true
		}
		nr.Logger.Printf("Mandato %d. Voto dado a %d\n",
			peticion.MandSolicitante, peticion.IdSolicitante)

	}
	nr.Mux.Unlock()
	return nil
}

// Niega el voto al candidato y le avisa de que está en un mandato inferior al
// actual
func negarVoto(mandatoActual *int, ultimoMandato *int, haVotado *bool) {

	*mandatoActual = *ultimoMandato
	*haVotado = false
	//*ultimoMandato++

}

// Da el voto al candidato
func darVoto(candidatoVotado *int, idSolicitante int,
	mandatoActual *int, ultimoMandato *int, haVotado *bool) {

	//*ultimoMandato = mandatoCandidato
	*candidatoVotado = idSolicitante
	*mandatoActual = *ultimoMandato
	*haVotado = true

}

type ArgAppendEntries struct {
	// Mandato del lider
	MandLider int
	// Id del lider
	IdLider int
	// Indice de la entrada anterior
	prevLogIndice int
	// Mandato de la entrada anterior
	prevLogMandato int
	// Entrada a añadir al log (por ahora solo una)
	Entradas Entrada
	// Commit index del lider
	liderCommit int
}

type Results struct {
	// Vuestros datos aqui
	Exito         bool
	MandatoActual int
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	// Completar....
	//si esta vacio se trata de un latido,
	//sino, se esta replicando una nueva entrada
	//en el registro de operaciones
	if args.Entradas == (Entrada{}) {
		//fmt.Printf("Mandato %d. Latido recibido\n", nr.MandatoActual)
		if (args.MandLider >= nr.MandatoActual) && (args.IdLider != nr.IdLider) {

			nr.reconocerNuevoLider(args.IdLider, nr.MandatoActual)

		}
		nr.actualizarMandato(args.MandLider)
		nr.Latido <- true //le hago saber que me ha llegado el latido

		results.Exito = true
		results.MandatoActual = nr.MandatoActual
	} else {
		//se introduce nueva entrada en el log
		nr.Logger.Printf("(%d,%d,%s,%s,%s)", args.Entradas.Operacion.Indice, args.Entradas.Mandato, args.Entradas.Operacion.Operacion.Operacion, args.Entradas.Operacion.Operacion.Clave, args.Entradas.Operacion.Operacion.Valor)

		nr.Logger.Printf(
			"Mandato %d. Entrada comprometida\n",
			nr.MandatoActual,
		)
		results.Exito = true
		results.MandatoActual = nr.MandatoActual
	}

	return nil
}

func (nr *NodoRaft) addEntrada(entrada Entrada) {
	nr.Log = append(nr.Log, entrada)
	nr.lastIndice[nr.Yo] = entrada.Operacion.Indice
}

// Reconoce a un nuevo nodo como lider
func (nr *NodoRaft) reconocerNuevoLider(nuevoIdLider int, mandato int) {
	nr.IdLider = nuevoIdLider
	nr.Logger.Printf("Mandato %d. Mi nuevo lider es %d.\n", mandato, nr.IdLider)
}

// --------------------------------------------------------------------------
// ----- METODOS/FUNCIONES desde nodo Raft, como cliente, a otro nodo Raft
// --------------------------------------------------------------------------

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros)
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timeout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre de todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, reply, tRespCall)
	if err != nil {
		return false
	} else {
		if reply.HaDadoSuVoto && (reply.MandatoActual == nr.MandatoActual) {

			nr.VotosRecibidos++
			if nr.VotosRecibidos > (len(nr.Nodos) / 2) {
				nr.IdLider = nr.Yo
				nr.cambiarALider <- true
			}

		}
		nr.Logger.Printf("Mandato %d. Llega voto con valor: %t\n", nr.MandatoActual,
			reply.HaDadoSuVoto)
	}

	return true
}
