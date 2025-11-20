// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
// Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
// Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
// Solicitar a un nodo de elección por "yo", su mandato en curso,
// y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	// false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = true

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las entradas de registro
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

	// Estado Persistente - - - - -- - - - - - - - - - - - - - -- - - - -- - - -
	// último mandato (iniciado en 0, incrementado monotonicamente)
	CurrentTerm int
	// candidato al que se le ha otorgado el voto en el mandato actual
	VotedFor int
	// registro de entradas; cada entrada contiene el mandato y la operación
	Log []LogEnt

	// Estado Volátil - - - - - - - - - - - - - - - -- - - - - - - - - - - - - -
	// índice de la mayor entrada de registro conocida por estar comprometida
	CommitIndex int
	// índice de la mayor entrada de registro aplicada a la máquina de estados
	LastApplied int

	// Estado Volátil del lider - - - - - - - - - - - - - - - - - - - - - - - -
	// para cada servidor, índice de la siguiente entrada de registro a enviar
	NextIndex []int
	// para cada servidor, índice de la mayor
	// entrada de registro conocida por estar replicada
	MatchIndex []int

	// "Follower", "Candidate", "Leader"
	Estado string
	// Canal para enviar operaciones comprometidas a la máquina de estados
	AplicCh chan AplicaOperacion
	// Canal para parar las gorutinas
	PararCh chan struct{}
	// Tiempo del último latido recibido
	ultimoLatido time.Time
}

type LogEnt struct {
	Mandato int
	Command TipoOperacion
}

// PRACTICA_4: funciones auxiliares internas

// PRACTICA_4: comprobar si el log de un candidato está al menos tan actualizado
// como el de este nodo, siguiendo 5.4.1 del paper de Raft.
func (nr *NodoRaft) logCandidatoEsMasActual(ultimoMandatoCand, ultimoIndiceCand int) bool {
	ultimoIndiceLocal := len(nr.Log) - 1
	ultimoMandatoLocal := nr.Log[ultimoIndiceLocal].Mandato

	if ultimoMandatoCand != ultimoMandatoLocal {
		return ultimoMandatoCand > ultimoMandatoLocal
	}
	return ultimoIndiceCand >= ultimoIndiceLocal
}

// PRACTICA_4: aplicar todas las entradas comprometidas pendientes a la máquina
// de estados a través del canal AplicCh. Debe llamarse con el mutex tomado.
func (nr *NodoRaft) aplicarEntradasComprometidasBloqueado() {
	for nr.LastApplied < nr.CommitIndex {
		nr.LastApplied++
		entrada := nr.Log[nr.LastApplied]
		op := AplicaOperacion{
			Indice:    nr.LastApplied,
			Operacion: entrada.Command,
		}
		// Intentamos no bloquear eternamente: si el buffer está lleno,
		// hacemos un envío bloqueante para no perder operaciones.
		select {
		case nr.AplicCh <- op:
		default:
			nr.AplicCh <- op
		}
	}
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
	nr.IdLider = -1

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix),
				os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// PRACTICA_4: inicialización completa del estado de Raft
	nr.CurrentTerm = 0         // Inicializamos el mandato actual a 0
	nr.VotedFor = -1           // No hay ningún candidato votado
	nr.Log = make([]LogEnt, 1) // Entrada ficticia en índice 0
	nr.Log[0] = LogEnt{Mandato: 0}

	nr.CommitIndex = 0 // Inicializamos el índice de compromiso a 0
	nr.LastApplied = 0 // Inicializamos el último índice aplicado a 0

	nr.Estado = "Follower"             // Inicializamos el estado a Follower
	nr.AplicCh = canalAplicarOperacion // Canal para aplicar operaciones
	nr.PararCh = make(chan struct{})   // Canal para parar gorutinas
	// Inicializamos el tiempo del último latido al de ahora
	nr.ultimoLatido = time.Now()

	nr.NextIndex = make([]int, len(nodos))  // Inicializamos NextIndex
	nr.MatchIndex = make([]int, len(nodos)) // Inicializamos MatchIndex
	for i := range nr.Nodos {
		nr.NextIndex[i] = 1 // siguiente entrada a enviar (log empieza en 1)
		nr.MatchIndex[i] = 0
	}

	// Lanzamos goRutina principal de control de elección
	go nr.bucleEleccion()

	nr.Logger.Printf("Nodo %d inicializado como %s (F) [mandato=%d]\n",
		nr.Yo, nr.Estado, nr.CurrentTerm)

	return nr
}

// Función NUEVA para el bucle de elección
func (nr *NodoRaft) bucleEleccion() {
	for {
		// Esperar un tiempo aleatorio entre 150 y 300 ms
		n, _ := rand.Int(rand.Reader, big.NewInt(150))

		timeout := time.Duration(150+int(n.Int64())) * time.Millisecond

		fmt.Printf("[bucleEleccion] %d esperando timeout=%v estado=%s term=%d\n",
			nr.Yo, timeout, nr.Estado, nr.CurrentTerm)

		nr.Logger.Printf("[bucleEleccion] Esperando timeout de %v (Estado=%s, Term=%d)\n",
			timeout, nr.Estado, nr.CurrentTerm)

		timer := time.NewTimer(timeout)

		select {
		case <-nr.PararCh: // En caso de parada, salir
			timer.Stop()
			nr.Logger.Printf("[bucleEleccion] Nodo %d detenido\n", nr.Yo)
			return

		case <-timer.C: // Timeout, iniciar elección
			// Bloquear mutex para acceder a estado compartido
			nr.Mux.Lock()
			// Tiempo desde último latido
			tiempoInactivo := time.Since(nr.ultimoLatido)

			nr.Logger.Printf("[bucleEleccion] Timeout alcanzado: sin latidos durante %v (Estado=%s, Term=%d)\n",
				tiempoInactivo, nr.Estado, nr.CurrentTerm)

			// Si ha pasado el timeout y no es líder
			if tiempoInactivo >= timeout && nr.Estado != "Leader" {

				nr.Logger.Printf("[bucleEleccion] Nodo %d inicia elección (Term=%d)\n",
					nr.Yo, nr.CurrentTerm+1)

				nr.Mux.Unlock()
				nr.iniciarEleccion()
			} else {
				nr.Mux.Unlock()
			}
		}
	}
}

// PRACTICA_4: iniciarEleccion ahora incluye los datos del log del candidato
func (nr *NodoRaft) iniciarEleccion() {
	nr.Mux.Lock() // Bloquear mutex
	nr.Estado = "Candidate"
	nr.CurrentTerm++
	nr.VotedFor = nr.Yo // Votarse a sí mismo
	nr.IdLider = -1     // No hay líder conocido

	ultimoIndice := len(nr.Log) - 1
	ultimoMandato := nr.Log[ultimoIndice].Mandato
	mandatoCandidato := nr.CurrentTerm
	idCandidato := nr.Yo
	nr.Mux.Unlock() // Desbloquear mutex

	nr.Logger.Printf("Nodo %d inicia elección para mandato %d\n",
		nr.Yo, mandatoCandidato)
	fmt.Printf(">>> %d iniciaEleccion() TERM=%d (ultimoIndice=%d ultimoMandato=%d)\n",
		idCandidato, mandatoCandidato, ultimoIndice, ultimoMandato)

	// Enviar peticiones de voto a todos los demás nodos
	votos := 1 // Autovoto
	var muVotos sync.Mutex

	for i := range nr.Nodos {
		if i == nr.Yo {
			continue // No enviar a mi mismo
		}

		go func(i int) {
			dest := nr.Nodos[i]
			fmt.Printf(">>> %d ENVIANDO RPC PedirVoto A %d (%v)\n",
				idCandidato, i, dest)

			args := ArgsPeticionVoto{
				Mandato:       mandatoCandidato,
				IdCandidato:   idCandidato,
				UltimoIndice:  ultimoIndice,
				UltimoMandato: ultimoMandato,
			}
			var respuesta RespuestaPeticionVoto

			// Si la llamada RPC ha ido bien
			if nr.enviarPeticionVoto(i, &args, &respuesta) {
				fmt.Printf("<<< %d RESPUESTA RPC DE %d: voto=%v mandato=%d\n",
					idCandidato, i, respuesta.VotoConcedido, respuesta.Mandato)

				debeConvertirseEnLider := false

				nr.Mux.Lock()

				// Si el mandato del otro es mayor, nos rendimos
				if respuesta.Mandato > nr.CurrentTerm {
					nr.CurrentTerm = respuesta.Mandato
					nr.Estado = "Follower"
					nr.VotedFor = -1
					nr.IdLider = -1
					nr.Mux.Unlock()
					return
				}

				// Si el voto fue concedido y seguimos siendo candidato en este mandato
				if respuesta.VotoConcedido &&
					respuesta.Mandato == nr.CurrentTerm &&
					nr.Estado == "Candidate" {

					muVotos.Lock()
					votos++
					tieneMayoría := votos > len(nr.Nodos)/2
					muVotos.Unlock()

					fmt.Printf("+++ %d recibe voto de %d (votos=%d de %d)\n",
						idCandidato, i, votos, len(nr.Nodos))

					if tieneMayoría {
						debeConvertirseEnLider = true
					}
				}

				nr.Mux.Unlock()

				// IMPORTANTE: llamar a convertirseEnLider SIN el mutex cogido
				if debeConvertirseEnLider {
					nr.convertirseEnLider()
				}

			} else {
				fmt.Printf("XXX %d ERROR RPC A %d: %v\n",
					idCandidato, i, "RPC fallo en PeticionVoto")
				fmt.Printf("!!! %d NO consigue voto de %d (RPC fallo)\n",
					idCandidato, i)
			}
		}(i)
	}
}

// PRACTICA_4: al convertirse en líder se inicializan NextIndex/MatchIndex
func (nr *NodoRaft) convertirseEnLider() {
	nr.Mux.Lock()
	if nr.Estado == "Leader" {
		// ya soy líder, evitar arrancar latidos duplicados
		nr.Mux.Unlock()
		return
	}
	nr.Estado = "Leader"
	nr.IdLider = nr.Yo
	lastIndex := len(nr.Log) - 1
	for i := range nr.Nodos {
		nr.NextIndex[i] = lastIndex + 1
		nr.MatchIndex[i] = 0
	}
	nr.Mux.Unlock()

	nr.Logger.Printf("Nodo %d se convierte en LIDER para mandato %d\n",
		nr.Yo, nr.CurrentTerm)
	fmt.Printf("*** %d SE CONVIERTE EN LIDER (term=%d)\n", nr.Yo, nr.CurrentTerm)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-nr.PararCh:
				return
			case <-ticker.C:
				nr.enviarLatidos()
			}
		}
	}()
}

// PRACTICA_4: latidos periódicos sin entradas (para mantener el liderazgo)
func (nr *NodoRaft) enviarLatidos() {
	nr.Mux.Lock()
	mandato := nr.CurrentTerm
	idLider := nr.Yo
	leaderCommit := nr.CommitIndex

	// *** ARREGLO PRACTICA 4 ***
	// Los latidos deben llevar el PrevLogIndex/PrevLogTerm REAL,
	// aunque Entries sea nil, para evitar conflictos falsos.
	lastIndex := len(nr.Log) - 1
	prevTerm := nr.Log[lastIndex].Mandato
	// *** FIN ARREGLO ***

	nr.Mux.Unlock()

	for i := range nr.Nodos {
		if i == nr.Yo {
			continue // No enviarse a sí mismo
		}
		go func(i int) {
			args := ArgAppendEntries{
				Mandato:      mandato,
				IdLider:      idLider,
				PrevLogIndex: lastIndex,
				PrevLogTerm:  prevTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			}
			var results Results

			err := nr.Nodos[i].CallTimeout(
				"NodoRaft.AppendEntries",
				&args,
				&results,
				50*time.Millisecond,
			)

			if err != nil && kEnableDebugLogs {
				nr.Logger.Printf("Error enviando latido a nodo %d: %v\n",
					i, err)
				fmt.Printf("LATIDO ERROR: lider=%d → seguidor=%d err=%v\n",
					idLider, i, err)
				return
			}

			nr.Mux.Lock()
			defer nr.Mux.Unlock()
			if results.Mandato > nr.CurrentTerm {
				nr.CurrentTerm = results.Mandato
				nr.Estado = "Follower"
				nr.IdLider = -1
				nr.VotedFor = -1
			}
		}(i)
	}
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo, ojo piojo con estos
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	var yo int = nr.Yo
	var mandato int
	var esLider bool
	var idLider int = nr.IdLider

	mandato = nr.CurrentTerm // Obtener el mandato actual
	if nr.Estado == "Leader" {
		esLider = true // Si el estado es Leader, esLider es true
	} else {
		esLider = false // Si no, es false
	}

	nr.Logger.Printf("ESTADO CONSULTADO -> Yo=%d | Term=%d | Estado=%s | EsLider=%v | IdLider=%d\n",
		yo, mandato, nr.Estado, esLider, idLider)
	return yo, mandato, esLider, idLider
}

// PRACTICA_4: replicación de log en un seguidor concreto utilizando NextIndex/MatchIndex
func (nr *NodoRaft) replicarLogEnSeguidor(idxSeguidor int) bool {
	for {
		nr.Mux.Lock()
		if nr.Estado != "Leader" {
			nr.Mux.Unlock()
			return false
		}

		nextIdx := nr.NextIndex[idxSeguidor]
		lastIndex := len(nr.Log) - 1
		if nextIdx > lastIndex {
			// Nada nuevo que enviar
			nr.Mux.Unlock()
			return true
		}

		prevIdx := nextIdx - 1
		prevTerm := nr.Log[prevIdx].Mandato
		entries := make([]LogEnt, lastIndex-nextIdx+1)
		copy(entries, nr.Log[nextIdx:])

		args := ArgAppendEntries{
			Mandato:      nr.CurrentTerm,
			IdLider:      nr.Yo,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: nr.CommitIndex,
		}
		destino := nr.Nodos[idxSeguidor]
		mandatoLider := nr.CurrentTerm
		nr.Mux.Unlock()

		var results Results
		err := destino.CallTimeout(
			"NodoRaft.AppendEntries",
			&args,
			&results,
			200*time.Millisecond,
		)
		if err != nil {
			if kEnableDebugLogs {
				nr.Logger.Printf("Error replicando en seguidor %d: %v\n",
					idxSeguidor, err)
				fmt.Printf("REPLICACION ERROR: lider=%d → seguidor=%d err=%v\n",
					nr.Yo, idxSeguidor, err)
			}
			// Consideramos que este seguidor está caído
			return false
		}

		nr.Mux.Lock()
		if results.Mandato > nr.CurrentTerm {
			// Nos hemos quedado atrás: pasamos a seguidor
			nr.CurrentTerm = results.Mandato
			nr.Estado = "Follower"
			nr.VotedFor = -1
			nr.IdLider = -1
			nr.Mux.Unlock()
			return false
		}

		// Si ha habido cambio de mandato durante la replicación, abortamos
		if nr.CurrentTerm != mandatoLider {
			nr.Mux.Unlock()
			return false
		}

		if results.Success {
			// Avanzamos índices
			nr.MatchIndex[idxSeguidor] = prevIdx + len(entries)
			nr.NextIndex[idxSeguidor] = nr.MatchIndex[idxSeguidor] + 1
			nr.Mux.Unlock()
			return true
		}

		// FALLO de consistencia de log: retroceder NextIndex y volver a intentar
		if nr.NextIndex[idxSeguidor] > 1 {
			nr.NextIndex[idxSeguidor]--
		} else {
			nr.NextIndex[idxSeguidor] = 1
		}
		nr.Mux.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
// Quinto valor es el valor a devolver en caso de lectura (en esta práctica,
// lo dejamos vacío; la máquina de estados se implementa fuera).
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {

	indice := -1
	mandato := -1
	esLider := false
	idLider := -1
	valorADevolver := ""

	// PRACTICA_4: implementación de SometerOperacion
	nr.Mux.Lock()
	if nr.Estado != "Leader" {
		mandato = nr.CurrentTerm
		idLider = nr.IdLider
		nr.Mux.Unlock()
		return indice, mandato, esLider, idLider, valorADevolver
	}

	// Añadir entrada al log del líder
	nuevaEntrada := LogEnt{
		Mandato: nr.CurrentTerm,
		Command: operacion,
	}
	nr.Log = append(nr.Log, nuevaEntrada)
	indice = len(nr.Log) - 1
	mandato = nr.CurrentTerm
	esLider = true
	idLider = nr.Yo

	// El propio líder tiene la entrada replicada
	nr.MatchIndex[nr.Yo] = indice
	nr.NextIndex[nr.Yo] = indice + 1

	mandatoLider := nr.CurrentTerm
	nr.Mux.Unlock()

	// Replicar en seguidores
	acks := 1 // el líder cuenta como un ack
	var wg sync.WaitGroup
	ackCh := make(chan bool, len(nr.Nodos)-1)

	for i := range nr.Nodos {
		if i == nr.Yo {
			continue
		}
		wg.Add(1)
		go func(idxSeguidor int) {
			defer wg.Done()
			ok := nr.replicarLogEnSeguidor(idxSeguidor)
			if ok {
				ackCh <- true
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(ackCh)
	}()

	for ok := range ackCh {
		if ok {
			acks++
			if acks > len(nr.Nodos)/2 {
				break
			}
		}
	}

	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	// Confirmar compromiso si seguimos siendo líder en el mismo mandato
	if nr.Estado == "Leader" && nr.CurrentTerm == mandatoLider &&
		acks > len(nr.Nodos)/2 && indice > nr.CommitIndex {

		nr.CommitIndex = indice
		nr.aplicarEntradasComprometidasBloqueado()
	}

	// Para esta práctica no devolvemos el valor de una lectura;
	// la máquina de estados se implementa fuera.
	valorADevolver = ""

	return indice, nr.CurrentTerm, nr.Estado == "Leader", nr.IdLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
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
	reply.IdNodo, reply.Mandato,
		reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// PRACTICA_4 - RPC para exponer longitud del log a los tests
func (nr *NodoRaft) ObtenerLongitudLog(args Vacio, reply *int) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	// Log[0] es la entrada ficticia, por eso restamos 1
	*reply = len(nr.Log) - 1
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
//
// PRACTICA_4: añadimos info del último índice y mandato del log del candidato
type ArgsPeticionVoto struct {
	Mandato       int // Mandato del candidato
	IdCandidato   int // ID del candidato que pide el voto
	UltimoIndice  int // Último índice del log del candidato
	UltimoMandato int // Mandato de la última entrada del log del candidato
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	Mandato       int  // Mandato actual del votante
	VotoConcedido bool // True si concede el voto
}

// PRACTICA_4: PedirVoto aplica ahora la regla de "log más actualizado"
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {

	fmt.Printf("### %d RECIBE PedirVoto DE %d (term=%d)\n",
		nr.Yo, peticion.IdCandidato, peticion.Mandato)

	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if peticion.Mandato < nr.CurrentTerm { // Si el mandato es menor, rechazar
		reply.Mandato = nr.CurrentTerm
		reply.VotoConcedido = false
		return nil
	}

	if peticion.Mandato > nr.CurrentTerm { // Si el mandato es mayor, actualizar
		nr.CurrentTerm = peticion.Mandato
		nr.Estado = "Follower"
		nr.VotedFor = -1
		nr.IdLider = -1
	}

	// Comprobar si el log del candidato está al menos tan actualizado
	logOk := nr.logCandidatoEsMasActual(peticion.UltimoMandato, peticion.UltimoIndice)

	if logOk && (nr.VotedFor == -1 || nr.VotedFor == peticion.IdCandidato) {
		nr.VotedFor = peticion.IdCandidato
		reply.VotoConcedido = true
		nr.ultimoLatido = time.Now() // Actualizar tiempo del último latido
	} else {
		reply.VotoConcedido = false
	}

	reply.Mandato = nr.CurrentTerm

	return nil
}

// PRACTICA_4: AppendEntries (latidos + replicación de entradas)
type ArgAppendEntries struct {
	Mandato      int      // Mandato del líder
	IdLider      int      // ID del líder
	PrevLogIndex int      // Índice de la entrada anterior a las nuevas
	PrevLogTerm  int      // Mandato de la entrada anterior
	Entries      []LogEnt // Entradas del log a replicar (vacío para latido)
	LeaderCommit int      // Índice de compromiso del líder
}

type Results struct {
	Mandato      int  // Mandato del seguidor
	Success      bool // True si el seguidor acepta las entradas
	UltimoIndice int  // Último índice de log conocido por el seguidor
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {

	fmt.Printf("### %d RECIBE AppendEntries DE %d (entries=%d term=%d)\n",
		nr.Yo, args.IdLider, len(args.Entries), args.Mandato)

	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if args.Mandato < nr.CurrentTerm { // Si el mandato es menor, rechazar
		results.Mandato = nr.CurrentTerm
		results.Success = false
		results.UltimoIndice = len(nr.Log) - 1
		return nil
	}

	if args.Mandato > nr.CurrentTerm {
		nr.CurrentTerm = args.Mandato
		nr.Estado = "Follower"
		nr.VotedFor = -1
	}
	nr.IdLider = args.IdLider
	nr.ultimoLatido = time.Now()

	// Caso especial: latido sin entradas -> no tocamos el log
	if len(args.Entries) == 0 {
		// Actualizar commitIndex si procede
		if args.LeaderCommit > nr.CommitIndex {
			maxPosible := len(nr.Log) - 1
			if args.LeaderCommit < maxPosible {
				nr.CommitIndex = args.LeaderCommit
			} else {
				nr.CommitIndex = maxPosible
			}
			nr.aplicarEntradasComprometidasBloqueado()
		}
		results.Mandato = nr.CurrentTerm
		results.Success = true
		results.UltimoIndice = len(nr.Log) - 1
		return nil
	}

	// Comprobación de consistencia del log
	if args.PrevLogIndex > len(nr.Log)-1 {
		// El seguidor no tiene todavía esa entrada
		results.Mandato = nr.CurrentTerm
		results.Success = false
		results.UltimoIndice = len(nr.Log) - 1
		return nil
	}

	if args.PrevLogIndex >= 0 {
		if nr.Log[args.PrevLogIndex].Mandato != args.PrevLogTerm {
			// Conflicto: truncar log a PrevLogIndex
			nr.Log = nr.Log[:args.PrevLogIndex]
			results.Mandato = nr.CurrentTerm
			results.Success = false
			results.UltimoIndice = len(nr.Log) - 1
			return nil
		}
	}

	// Añadir / sobreescribir entradas nuevas
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(nr.Log) {
			nr.Log[index] = entry
		} else {
			nr.Log = append(nr.Log, entry)
		}
	}

	// Actualizar commitIndex
	if args.LeaderCommit > nr.CommitIndex {
		nuevoCommit := args.LeaderCommit
		ultimoIndiceNuevo := args.PrevLogIndex + len(args.Entries)
		if nuevoCommit > ultimoIndiceNuevo {
			nuevoCommit = ultimoIndiceNuevo
		}
		if nuevoCommit > nr.CommitIndex {
			nr.CommitIndex = nuevoCommit
			nr.aplicarEntradasComprometidasBloqueado()
		}
	}

	results.Mandato = nr.CurrentTerm
	results.Success = true
	results.UltimoIndice = len(nr.Log) - 1
	return nil
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

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
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	fmt.Printf(">>> %d ENVIANDO RPC PedirVoto A %d (%s)\n",
		nr.Yo, nodo, string(nr.Nodos[nodo]))

	err := nr.Nodos[nodo].CallTimeout(
		"NodoRaft.PedirVoto",
		args,
		reply,
		50*time.Millisecond,
	)

	if err != nil {
		fmt.Printf("XXX %d ERROR RPC A %d: %v\n", nr.Yo, nodo, err)
	} else {
		fmt.Printf("<<< %d RESPUESTA RPC DE %d: voto=%v mandato=%d\n",
			nr.Yo, nodo, reply.VotoConcedido, reply.Mandato)
	}

	return err == nil
}
