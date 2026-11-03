// Pacote persistencia cuida de gravar e carregar o estado.json local de cada empresa.
//
// Duas ideias principais aqui:
//  1. Escrita atômica: nunca escrevemos direto por cima do estado.json. Primeiro
//     gravamos tudo em "estado.json.tmp", forçamos o sync pro disco e só no fim
//     damos os.Rename pro nome oficial. Se o servidor cair no meio, o arquivo
//     antigo continua inteiro sem corromper.
//  2. Gravação agrupada: se chegarem várias requisições ao mesmo tempo, uma única
//     ida ao disco já salva o snapshot com as alterações de todo mundo que estava
//     esperando, evitando gargalo de I/O sob carga.
package persistencia

import (
	"os"
	"path/filepath"
	"sync"
)

// Arquivo coordena os pedidos concorrentes de salvamento em disco
type Arquivo struct {
	caminho string
	gerar   func() ([]byte, error) // função que monta os bytes JSON do estado atual

	mu        sync.Mutex
	cond      *sync.Cond
	pedido    uint64 // contador de quantos salvamentos já foram solicitados
	gravado   uint64 // até qual número de pedido já foi efetivamente salvo no disco
	ultimoErr error
}

// Novo garante que a pasta existe e sobe a goroutine responsável por gravar no disco
func Novo(caminho string, gerar func() ([]byte, error)) (*Arquivo, error) {
	if err := os.MkdirAll(filepath.Dir(caminho), 0o755); err != nil {
		return nil, err
	}
	a := &Arquivo{caminho: caminho, gerar: gerar}
	a.cond = sync.NewCond(&a.mu)
	go a.loop()
	return a, nil
}

// Carregar lê o JSON salvo no boot. Se o arquivo ainda não existir, devolve (nil, nil).
func Carregar(caminho string) ([]byte, error) {
	dados, err := os.ReadFile(caminho)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return dados, err
}

// Salvar registra que houve mudança e espera até uma gravação contendo essa
// mudança terminar no disco. Deve ser chamado fora do lock principal do servidor.
func (a *Arquivo) Salvar() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pedido++
	meu := a.pedido
	a.cond.Broadcast()
	for a.gravado < meu {
		a.cond.Wait()
	}
	return a.ultimoErr
}

// loop fica acordando sempre que há pedidos novos e agrupa todos num único gravar()
func (a *Arquivo) loop() {
	for {
		a.mu.Lock()
		for a.gravado == a.pedido {
			a.cond.Wait()
		}
		alvo := a.pedido // tudo que foi pedido até este instante entra junto agora
		a.mu.Unlock()

		err := a.gravar()

		a.mu.Lock()
		a.gravado = alvo
		a.ultimoErr = err
		a.cond.Broadcast()
		a.mu.Unlock()
	}
}

// gravar escreve o snapshot no arquivo temporário, dá fsync e substitui o oficial
func (a *Arquivo) gravar() error {
	dados, err := a.gerar()
	if err != nil {
		return err
	}
	tmp := a.caminho + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(dados); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil { // garante que foi pro disco físico antes do rename
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, a.caminho)
}
