// relógio lógico de Lamport usado pra ordenar os pedidos entre servidores
package relogio

import "sync"

// Lamport guarda o contador lógico protegido por mutex
type Lamport struct {
	mu    sync.Mutex
	valor int64
}

// Novo inicia o relógio com o valor salvo no disco (ou 0 se for novo)
func Novo(inicial int64) *Lamport {
	return &Lamport{valor: inicial}
}

// Tick incrementa o relógio num evento local ou envio de mensagem
func (l *Lamport) Tick() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.valor++
	return l.valor
}

// Receber atualiza o relógio ao receber mensagem de outro servidor: max(local, recebido) + 1
func (l *Lamport) Receber(recebido int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if recebido > l.valor {
		l.valor = recebido
	}
	l.valor++
	return l.valor
}

// Atual só lê o valor corrente sem incrementar (usado na hora de salvar o snapshot em disco)
func (l *Lamport) Atual() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.valor
}
