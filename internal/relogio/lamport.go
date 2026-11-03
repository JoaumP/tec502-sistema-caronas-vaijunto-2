// Pacote relogio implementa o relógio lógico de Lamport.
// Como cada servidor roda numa máquina diferente sem garantia de relógio físico
// sincronizado, não dá pra usar time.Now() pra decidir quem pediu uma vaga antes.
// Com Lamport, sempre que um evento A acontece antes de B, o contador de A fica
// menor que o de B, e os empates são resolvidos pelo nome da empresa.
package relogio

import "sync"

// Lamport guarda o contador lógico protegido por mutex pra acesso concorrente
type Lamport struct {
	mu    sync.Mutex
	valor int64
}

// Novo inicia o relógio a partir do último valor salvo no estado.json (ou 0 se for a primeira vez)
func Novo(inicial int64) *Lamport {
	return &Lamport{valor: inicial}
}

// Tick soma 1 no relógio num evento local ou antes de enviar mensagem pra outra empresa
func (l *Lamport) Tick() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.valor++
	return l.valor
}

// Receber atualiza o relógio quando chega requisição/resposta de outro servidor:
// L = max(L_local, L_recebido) + 1
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
