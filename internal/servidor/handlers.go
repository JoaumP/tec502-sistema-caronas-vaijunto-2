// trata os pedidos que chegam dos clientes via TCP (login, caronas e reservas)
package servidor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
)

// respostaErro monta o envelope padrão de falha pro cliente TCP
func respostaErro(msg string) protocolo.Resposta {
	return protocolo.Resposta{Sucesso: false, Erro: msg}
}

// Autenticação de usuários (sempre local na empresa em que o cliente conectou)

// HandleCadastro cria uma conta nova nesta empresa salvando a senha com hash bcrypt
func (e *Estado) HandleCadastro(payload json.RawMessage) protocolo.Resposta {
	var pedido protocolo.PedidoLogin
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}
	if pedido.Login == "" || pedido.Senha == "" {
		return respostaErro("login e senha são obrigatórios")
	}

	// bcrypt é custoso de propósito; rodamos fora do lock pra não travar o servidor
	hash, err := bcrypt.GenerateFromPassword([]byte(pedido.Senha), bcrypt.DefaultCost)
	if err != nil {
		return respostaErro("erro ao processar senha")
	}

	e.relogio.Tick()
	e.mu.Lock()
	if _, existe := e.usuarios[pedido.Login]; existe {
		e.mu.Unlock()
		return respostaErro("login já cadastrado")
	}
	e.proximoIDUsuario++
	u := &modelos.Usuario{
		ID:        e.proximoIDUsuario,
		Login:     pedido.Login,
		SenhaHash: string(hash),
	}
	e.usuarios[u.Login] = u
	e.mu.Unlock()
	e.salvar()

	dados, _ := json.Marshal(protocolo.RespostaLogin{IDUsuario: u.ID})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// HandleLogin valida login e senha do cliente na base local desta empresa
func (e *Estado) HandleLogin(payload json.RawMessage) protocolo.Resposta {
	var pedido protocolo.PedidoLogin
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}

	e.mu.RLock()
	usuario, existe := e.usuarios[pedido.Login]
	var id int64
	var hash string
	if existe {
		id = usuario.ID
		hash = usuario.SenhaHash
	}
	e.mu.RUnlock()

	if !existe {
		return respostaErro("login não encontrado")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(pedido.Senha)); err != nil {
		return respostaErro("senha incorreta")
	}

	dados, _ := json.Marshal(protocolo.RespostaLogin{IDUsuario: id})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// Operações do motorista (caronas e assentos ficam sob responsabilidade desta empresa)

// HandlePublicarCarona valida a sequência de trechos informada pelo motorista e
// salva a carona localmente como ativa
func (e *Estado) HandlePublicarCarona(idMotorista int64, payload json.RawMessage) protocolo.Resposta {
	if idMotorista == 0 {
		return respostaErro("não autenticado")
	}

	var pedido protocolo.PedidoPublicarCarona
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}
	if err := validarTrechos(pedido.Trechos); err != nil {
		return respostaErro(err.Error())
	}

	e.relogio.Tick()
	e.mu.Lock()
	e.proximoIDCarona++
	idCarona := e.proximoIDCarona

	idsTrechos := make([]int64, 0, len(pedido.Trechos))
	for _, tp := range pedido.Trechos {
		e.proximoIDTrecho++
		t := &modelos.Trecho{
			ID:             e.proximoIDTrecho,
			IDCarona:       idCarona,
			Origem:         tp.Origem,
			Destino:        tp.Destino,
			HorarioSaida:   tp.HorarioSaida,
			HorarioChegada: tp.HorarioChegada,
			PrecoCentavos:  tp.PrecoCentavos,
			AssentosTotais: tp.AssentosTotais,
			AssentosLivres: tp.AssentosTotais,
		}
		e.trechos[t.ID] = t
		idsTrechos = append(idsTrechos, t.ID)
	}

	carona := &modelos.Carona{
		ID:          idCarona,
		MotoristaID: idMotorista,
		Status:      modelos.CaronaAtiva,
		Trechos:     idsTrechos,
	}
	e.caronas[carona.ID] = carona
	e.mu.Unlock()
	e.salvar()

	dados, _ := json.Marshal(protocolo.RespostaPublicarCarona{IDCarona: carona.ID})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// HandleConsultarCaronas devolve todas as caronas do motorista logado junto com
// as vagas restantes e os passageiros confirmados em cada trecho
func (e *Estado) HandleConsultarCaronas(idMotorista int64) protocolo.Resposta {
	if idMotorista == 0 {
		return respostaErro("não autenticado")
	}

	e.mu.RLock()
	var resultado []protocolo.CaronaDetalhada
	for _, c := range e.caronas {
		if c.MotoristaID != idMotorista {
			continue
		}
		trechos := make([]protocolo.TrechoDetalhado, 0, len(c.Trechos))
		for _, idTrecho := range c.Trechos {
			t := e.trechos[idTrecho]
			if t == nil {
				continue
			}
			trechos = append(trechos, protocolo.TrechoDetalhado{
				ID:             t.ID,
				Origem:         t.Origem,
				Destino:        t.Destino,
				HorarioSaida:   t.HorarioSaida,
				HorarioChegada: t.HorarioChegada,
				AssentosTotais: t.AssentosTotais,
				AssentosLivres: t.AssentosLivres,
				Passageiros:    e.passageirosDoTrecho(t.ID),
			})
		}
		resultado = append(resultado, protocolo.CaronaDetalhada{
			ID:      c.ID,
			Status:  c.Status,
			Trechos: trechos,
		})
	}
	e.mu.RUnlock()

	sort.Slice(resultado, func(i, j int) bool { return resultado[i].ID < resultado[j].ID })
	dados, _ := json.Marshal(protocolo.RespostaConsultarCaronas{Caronas: resultado})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// HandleCancelarCarona cancela uma carona do motorista e propaga o cancelamento
// pra todas as reservas que usavam algum trecho dela (mesmo quando o passageiro
// é de outra empresa e tinha trechos combinados numa terceira empresa)
func (e *Estado) HandleCancelarCarona(idMotorista int64, payload json.RawMessage) protocolo.Resposta {
	if idMotorista == 0 {
		return respostaErro("não autenticado")
	}

	var pedido protocolo.PedidoCancelarCarona
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}

	e.relogio.Tick()
	e.mu.Lock()
	carona, existe := e.caronas[pedido.IDCarona]
	if !existe {
		e.mu.Unlock()
		return respostaErro("carona não encontrada")
	}
	if carona.MotoristaID != idMotorista {
		e.mu.Unlock()
		return respostaErro("carona não pertence a este motorista")
	}
	if carona.Status != modelos.CaronaAtiva {
		e.mu.Unlock()
		return respostaErro("carona já está cancelada")
	}

	carona.Status = modelos.CaronaCancelada

	// descobre todas as transações que ocupavam assento em algum trecho desta carona
	txAfetadas := make(map[string]*modelos.TxParticipante)
	for _, idTrecho := range carona.Trechos {
		for txID := range e.ocupacoes[idTrecho] {
			if tx := e.txPart[txID]; tx != nil {
				txAfetadas[txID] = tx
			}
		}
	}

	for _, tx := range txAfetadas {
		eraConfirmada := tx.Estado == modelos.TxConfirmada
		if eraConfirmada {
			tx.Estado = modelos.TxCancelada
		} else {
			tx.Estado = modelos.TxAbortada
		}
		e.liberarAssentos(tx)

		if eraConfirmada {
			e.propagarCancelamentoCaronaLocked(tx, carona.ID)
		}
	}
	e.mu.Unlock()

	e.salvar()
	e.dispararOutboxRapido()
	return protocolo.Resposta{Sucesso: true}
}

// propagarCancelamentoCaronaLocked avisa tanto a empresa coordenadora do
// passageiro quanto diretamente as outras empresas participantes do itinerário.
// Assim, mesmo que o coordenador esteja fora do ar na hora, quem estiver online
// já libera seus assentos imediatamente. Chamar com e.mu travado.
func (e *Estado) propagarCancelamentoCaronaLocked(tx *modelos.TxParticipante, idCarona int64) {
	if tx.Coordenador == e.cfg.Empresa {
		e.garantirReservaDoCoordLocked(tx.ID)
		e.cancelarReservaLocalLocked(tx.ID, e.cfg.Empresa)
		return
	}
	if tx.Coordenador != "" {
		caminho := fmt.Sprintf("%s/reservas/%s/cancelamento-carona", consorcio.Prefixo, tx.ID)
		e.enfileirar(tx.Coordenador, caminho, consorcio.PedidoCancelamentoCarona{
			Empresa:  e.cfg.Empresa,
			IDCarona: idCarona,
		})
	}
	for _, emp := range tx.Participantes {
		if emp == "" || emp == e.cfg.Empresa || emp == tx.Coordenador {
			continue
		}
		caminho := fmt.Sprintf("%s/reservas/%s/cancelar", consorcio.Prefixo, tx.ID)
		e.enfileirar(emp, caminho, nil)
	}
}

// algumaCaronaCanceladaLocked checa se algum dos trechos pertence a uma carona já cancelada
func (e *Estado) algumaCaronaCanceladaLocked(trechos []int64) bool {
	for _, idTrecho := range trechos {
		t := e.trechos[idTrecho]
		if t == nil || !e.caronaAtiva(t.IDCarona) {
			return true
		}
	}
	return false
}

// Operações de reserva do passageiro

// HandleConfirmarReserva dispara o protocolo distribuído (3PC) pra reservar todos
// os trechos do itinerário escolhido ou nenhum deles
func (e *Estado) HandleConfirmarReserva(idPassageiro int64, payload json.RawMessage) protocolo.Resposta {
	if idPassageiro == 0 {
		return respostaErro("não autenticado")
	}

	var pedido protocolo.PedidoConfirmarReserva
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}

	idReserva, err := e.confirmarItinerario(idPassageiro, pedido.Itinerario)
	if err != nil {
		return respostaErro(err.Error())
	}

	dados, _ := json.Marshal(protocolo.RespostaConfirmarReserva{IDReserva: idReserva})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// HandleConsultarReservas lista as reservas feitas pelo passageiro nesta empresa
func (e *Estado) HandleConsultarReservas(idPassageiro int64) protocolo.Resposta {
	if idPassageiro == 0 {
		return respostaErro("não autenticado")
	}

	e.mu.RLock()
	var resultado []protocolo.ReservaDetalhada
	for _, r := range e.reservas {
		if r.PassageiroID != idPassageiro {
			continue
		}
		trechos := make([]protocolo.TrechoResumo, 0, len(r.Trechos))
		for _, t := range r.Trechos {
			trechos = append(trechos, protocolo.TrechoResumo{
				Empresa:        t.Empresa,
				ID:             t.ID,
				Origem:         t.Origem,
				Destino:        t.Destino,
				HorarioSaida:   t.HorarioSaida,
				HorarioChegada: t.HorarioChegada,
				PrecoCentavos:  t.PrecoCentavos,
			})
		}
		resultado = append(resultado, protocolo.ReservaDetalhada{
			ID:      r.ID,
			Trechos: trechos,
			Status:  r.Status,
		})
	}
	e.mu.RUnlock()

	sort.Slice(resultado, func(i, j int) bool { return resultado[i].ID < resultado[j].ID })
	dados, _ := json.Marshal(protocolo.RespostaConsultarReservas{Reservas: resultado})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// HandleCancelarReserva cancela uma reserva confirmada do passageiro e enfileira
// na outbox a liberação das vagas nas empresas donas de cada trecho
func (e *Estado) HandleCancelarReserva(idPassageiro int64, payload json.RawMessage) protocolo.Resposta {
	if idPassageiro == 0 {
		return respostaErro("não autenticado")
	}

	var pedido protocolo.PedidoCancelarReserva
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}

	e.relogio.Tick()
	e.mu.Lock()
	reserva, existe := e.reservas[pedido.IDReserva]
	if !existe {
		e.mu.Unlock()
		return respostaErro("reserva não encontrada")
	}
	if reserva.PassageiroID != idPassageiro {
		e.mu.Unlock()
		return respostaErro("reserva não pertence a este passageiro")
	}
	if reserva.Status != modelos.ReservaConfirmada {
		e.mu.Unlock()
		return respostaErro("reserva já não está mais confirmada")
	}

	e.cancelarReservaLocalLocked(reserva.ID, "")
	e.mu.Unlock()

	e.salvar()
	e.dispararOutboxRapido()
	return protocolo.Resposta{Sucesso: true}
}

// CancelarPorCaronaRemota é chamada via REST quando outra empresa avisa que um
// motorista de lá cancelou uma carona que fazia parte da reserva de um passageiro nosso
func (e *Estado) CancelarPorCaronaRemota(idReserva, empresaQueAvisou string) {
	e.mu.Lock()
	e.garantirReservaDoCoordLocked(idReserva)
	if _, existe := e.reservas[idReserva]; !existe {
		e.mu.Unlock()
		return
	}
	alterou := e.cancelarReservaLocalLocked(idReserva, empresaQueAvisou)
	e.mu.Unlock()

	if alterou {
		e.salvar()
		e.dispararOutboxRapido()
	}
}

// garantirReservaDoCoordLocked monta o registro de reserva a partir de txCoord se
// o aviso de cancelamento chegar logo após o reinício do coordenador (antes do boot terminar)
func (e *Estado) garantirReservaDoCoordLocked(idReserva string) {
	if _, existe := e.reservas[idReserva]; existe {
		return
	}
	if c := e.txCoord[idReserva]; c != nil && len(c.Trechos) > 0 {
		c.Fase = modelos.CoordConfirmada
		e.reservas[idReserva] = &modelos.Reserva{
			ID:           idReserva,
			PassageiroID: c.PassageiroID,
			Trechos:      c.Trechos,
			Status:       modelos.ReservaConfirmada,
			CriadoEm:     time.Now(),
		}
	}
}

// cancelarReservaLocalLocked marca a reserva como cancelada, solta os assentos
// locais (se houver) e enfileira o cancelamento pras outras empresas do itinerário.
// Chamar com e.mu travado.
func (e *Estado) cancelarReservaLocalLocked(idReserva, ignorarEmpresa string) bool {
	r := e.reservas[idReserva]
	if r == nil || r.Status != modelos.ReservaConfirmada {
		return false
	}
	r.Status = modelos.ReservaCancelada

	empresas := make(map[string]bool)
	for _, t := range r.Trechos {
		empresas[t.Empresa] = true
	}

	for emp := range empresas {
		if emp == e.cfg.Empresa {
			if tx := e.txPart[idReserva]; tx != nil && tx.Estado != modelos.TxCancelada {
				tx.Estado = modelos.TxCancelada
				e.liberarAssentos(tx)
			}
			continue
		}
		if emp == ignorarEmpresa {
			continue
		}
		caminho := fmt.Sprintf("%s/reservas/%s/cancelar", consorcio.Prefixo, idReserva)
		e.enfileirar(emp, caminho, nil)
	}
	return true
}

// validarTrechos confere se a rota enviada pelo motorista faz sentido (cidades
// preenchidas, horários crescentes, conexões encadeadas, vagas > 0 e preço >= 0)
func validarTrechos(trechos []protocolo.TrechoPedido) error {
	if len(trechos) == 0 {
		return errors.New("carona precisa de pelo menos um trecho")
	}
	for i, tp := range trechos {
		if tp.Origem == "" || tp.Destino == "" {
			return errors.New("origem e destino não podem ser vazios")
		}
		if tp.Origem == tp.Destino {
			return errors.New("origem e destino não podem ser iguais")
		}
		if len(tp.HorarioSaida) < 10 || len(tp.HorarioChegada) < 10 {
			return errors.New("horário inválido")
		}
		if tp.HorarioChegada <= tp.HorarioSaida {
			return errors.New("horário de chegada deve ser depois da saída")
		}
		if tp.AssentosTotais <= 0 {
			return errors.New("assentos totais deve ser maior que zero")
		}
		if tp.PrecoCentavos < 0 {
			return errors.New("preço não pode ser negativo")
		}
		if i > 0 && tp.Origem != trechos[i-1].Destino {
			return errors.New("origem deve ser o destino do trecho anterior")
		}
		if i > 0 && tp.HorarioSaida < trechos[i-1].HorarioChegada {
			return errors.New("um trecho não pode partir antes do anterior chegar")
		}
	}
	return nil
}
