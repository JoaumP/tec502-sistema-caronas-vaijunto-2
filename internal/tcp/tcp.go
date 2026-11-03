// Pacote tcp cuida do envio e leitura de mensagens na conexão TCP com os clientes.
// Como o TCP é um fluxo contínuo de bytes (não separa mensagens sozinho), cada
// JSON enviado termina com '\n' pra marcar onde uma mensagem acaba e a outra começa.
package tcp

import "bufio"

// EnviarMensagem escreve os bytes do JSON no buffer, coloca o '\n' no fim e dá flush pro socket
func EnviarMensagem(w *bufio.Writer, dados []byte) error {
	if _, err := w.Write(dados); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return w.Flush()
}

// LerMensagem lê do socket até achar o '\n' que fecha a mensagem atual
func LerMensagem(r *bufio.Reader) ([]byte, error) {
	linha, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// remove o \n do final antes de devolver
	return linha[:len(linha)-1], nil
}
