// funções pra converter e formatar datas e horários nos clientes
package cliente

import "time"

// ConverterHora só valida se a string está no formato HH:MM
func ConverterHora(entrada string) (string, error) {
	_, err := time.Parse("15:04", entrada)
	return entrada, err
}

// ConverterData passa de "DD/MM/AAAA" (digitado pelo usuário) pra "AAAA-MM-DD",
// que é o formato usado no protocolo porque permite comparar datas como string
func ConverterData(entrada string) (string, error) {
	t, err := time.Parse("02/01/2006", entrada)
	if err != nil {
		return "", err
	}
	return t.Format("2006-01-02"), nil
}

// MontarHorarios junta a data (já no formato "2006-01-02") com os horários de
// saída e chegada informados pelo motorista. Se o horário de chegada for menor
// que o de saída, significa que a viagem passou da meia-noite, então jogamos
// a chegada pro dia seguinte.
func MontarHorarios(dataInterna, horaSaida, horaChegada string) (string, string, error) {
	saida := dataInterna + "T" + horaSaida
	chegada := dataInterna + "T" + horaChegada

	// confere se a data + hora de saída formam um horário válido
	if _, err := time.Parse("2006-01-02T15:04", saida); err != nil {
		return "", "", err
	}

	// chegada "menor" que a saída (ex: sai 23:00 e chega 02:00) -> soma 1 dia
	if chegada < saida {
		t, err := time.Parse("2006-01-02T15:04", chegada)
		if err != nil {
			return "", "", err
		}
		chegada = t.AddDate(0, 0, 1).Format("2006-01-02T15:04")
	}

	return saida, chegada, nil
}

// FormatarHorario deixa o horário salvo no sistema ("2026-09-15T12:00") mais
// amigável pra mostrar na tela ("15/09 12:00")
func FormatarHorario(h string) string {
	t, err := time.Parse("2006-01-02T15:04", h)
	if err != nil {
		return h
	}
	return t.Format("02/01 15:04")
}
